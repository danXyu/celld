"""Snowflake's SQL API (/api/v2/statements) on fakesnow, for running the
loader binary end to end.

It answers the way the SQL API does: every value as text, results split into
partitions, a statement that runs long answered 202 and then polled, a
retried request (same requestId, retry=true) answered from the first run. To
exercise the client, it answers `CREATE OR REPLACE DYNAMIC TABLE` with 202,
fails the first attempt of `ALTER TASK` with 503, and puts
PARTITION_ROWS rows in a partition.

It checks the key-pair JWT's claims against the fingerprint of the test key
computed the way Snowflake's documentation does (openssl), so the loader's
fingerprint is checked against Snowflake's definition. It does not check the
signature; the loader's unit tests do.

What fakesnow cannot run is emulated, on top of the replacements test_sql.py
makes:

- Snowpipe Streaming: GET /v2/streaming/hostname answers this server's own
  address, POST /oauth/token trades the key-pair JWT for a scoped token,
  and an append to a pipe's elastic channel runs the pipe's COPY (as
  test_sql.py does) over the NDJSON rows. The first attempt of the first
  append fails with 503 and must come back with the same request id and
  retryCount 1; `appends` records every attempt. As on an elastic channel,
  an acknowledged append is not queryable at once: its rows land only when
  VISIBLE_AFTER more statements have run;
- the stream EXPORT_LANDING_NEW is a view over EXPORT_LANDING's rows past an
  offset, advanced when a task that read it commits;
- an EXECUTE IMMEDIATE block runs its statements one by one, a transaction's
  BEGIN and COMMIT dropped;
- a task is its EXECUTE IMMEDIATE block, run by `run_task()`, standing in
  for the schedule. EXECUTE TASK only records that a run was asked for,
  since in Snowflake it only schedules one, so nothing may rely on it having
  run. ALTER TASK RESUME and SUSPEND set a task's state.
"""

import base64
import json
import re
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from test_sql import connect, dynamic_table_as_view, emulate, pipe_as_insert

PARTITION_ROWS = 3
# Statements that run before an acknowledged append's rows are queryable.
VISIBLE_AFTER = 2


class Emulator:
    def __init__(self, account, user, fingerprint):
        self.conn = connect()
        self.cur = self.conn.cursor()
        self.lock = threading.Lock()
        self.sub = f"{account.split('.')[0].upper()}.{user.upper()}"
        self.fingerprint = fingerprint
        self.results = {}  # statement handle -> (columns, partitions)
        self.requests = {}  # requestId -> handle
        self.pending = set()  # handles answered 202 and not yet polled
        self.failed_once = set()  # requestIds already failed with 503
        self.stream_offset = None
        self.pipes = {}  # name -> CREATE PIPE statement
        self.scoped_tokens = set()
        self.appends = []  # (pipe, requestId, retryCount, rows, status)
        self.buffered = []  # [statements still to wait, pipe, rows], acknowledged, not yet queryable
        self.host = None  # this server's host:port, set by serve()
        self.tasks = {}  # name -> {"statements": [...], "state": ...}
        self.scheduled = []  # tasks EXECUTE TASK asked to run
        self.log = []  # every statement received, in order

    # ------------------------------------------------------------ SQL

    def query(self, sql, params=None):
        # The loader's record of a Dynamic Table holds its statement as a
        # literal, which must be stored as sent, not emulated.
        if not sql.startswith("INSERT INTO EXPORT_DYNAMIC_TABLES"):
            sql = emulate(sql)
        self.cur.execute(sql, params)
        if self.cur.description is None:
            return ["status"], [["Statement executed successfully."]]
        return [d[0] for d in self.cur.description], [list(r) for r in self.cur.fetchall()]

    def stream_view(self, upper=None):
        where = f"rowid > {self.stream_offset}"
        if upper is not None:
            where += f" AND rowid <= {upper}"
        self.cur.execute(f"CREATE OR REPLACE VIEW EXPORT_LANDING_NEW AS SELECT * FROM EXPORT_LANDING WHERE {where}")

    def run_task(self, name):
        self.run_block(self.tasks[name]["statements"])
        return ["status"], [[f"Task {name} executed."]]

    @staticmethod
    def block_statements(body):
        statements = [b.strip() for b in re.split(r";\s*\n", body) if b.strip()]
        return [b.rstrip(";") for b in statements if b not in ("BEGIN TRANSACTION", "COMMIT")]

    def run_block(self, statements):
        reads_stream = any("EXPORT_LANDING_NEW" in s for s in statements)
        upper = None
        if reads_stream:
            upper = self.cur.execute("SELECT COALESCE(MAX(rowid), -1) FROM EXPORT_LANDING").fetchall()[0][0]
            self.stream_view(upper)
        try:
            for s in statements:
                self.query(s)
        finally:
            if reads_stream:
                self.stream_offset = upper
                self.stream_view()

    def execute(self, sql):
        self.log.append(sql)
        s = sql.strip()
        if m := re.match(r"CREATE PIPE IF NOT EXISTS (\w+) AS", s):
            self.pipes.setdefault(m.group(1), s)
            return ["status"], [["Pipe created."]]
        if re.match(r"CREATE STREAM IF NOT EXISTS EXPORT_LANDING_NEW\s+ON TABLE EXPORT_LANDING APPEND_ONLY = TRUE$", s):
            if self.stream_offset is None:
                self.stream_offset = -1
                self.stream_view()
            return ["status"], [["Stream created."]]
        if m := re.match(r"CREATE TASK IF NOT EXISTS (\w+)\s.*?\bAS\s+EXECUTE IMMEDIATE \$\$\s*BEGIN\s*(.*)END;\s*\$\$$", s, re.S):
            name, body = m.groups()
            if name not in self.tasks:
                self.tasks[name] = {"statements": self.block_statements(body), "state": "suspended", "sql": s}
            return ["status"], [[f"Task {name} created."]]
        if m := re.match(r"ALTER TASK (\w+) (RESUME|SUSPEND)$", s):
            name, action = m.groups()
            if name not in self.tasks:
                raise RuntimeError(f"Task '{name}' does not exist or not authorized.")
            self.tasks[name]["state"] = "started" if action == "RESUME" else "suspended"
            return ["status"], [["Statement executed successfully."]]
        if m := re.match(r"EXECUTE TASK (\w+)$", s):
            if m.group(1) not in self.tasks:
                raise RuntimeError(f"Task '{m.group(1)}' does not exist or not authorized.")
            self.scheduled.append(m.group(1))
            return ["status"], [[f"Task {m.group(1)} is scheduled to run immediately."]]
        if m := re.match(r"EXECUTE IMMEDIATE \$\$\s*BEGIN\s*(.*)END;\s*\$\$$", s, re.S):
            self.run_block(self.block_statements(m.group(1)))
            return ["anonymous block"], [[None]]
        if s.startswith("CREATE OR REPLACE DYNAMIC TABLE"):
            return self.query(dynamic_table_as_view(s))
        return self.query(s)

    def execute_bound(self, sql, bindings):
        """A statement with `?` binds, as the SQL API takes them: every value
        text, typed by `type`."""
        self.log.append(sql)
        convert = {"TEXT": str, "FIXED": int, "REAL": float, "BOOLEAN": lambda v: v == "true"}
        params = []
        for i in range(1, len(bindings) + 1):
            b = bindings[str(i)]
            params.append(None if b["value"] is None else convert[b["type"]](b["value"]))
        assert sql.count("?") == len(params), sql
        return self.query(sql.replace("?", "%s"), params)

    # ------------------------------------------------------------ HTTP

    def check_auth(self, headers, token_type=True):
        auth = headers.get("Authorization", "")
        if token_type and headers.get("X-Snowflake-Authorization-Token-Type") != "KEYPAIR_JWT":
            return "missing key-pair token type"
        if not auth.startswith("Bearer "):
            return "missing key-pair token"
        parts = auth[len("Bearer "):].split(".")
        if len(parts) != 3:
            return "not a JWT"
        pad = lambda p: p + "=" * (-len(p) % 4)
        header = json.loads(base64.urlsafe_b64decode(pad(parts[0])))
        claims = json.loads(base64.urlsafe_b64decode(pad(parts[1])))
        if header.get("alg") != "RS256":
            return "not RS256"
        if claims.get("sub") != self.sub:
            return f"sub {claims.get('sub')!r} is not {self.sub!r}"
        if claims.get("iss") != f"{self.sub}.{self.fingerprint}":
            return f"iss {claims.get('iss')!r} does not name the key {self.fingerprint}"
        if not claims.get("exp", 0) > claims.get("iat", 0):
            return "expired"
        return None

    @staticmethod
    def text(v):
        if v is None:
            return None
        if isinstance(v, bool):
            return "true" if v else "false"
        if isinstance(v, (bytes, bytearray)):
            return bytes(v).hex()
        if isinstance(v, (dict, list)):
            return json.dumps(v)
        return str(v)

    def result(self, handle, partition=0):
        columns, parts = self.results[handle]
        if partition:
            return {"data": parts[partition]}
        return {
            "resultSetMetaData": {
                "numRows": sum(len(p) for p in parts),
                "format": "jsonv2",
                "rowType": [{"name": c, "type": "text", "nullable": True} for c in columns],
                "partitionInfo": [{"rowCount": len(p)} for p in parts],
            },
            "data": parts[0],
            "code": "090001",
            "sqlState": "00000",
            "statementHandle": handle,
            "message": "Statement executed successfully.",
            "statementStatusUrl": f"/api/v2/statements/{handle}",
        }

    def post(self, query, body):
        request_id = query.get("requestId", [None])[0]
        sql = body["statement"]
        if request_id in self.requests:
            if query.get("retry", [""])[0] != "true":
                return 422, {"code": "391918", "message": "requestId reused without retry=true"}
            return 200, self.result(self.requests[request_id])
        if sql.startswith("ALTER TASK") and request_id not in self.failed_once:
            self.failed_once.add(request_id)
            return 503, {"message": "Service Unavailable"}
        handle = str(uuid.uuid4())
        with self.lock:
            self.make_visible()
            try:
                if "bindings" in body:
                    columns, rows = self.execute_bound(sql, body["bindings"])
                else:
                    columns, rows = self.execute(sql)
            except Exception as e:  # noqa: BLE001 -- relayed to the client as a failed statement
                return 422, {"code": "002003", "sqlState": "42000", "message": str(e), "statementHandle": handle}
        rows = [[self.text(v) for v in r] for r in rows]
        parts = [rows[i:i + PARTITION_ROWS] for i in range(0, len(rows), PARTITION_ROWS)] or [[]]
        self.results[handle] = (columns, parts)
        if request_id:
            self.requests[request_id] = handle
        if sql.startswith("CREATE OR REPLACE DYNAMIC TABLE"):
            self.pending.add(handle)
            return 202, {
                "code": "333334",
                "message": "Asynchronous execution in progress.",
                "statementHandle": handle,
                "statementStatusUrl": f"/api/v2/statements/{handle}",
            }
        return 200, self.result(handle)

    # ------------------------------------------------------- streaming

    def hostname(self):
        return 200, self.host

    def scoped_token(self, form):
        if form.get("grant_type") != ["urn:ietf:params:oauth:grant-type:jwt-bearer"]:
            return 400, {"message": f"bad grant_type {form.get('grant_type')}"}
        if form.get("scope") != [self.host]:
            return 400, {"message": f"scope {form.get('scope')} is not {self.host}"}
        token = f"scoped-{uuid.uuid4()}"
        self.scoped_tokens.add(token)
        return 200, token

    def append(self, db, schema, pipe, query, body):
        request_id = query.get("requestId", [None])[0]
        retry = int(query.get("retryCount", ["0"])[0])
        rows = [json.loads(line) for line in body.decode().splitlines() if line.strip()]
        seen = [a for a in self.appends if a[1] == request_id]
        # retryCount counts the requests sent before under this id, and a
        # request the emulator never received (the client gave up waiting
        # before it arrived, say) is one the emulator did not see.
        if request_id is None or retry < len(seen):
            status = 400
        elif not self.appends:
            status = 503
        elif (db, schema) != ("EXPORT", "CELLS") or pipe not in self.pipes:
            status = 404
        else:
            status = 200
        self.appends.append((pipe, request_id, retry, rows, status))
        if status == 200:
            with self.lock:
                self.buffered.append([VISIBLE_AFTER, pipe, rows])
            return 200, {"message": "OK"}
        return status, {"code": "EMULATED", "message": f"append answered {status}"}

    def make_visible(self, everything=False):
        """Count a statement against each buffered append, and land the
        rows of those whose wait is over."""
        waiting = []
        for b in self.buffered:
            b[0] -= 1
            if b[0] <= 0 or everything:
                self.cur.execute(emulate(pipe_as_insert(self.pipes[b[1]])), (json.dumps(b[2]),))
            else:
                waiting.append(b)
        self.buffered = waiting

    def get(self, handle, query):
        if handle not in self.results:
            return 404, {"message": "no such statement"}
        if handle in self.pending:
            self.pending.discard(handle)
        return 200, self.result(handle, int(query.get("partition", ["0"])[0]))


def serve(emulator):
    """Serve `emulator` on a free local port; returns the server, whose
    `server_address` names the port. Stop it with `shutdown()`."""

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, doc):
            text = isinstance(doc, str)
            data = doc.encode() if text else json.dumps(doc).encode()
            self.send_response(status)
            self.send_header("Content-Type", "text/plain" if text else "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            url = urlparse(self.path)
            raw = self.rfile.read(int(self.headers["Content-Length"]))
            if m := re.fullmatch(
                r"/v2/streaming/data/databases/(\w+)/schemas/(\w+)/pipes/(\w+)/channels/ELASTIC/rows",
                url.path,
            ):
                if self.headers.get("Authorization", "")[len("Bearer "):] not in emulator.scoped_tokens:
                    return self.reply(401, {"message": "not a scoped token"})
                if self.headers.get("Content-Type") != "application/x-ndjson":
                    return self.reply(415, {"message": "not NDJSON"})
                return self.reply(*emulator.append(*m.groups(), parse_qs(url.query), raw))
            if err := emulator.check_auth(self.headers, token_type=url.path != "/oauth/token"):
                return self.reply(401, {"code": "390144", "message": err})
            if url.path == "/oauth/token":
                return self.reply(*emulator.scoped_token(parse_qs(raw.decode())))
            if url.path != "/api/v2/statements":
                return self.reply(404, {"message": "not found"})
            self.reply(*emulator.post(parse_qs(url.query), json.loads(raw)))

        def do_GET(self):
            url = urlparse(self.path)
            if url.path == "/v2/streaming/hostname":
                if err := emulator.check_auth(self.headers):
                    return self.reply(401, {"code": "390144", "message": err})
                return self.reply(*emulator.hostname())
            m = re.fullmatch(r"/api/v2/statements/([\w-]+)", url.path)
            if not m:
                return self.reply(404, {"message": "not found"})
            if err := emulator.check_auth(self.headers):
                return self.reply(401, {"code": "390144", "message": err})
            self.reply(*emulator.get(m.group(1), parse_qs(url.query)))

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    emulator.host = f"127.0.0.1:{server.server_address[1]}"
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
