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

- a stage is EXPORT_TEST_STAGE (path, c1): one row per record, `path` the
  file's path under the stage URL. COPY INTO reads the rows under the stage
  path it names whose path matches PATTERN and keeps a load history, so a
  file loads once, as COPY's does;
- the pipe runs its COPY when the test calls `ingest()`, standing in for a
  bucket notification;
- the stream EXPORT_LANDING_NEW is a view over EXPORT_LANDING's rows past an
  offset, advanced when a task that read it commits;
- a task is its EXECUTE IMMEDIATE body, run statement by statement by
  EXECUTE TASK or `run_task()`, standing in for the schedule; ALTER TASK
  RESUME and SUSPEND set its state;
- SHOW PIPES answers one row with a made-up notification channel.
"""

import base64
import json
import re
import threading
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from test_sql import connect, dynamic_table_as_view, emulate

PARTITION_ROWS = 3
NOTIFICATION_CHANNEL = "arn:aws:sqs:us-east-1:000000000000:sf-snowpipe-emulator"


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
        self.stage_url = None
        self.pipe_copy = None
        self.stream_offset = None
        self.loaded_files = set()
        self.tasks = {}  # name -> {"statements": [...], "state": ...}
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

    def copy(self, sql):
        m = re.match(
            r"COPY INTO (\w+) \((.*?)\)\s*FROM \((.*)FROM @EXPORT_STAGE(/[^\s)]*)?\s*\)\s*PATTERN = '([^']*)'$",
            sql,
            re.S,
        )
        assert m, sql
        table, columns, select, path, pattern = m.groups()
        prefix = (path or "/")[1:]
        _, rows = self.query("SELECT DISTINCT path FROM EXPORT_TEST_STAGE")
        files = sorted(
            p for (p,) in rows
            if p.startswith(prefix) and re.fullmatch(pattern, p) and p not in self.loaded_files
        )
        out = []
        for f in files:
            select_f = select.replace("$1:", "c1:").replace("METADATA$FILENAME", "path")
            self.cur.execute(
                emulate(f"INSERT INTO {table} ({columns}) {select_f} FROM EXPORT_TEST_STAGE WHERE path = %s"),
                (f,),
            )
            n = self.cur.fetchall()[0][0]
            self.loaded_files.add(f)
            out.append([f, "LOADED", n, n, 1, 0, None, None, None, None])
        if not files:
            return ["status"], [["Copy executed with 0 files processed."]]
        columns = ["file", "status", "rows_parsed", "rows_loaded", "error_limit", "errors_seen",
                   "first_error", "first_error_line", "first_error_character", "first_error_column_name"]
        return columns, out

    def run_task(self, name):
        task = self.tasks[name]
        reads_stream = any("EXPORT_LANDING_NEW" in s for s in task["statements"])
        upper = None
        if reads_stream:
            upper = self.cur.execute("SELECT COALESCE(MAX(rowid), -1) FROM EXPORT_LANDING").fetchall()[0][0]
            self.stream_view(upper)
        try:
            for s in task["statements"]:
                self.query(s)
        finally:
            if reads_stream:
                self.stream_offset = upper
                self.stream_view()
        return ["status"], [[f"Task {name} executed."]]

    def ingest(self):
        """The pipe's COPY, as a bucket notification would run it."""
        with self.lock:
            return self.copy(self.pipe_copy)

    def execute(self, sql):
        self.log.append(sql)
        s = sql.strip()
        if re.match(r"CREATE FILE FORMAT\b", s):
            return ["status"], [["File format created."]]
        if m := re.match(r"CREATE STAGE IF NOT EXISTS EXPORT_STAGE\s+URL = '([^']*)'", s):
            if self.stage_url is None:
                self.stage_url = m.group(1)
                self.cur.execute("CREATE TABLE EXPORT_TEST_STAGE (path STRING, c1 VARIANT)")
            return ["status"], [["Stage created."]]
        if m := re.match(r"CREATE PIPE IF NOT EXISTS EXPORT_PIPE AUTO_INGEST = TRUE AS\s*(.*)$", s, re.S):
            self.pipe_copy = self.pipe_copy or m.group(1)
            return ["status"], [["Pipe created."]]
        if re.match(r"CREATE STREAM IF NOT EXISTS EXPORT_LANDING_NEW\s+ON TABLE EXPORT_LANDING APPEND_ONLY = TRUE$", s):
            if self.stream_offset is None:
                self.stream_offset = -1
                self.stream_view()
            return ["status"], [["Stream created."]]
        if m := re.match(r"CREATE TASK IF NOT EXISTS (\w+)\s.*?\bAS\s+EXECUTE IMMEDIATE \$\$\s*BEGIN\s*(.*)END;\s*\$\$$", s, re.S):
            name, body = m.groups()
            if name not in self.tasks:
                statements = [b.strip() for b in re.split(r";\s*\n", body) if b.strip()]
                statements = [b.rstrip(";") for b in statements if b not in ("BEGIN TRANSACTION", "COMMIT")]
                self.tasks[name] = {"statements": statements, "state": "suspended", "sql": s}
            return ["status"], [[f"Task {name} created."]]
        if m := re.match(r"ALTER TASK (\w+) (RESUME|SUSPEND)$", s):
            name, action = m.groups()
            if name not in self.tasks:
                raise RuntimeError(f"Task '{name}' does not exist or not authorized.")
            self.tasks[name]["state"] = "started" if action == "RESUME" else "suspended"
            return ["status"], [["Statement executed successfully."]]
        if m := re.match(r"EXECUTE TASK (\w+)$", s):
            return self.run_task(m.group(1))
        if re.match(r"SHOW PIPES LIKE 'EXPORT_PIPE'$", s):
            columns = ["created_on", "name", "database_name", "schema_name", "definition", "owner",
                       "notification_channel", "comment"]
            if self.pipe_copy is None:
                return columns, []
            return columns, [[None, "EXPORT_PIPE", "EXPORT", "CELLS", self.pipe_copy, "LOADER",
                              NOTIFICATION_CHANNEL, ""]]
        if s.startswith("COPY INTO "):
            return self.copy(s)
        if s.startswith("CREATE OR REPLACE DYNAMIC TABLE"):
            return self.query(dynamic_table_as_view(s))
        return self.query(s)

    # ------------------------------------------------------------ HTTP

    def check_auth(self, headers):
        auth = headers.get("Authorization", "")
        if headers.get("X-Snowflake-Authorization-Token-Type") != "KEYPAIR_JWT" or not auth.startswith("Bearer "):
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
            try:
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
            data = json.dumps(doc).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def do_POST(self):
            url = urlparse(self.path)
            if url.path != "/api/v2/statements":
                return self.reply(404, {"message": "not found"})
            if err := emulator.check_auth(self.headers):
                return self.reply(401, {"code": "390144", "message": err})
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            self.reply(*emulator.post(parse_qs(url.query), body))

        def do_GET(self):
            url = urlparse(self.path)
            m = re.fullmatch(r"/api/v2/statements/([\w-]+)", url.path)
            if not m:
                return self.reply(404, {"message": "not found"})
            if err := emulator.check_auth(self.headers):
                return self.reply(401, {"code": "390144", "message": err})
            self.reply(*emulator.get(m.group(1), parse_qs(url.query)))

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server
