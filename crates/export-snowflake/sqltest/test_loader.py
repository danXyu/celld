"""Run the celld-export-loader binary against the SQL API emulator.

Each scenario deploys from nothing with `deploy`, lands its records through
Snowpipe Streaming with `ingest` (half from a file, half from stdin as
`celld export inspect` prints them, then the first half again as a replay
would), syncs the Dynamic Tables with `sync`, and checks everything the SQL
derives against the reference consumer, exactly as test_sql.py does.
`ingest` lands through the same `Batch` and Snowpipe Streaming client as
`run`, whose blob-stream loop the crate's Rust tests cover. The loader's own
statements and requests are what run: the emulator only stands in for what
fakesnow lacks (see sqlapi.py).
"""

import json
import base64
import os
import shutil
import subprocess
from pathlib import Path

import fakesnow
import pytest

from conftest import CRATE
from sqlapi import Emulator, serve
from real_account import EDGE, files, verify, write_jsonl
from test_sql import Warehouse, emulate, pipe_as_insert, scenario_names, tombstone

ROOT = CRATE.parent.parent
ACCOUNT, USER = "xy12345.us-east-2.aws", "celld_loader"


@pytest.fixture(scope="session")
def binary():
    subprocess.run(
        ["cargo", "build", "-q", "-p", "celld-export-snowflake", "--features", "sql-api",
         "--bin", "celld-export-loader"],
        check=True,
        cwd=ROOT,
    )
    target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target"))
    return target / "debug" / "celld-export-loader"


@pytest.fixture(scope="session")
def key(tmp_path_factory):
    """A key pair and its fingerprint, the way Snowflake's documentation
    computes it: the SHA-256 of the public key's DER, in base64."""
    if not shutil.which("openssl"):
        pytest.skip("openssl is not installed")
    d = tmp_path_factory.mktemp("key")
    pem = d / "rsa_key.p8"
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048",
                    "-out", str(pem)], check=True, capture_output=True)
    der = subprocess.run(["openssl", "pkey", "-in", str(pem), "-pubout", "-outform", "DER"],
                         check=True, capture_output=True).stdout
    digest = subprocess.run(["openssl", "dgst", "-sha256", "-binary"], input=der,
                            check=True, capture_output=True).stdout
    return pem, "SHA256:" + base64.b64encode(digest).decode()


@pytest.fixture
def emulator(key):
    with fakesnow.patch():
        emu = Emulator(ACCOUNT, USER, key[1])
        server = serve(emu)
        emu.url = f"http://127.0.0.1:{server.server_address[1]}"
        yield emu
        server.shutdown()
        emu.conn.close()


@pytest.fixture
def loader(binary, key, emulator):
    env = {
        "PATH": os.environ.get("PATH", ""),
        "SNOWFLAKE_ACCOUNT": ACCOUNT,
        "SNOWFLAKE_USER": USER,
        "SNOWFLAKE_PRIVATE_KEY_FILE": str(key[0]),
        "SNOWFLAKE_DATABASE": "EXPORT",
        "SNOWFLAKE_SCHEMA": "CELLS",
        "SNOWFLAKE_WAREHOUSE": "EXPORT_WH",
        "SNOWFLAKE_URL": emulator.url,
        # Small batches, so a scenario lands in several appends.
        "EXPORT_BATCH_RECORDS": "7",
    }

    def run(*args, ok=True, stdin=None):
        p = subprocess.run([str(binary), *args], env=env, capture_output=True, text=True, timeout=120,
                           input=stdin)
        if ok:
            assert p.returncode == 0, p.stderr
        return p

    return run


def inspect_lines(records):
    """`records` as `celld export inspect` prints them: each with its object."""
    return "".join(json.dumps(dict(r, object=f"export/changes/node-b/{i}.parquet")) + "\n"
                   for i, r in enumerate(records))


@pytest.mark.parametrize("name", scenario_names())
def test_loader_end_to_end(emulator, loader, scenarios, tmp_path, name):
    s = scenarios[name]
    loader("deploy")
    assert {t: v["state"] for t, v in emulator.tasks.items()} == {
        "EXPORT_ROUTE": "started",
        "EXPORT_ERASE": "started",
    }
    assert list(emulator.pipes) == ["EXPORT_LANDING_PIPE"]
    w = Warehouse(emulator.cur)
    for t in s["tombstones"]:
        tombstone(w, t)

    records = s["records"]
    half = len(records) // 2
    first = tmp_path / "first.jsonl"
    write_jsonl(records[:half], first)
    out = loader("ingest", str(first)).stdout
    assert f"landed and routed {half} records" in out
    loader("ingest", "-", stdin=inspect_lines(records[half:]))
    # A replayed batch changes nothing the views derive.
    loader("ingest", str(first))
    landed = w.rows("SELECT source FROM EXPORT_LANDING")
    assert len(landed) == len(records) + half
    if half:
        assert {"export/changes/node-b/0.parquet", f"{first}:1"} <= {r["source"] for r in landed}
    # Every append carried at most one batch, and the one that failed was
    # sent again under its request id.
    ok = [a for a in emulator.appends if a[4] == 200]
    assert all(len(a[3]) <= 7 for a in ok)
    assert sum(len(a[3]) for a in ok) == len(records) + half
    failed = [a for a in emulator.appends if a[4] != 200]
    assert [(a[4], a[2]) for a in failed] == ([(503, 0)] if records else [])
    if failed:
        assert any(a[1] == failed[0][1] and a[2] == 1 and a[4] == 200 for a in emulator.appends)
    # `ingest` routes before it returns; the task was never asked to.
    assert emulator.scheduled == []

    out = loader("sync").stdout
    assert "failed" not in out
    verify(w, s)

    # Nothing changed, so nothing is replaced.
    out = loader("sync").stdout
    assert "created" not in out and "replaced" not in out

    # The deployment is idempotent and leaves the data alone.
    loader("deploy")
    verify(w, s)


def test_the_route_task_routes_what_lands(emulator, loader, scenarios):
    """What `run` lands reaches the tables on the route task's schedule."""
    s = scenarios["basic"]
    loader("deploy")
    w = Warehouse(emulator.cur)
    emulator.cur.execute(emulate(pipe_as_insert(emulator.pipes["EXPORT_LANDING_PIPE"])),
                         (json.dumps(s["landing_rows"]),))
    assert not w.rows("SELECT * FROM CELL_CHANGES")
    emulator.run_task("EXPORT_ROUTE")
    loader("sync")
    verify(w, s)


def test_erase(emulator, loader, scenarios):
    s = scenarios["basic"]
    lines = "".join(json.dumps(r) + "\n" for r in s["records"])
    loader("deploy")
    loader("ingest", "-", stdin=lines)
    loader("sync")
    w = Warehouse(emulator.cur)
    cells = {r["cell"] for r in w.rows("SELECT DISTINCT cell FROM CELL_CHANGES")}
    victim = sorted(cells)[0]
    loader("erase", "app", "Room", victim, "--reason", "test")
    loader("erase", "app", "Room", victim)  # a second erase adds no tombstone
    assert len(w.rows("SELECT * FROM EXPORT_TOMBSTONES")) == 1
    # The rows are gone when erase returns, not when a scheduled run gets to it.
    assert emulator.scheduled == []
    assert not w.rows(f"SELECT * FROM CELL_CHANGES WHERE cell = '{victim}'")
    assert not w.rows(f"SELECT * FROM CELL_META WHERE cell = '{victim}'")
    assert not w.rows(f"SELECT * FROM CELL_STREAMS WHERE cell = '{victim}' AND NOT removed")
    # Records replayed from the topic do not bring the erased stream back.
    loader("ingest", "-", stdin=lines)
    assert not w.rows(f"SELECT * FROM CELL_CHANGES WHERE cell = '{victim}'")


def test_read_side_and_errors(emulator, loader, scenarios):
    loader("deploy")
    gaps = loader("gaps").stdout.splitlines()
    assert gaps[0].split("\t")[:5] == ["SCRIPT", "CLASS", "CELL", "FACET", "INCARNATION"]
    assert loader("certified").stdout.splitlines()[0].startswith("SCRIPT\t")
    # A line that is not a record is reported and fails the command, after
    # the records around it land.
    good = json.dumps(scenarios["basic"]["records"][0])
    p = loader("ingest", "-", stdin=f"{good}\nnot json\n{good}\n", ok=False)
    assert p.returncode != 0 and "-:2: not a record" in p.stderr
    assert "landed and routed 2 records" in p.stdout
    # This binary has no blob-stream consumer, and says how to get one.
    p = loader("run", ok=False)
    assert p.returncode != 0 and "blob-stream feature" in p.stderr
    # A statement Snowflake rejects surfaces its message.
    emulator.cur.execute("DROP VIEW EXPORT_GAPS")
    p = loader("gaps", ok=False)
    assert p.returncode != 0 and "EXPORT_GAPS" in p.stderr
    # So does an append Snowpipe Streaming rejects.
    emulator.pipes.clear()
    p = loader("ingest", "-", stdin=good + "\n", ok=False)
    assert p.returncode != 0 and "Snowpipe Streaming answered 404" in p.stderr


def test_a_wrong_key_is_refused(emulator, loader, key, tmp_path, binary):
    other = tmp_path / "other.p8"
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048",
                    "-out", str(other)], check=True, capture_output=True)
    env = dict(os.environ, SNOWFLAKE_ACCOUNT=ACCOUNT, SNOWFLAKE_USER=USER,
               SNOWFLAKE_PRIVATE_KEY_FILE=str(other), SNOWFLAKE_DATABASE="EXPORT",
               SNOWFLAKE_SCHEMA="CELLS", SNOWFLAKE_WAREHOUSE="EXPORT_WH", SNOWFLAKE_URL=emulator.url)
    p = subprocess.run([str(binary), "gaps"], env=env, capture_output=True, text=True, timeout=120)
    assert p.returncode != 0 and "does not name the key" in p.stderr



def test_real_account_files_land_and_keep_their_unsigned_values(emulator, loader, tmp_path):
    files(tmp_path)
    for line in (tmp_path / "basic.jsonl").read_text().splitlines():
        assert "kind" in json.loads(line)
    loader("deploy")
    loader("ingest", str(tmp_path / "edge.jsonl"))
    w = Warehouse(emulator.cur)
    row = w.rows("SELECT * FROM CELL_META WHERE script = 'edge'")[0]
    assert (int(row["incarnation"]), int(row["txid"]), int(row["fragment"])) == (
        EDGE["incarnation"], EDGE["txid"], EDGE["fragment"])


def test_bound_statements_as_the_reconciler_runs_them(emulator, loader):
    """Statements shaped like #49's (crates/celld/export_audit/snowflake.rs):
    `?` binds, NULL among them, through the SQL API's bindings."""
    loader("deploy")
    loader(
        "query",
        "INSERT INTO EXPORT_TOMBSTONES (script, class, cell, facet, incarnation, erased_at, reason) "
        "SELECT ?, ?, ?, ?, ?, TO_TIMESTAMP_LTZ(?, 3), ?",
        '"app"', '"Room"', '"r9"', '""', "null", "1790000000123", '"test"',
    )
    loader(
        "query",
        "UPDATE EXPORT_TOMBSTONES SET cleared_at = TO_TIMESTAMP_LTZ(?, 3) "
        "WHERE script = ? AND class = ? AND cell = ? AND facet = ? "
        "AND EQUAL_NULL(incarnation, ?) AND cleared_at IS NULL",
        "1790000000999", '"app"', '"Room"', '"r9"', '""', "null",
    )
    out = loader("query", "SELECT cell, reason FROM EXPORT_TOMBSTONES WHERE cleared_at IS NOT NULL AND ?", "true").stdout
    assert out.splitlines() == ["CELL\tREASON", "r9\ttest"]
