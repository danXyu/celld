"""Run the celld-export-loader binary against the SQL API emulator.

Each scenario deploys from nothing with `deploy`, delivers half its records
through the pipe and the route task and the other half with `load`, syncs
the Dynamic Tables with `sync`, and checks everything the SQL derives
against the reference consumer, exactly as test_sql.py does. The loader's
own statements are what run: the emulator only stands in for what fakesnow
lacks (see sqlapi.py).
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
from sqlapi import NOTIFICATION_CHANNEL, Emulator, serve
from real_account import files, sink_row, verify
from test_sql import Warehouse, scenario_names, tombstone

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
        "EXPORT_STAGE_URL": "s3://fleet-bucket/export/changes/",
        "EXPORT_STORAGE_INTEGRATION": "CELLD_EXPORT_S3",
    }

    def run(*args, ok=True):
        p = subprocess.run([str(binary), *args], env=env, capture_output=True, text=True, timeout=120)
        if ok:
            assert p.returncode == 0, p.stderr
        return p

    return run


def stage(emu, path, rows):
    """Stage `rows` as one file, as the bucket sink writes them."""
    for row in rows:
        emu.cur.execute("INSERT INTO EXPORT_TEST_STAGE SELECT %s, PARSE_JSON(%s)", (path, json.dumps(sink_row(row))))


@pytest.mark.parametrize("name", scenario_names())
def test_loader_end_to_end(emulator, loader, scenarios, name):
    s = scenarios[name]
    out = loader("deploy").stdout
    assert NOTIFICATION_CHANNEL in out
    assert {t: v["state"] for t, v in emulator.tasks.items()} == {
        "EXPORT_ROUTE": "started",
        "EXPORT_ERASE": "started",
    }
    w = Warehouse(emulator.cur)
    for t in s["tombstones"]:
        tombstone(w, t)

    # Half the records arrive as files the pipe ingests and the route task
    # routes on its schedule, the rest as repair files `load` copies.
    rows = s["stage_rows"]
    half = len(rows) // 2
    stage(emulator, "node-a/2026/09/29/02/1790000000000000-a.parquet", rows[:half])
    emulator.ingest()
    emulator.run_task("EXPORT_ROUTE")
    stage(emulator, "repair/node-b/1790000000000001-b.parquet", rows[half:])
    stage(emulator, "repair/node-b/1790000000000002-b.json", rows[half:])
    out = loader("load", "repair/").stdout
    assert "repair/node-b/1790000000000001-b.parquet\tLOADED" in out
    assert ".json" not in out
    # A file already loaded is not loaded again.
    assert "LOADED" not in loader("load", "repair/").stdout
    assert emulator.ingest()[1] == [["Copy executed with 0 files processed."]]

    out = loader("sync").stdout
    assert "failed" not in out
    verify(w, s)

    # Nothing changed, so nothing is replaced.
    out = loader("sync").stdout
    assert "created" not in out and "replaced" not in out

    # The deployment is idempotent and leaves the data alone.
    loader("deploy")
    verify(w, s)


def test_erase(emulator, loader, scenarios):
    s = scenarios["basic"]
    loader("deploy")
    stage(emulator, "node-a/1.parquet", s["stage_rows"])
    emulator.ingest()
    emulator.run_task("EXPORT_ROUTE")
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
    # A reloaded file does not bring the erased stream back.
    stage(emulator, "repair/again.parquet", s["stage_rows"])
    loader("load", "repair/")
    assert not w.rows(f"SELECT * FROM CELL_CHANGES WHERE cell = '{victim}'")


def test_read_side_and_errors(emulator, loader):
    loader("deploy")
    gaps = loader("gaps").stdout.splitlines()
    assert gaps[0].split("\t")[:5] == ["SCRIPT", "CLASS", "CELL", "FACET", "INCARNATION"]
    assert loader("certified").stdout.splitlines()[0].startswith("SCRIPT\t")
    p = loader("load", "../outside", ok=False)
    assert p.returncode != 0 and "stage path" in p.stderr
    # A statement Snowflake rejects surfaces its message.
    emulator.cur.execute("DROP VIEW EXPORT_GAPS")
    p = loader("gaps", ok=False)
    assert p.returncode != 0 and "EXPORT_GAPS" in p.stderr


def test_a_wrong_key_is_refused(emulator, loader, key, tmp_path, binary):
    other = tmp_path / "other.p8"
    subprocess.run(["openssl", "genpkey", "-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048",
                    "-out", str(other)], check=True, capture_output=True)
    env = dict(os.environ, SNOWFLAKE_ACCOUNT=ACCOUNT, SNOWFLAKE_USER=USER,
               SNOWFLAKE_PRIVATE_KEY_FILE=str(other), SNOWFLAKE_DATABASE="EXPORT",
               SNOWFLAKE_SCHEMA="CELLS", SNOWFLAKE_WAREHOUSE="EXPORT_WH", SNOWFLAKE_URL=emulator.url)
    p = subprocess.run([str(binary), "gaps"], env=env, capture_output=True, text=True, timeout=120)
    assert p.returncode != 0 and "does not name the key" in p.stderr



def test_real_account_files_have_the_sinks_layout(tmp_path):
    import pyarrow.parquet as pq

    files(tmp_path)
    schema = pq.ParquetFile(tmp_path / "edge.parquet").schema
    types = {schema.column(i).name: str(schema.column(i).logical_type) for i in range(len(schema))}
    assert types["incarnation"] == "Int(bitWidth=64, isSigned=false)"
    assert types["fragment"] == "Int(bitWidth=32, isSigned=false)"
    assert types["committed_at"].startswith("Timestamp(isAdjustedToUTC=true, timeUnit=milliseconds")
    assert types["body"] == "String"
    row = pq.read_table(tmp_path / "basic.parquet").to_pylist()[0]
    assert json.loads(row["body"])["kind"] == row["kind"]


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
