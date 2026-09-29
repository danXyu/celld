"""Check the export on a real Snowflake account, where the emulator cannot:
the stage, the pipe's auto-ingest, the stream, the tasks, and the Dynamic
Tables' refresh. The README's "Verifying on a real account" says when to
run each command.

    python real_account.py files OUTDIR
        Write each scenario's records as a Parquet file in the bucket sink's
        layout (OUTDIR/<scenario>.parquet), and OUTDIR/edge.parquet, one row
        whose unsigned columns hold 2^64-1.

    python real_account.py check SCENARIO
        Compare what the account derived from SCENARIO's file with what the
        reference consumer derives, as the emulator tests do, and print each
        Dynamic Table's refresh mode. Connects with SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_FILE, SNOWFLAKE_ROLE (optional),
        SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA and SNOWFLAKE_WAREHOUSE.
"""

import copy
import json
import os
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from conftest import CRATE, RANDOM_COUNT
from test_sql import Warehouse, check

# crates/celld/export_sink.rs, MESSAGE_TYPE.
SINK_SCHEMA = pa.schema([
    pa.field("kind", pa.string(), nullable=False),
    pa.field("script", pa.string(), nullable=False),
    pa.field("class", pa.string(), nullable=False),
    pa.field("cell", pa.string(), nullable=False),
    pa.field("cell_name", pa.string()),
    pa.field("facet", pa.string()),
    pa.field("incarnation", pa.uint64(), nullable=False),
    pa.field("epoch", pa.uint64(), nullable=False),
    pa.field("txid", pa.uint64(), nullable=False),
    pa.field("commit", pa.uint64(), nullable=False),
    pa.field("committed_at", pa.timestamp("ms", tz="UTC"), nullable=False),
    pa.field("node", pa.string(), nullable=False),
    pa.field("origin", pa.string(), nullable=False),
    pa.field("fragment", pa.uint32(), nullable=False),
    pa.field("fragments", pa.uint32(), nullable=False),
    pa.field("body", pa.string(), nullable=False),
])


def sink_row(stage_row):
    """A stage row as the bucket sink writes it: its body also carries `kind`."""
    row = dict(stage_row)
    row["body"] = json.dumps({"kind": row["kind"], **json.loads(row["body"])})
    return row


def write_parquet(rows, path):
    columns = {f.name: [r[f.name] for r in rows] for f in SINK_SCHEMA}
    pq.write_table(pa.Table.from_pydict(columns, schema=SINK_SCHEMA), path, compression="zstd")


def scenarios():
    out = subprocess.run(
        ["cargo", "run", "-q", "-p", "celld-export-snowflake", "--example", "scenarios", RANDOM_COUNT],
        check=True,
        capture_output=True,
        cwd=CRATE,
    ).stdout
    return {s["name"]: s for s in json.loads(out)["scenarios"]}


def files(outdir):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    for name, s in scenarios().items():
        write_parquet([sink_row(r) for r in s["stage_rows"]], outdir / f"{name}.parquet")
        (outdir / f"{name}.tombstones.json").write_text(json.dumps(s["tombstones"]))
    top = 2**64 - 1
    write_parquet(
        [{
            "kind": "heartbeat", "script": "edge", "class": "Edge", "cell": "e1",
            "cell_name": None, "facet": None, "incarnation": top, "epoch": top, "txid": top,
            "commit": top, "committed_at": 1_790_000_000_123, "node": "n", "origin": "live",
            "fragment": 2**32 - 1, "fragments": 2**32 - 1, "body": '{"kind":"heartbeat"}',
        }],
        outdir / "edge.parquet",
    )


def with_loader_names(w, scenario):
    """The scenario with each Dynamic Table named as the loader named it.
    A table the loader made no Dynamic Table for keeps no entry, so its
    expected rows, if any, fail the check."""
    names = {
        (r["script"], r["class"], r["table_name"]): r["name"]
        for r in w.rows("SELECT * FROM EXPORT_DYNAMIC_TABLES")
    }
    s = copy.deepcopy(scenario)
    s["dynamic_tables"] = [
        dict(dt, name=names[(dt["script"], dt["class"], dt["table"])])
        for dt in s["dynamic_tables"]
        if (dt["script"], dt["class"], dt["table"]) in names
    ]
    return s


def verify(w, scenario):
    check(w, with_loader_names(w, scenario))


def main(argv):
    if len(argv) == 2 and argv[0] == "files":
        files(argv[1])
    elif len(argv) == 2 and argv[0] == "check":
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_FILE"],
            role=os.environ.get("SNOWFLAKE_ROLE"),
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
            warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        )
        w = Warehouse(conn.cursor())
        verify(w, scenarios()[argv[1]])
        print(f"{argv[1]}: every view and Dynamic Table matches the reference consumer")
        for r in w.rows("SHOW DYNAMIC TABLES"):
            print(r["name"], r.get("refresh_mode"), r.get("refresh_mode_reason") or "")
    else:
        sys.exit(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
