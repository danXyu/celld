"""Check the export on a real Snowflake account, where the emulator cannot:
the landing insert's JSON casts, the stream, the tasks, and the Dynamic
Tables' refresh. The README's "Verifying on a real account" says when to run
each command.

    python real_account.py files OUTDIR
        Write each scenario's records as JSON lines, as `celld export
        inspect` prints them and `celld-export-loader ingest` reads them
        (OUTDIR/<scenario>.jsonl), and OUTDIR/edge.jsonl, one record whose
        unsigned fields hold their largest values.

    python real_account.py check SCENARIO
        Compare what the account derived from SCENARIO's records with what
        the reference consumer derives, as the emulator tests do, and print
        each Dynamic Table's refresh mode. Connects with SNOWFLAKE_ACCOUNT,
        SNOWFLAKE_USER, SNOWFLAKE_PRIVATE_KEY_FILE, SNOWFLAKE_ROLE (optional),
        SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA and SNOWFLAKE_WAREHOUSE.
"""

import copy
import json
import os
import subprocess
import sys
from pathlib import Path

from conftest import CRATE, RANDOM_COUNT
from test_sql import Warehouse, check

TOP64, TOP32 = 2**64 - 1, 2**32 - 1

# A record at the edge of every unsigned field: a watermark, since it has
# no rows to project.
EDGE = {
    "kind": "watermark", "script": "edge", "class": "Edge", "cell": "e1",
    "cell_name": None, "facet": None, "incarnation": TOP64, "epoch": TOP64, "txid": TOP64,
    "commit": TOP64, "committed_at": 1_790_000_000_123, "node": "n", "origin": "live",
    "fragment": TOP32, "fragments": TOP32,
    "from": None, "through": {"epoch": TOP64, "txid": TOP64, "commit": TOP64},
    "commits": TOP64, "records": TOP64,
}


def write_jsonl(records, path):
    Path(path).write_text("".join(json.dumps(r) + "\n" for r in records))


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
        write_jsonl(s["records"], outdir / f"{name}.jsonl")
        (outdir / f"{name}.tombstones.json").write_text(json.dumps(s["tombstones"]))
    write_jsonl([EDGE], outdir / "edge.jsonl")


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
