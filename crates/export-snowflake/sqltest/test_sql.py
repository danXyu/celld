"""Run the export's Snowflake SQL against synthetic records.

The records, the rendered Dynamic Tables, and what the reference consumer
(crates/export-format) derives from the same records come from the
`scenarios` example. Each scenario loads its records into a fresh fakesnow
database (Snowflake SQL translated onto DuckDB) through the same COPY
transformation and routing statements a deployment runs, creates the views
and the Dynamic Tables, and checks that every derived answer equals the
consumer's.

What fakesnow cannot run is replaced, and nothing else:

- stages, pipes, streams, tasks: the COPY's SELECT reads a table of stage
  rows instead of @EXPORT_STAGE, and EXPORT_LANDING_NEW is a view over
  EXPORT_LANDING instead of a stream;
- a Dynamic Table is created as a view over the same query;
- ARRAY_POSITION and TRY_BASE64_DECODE_BINARY, which fakesnow does not
  translate, and TO_VARCHAR, which it translates keeping a JSON string's
  quotes, are DuckDB macros with Snowflake's semantics;
- `$` in a string or path is not a session variable here, so fakesnow's
  variable substitution is off.
"""

import json
import os
import re
import subprocess
from pathlib import Path

import fakesnow
import fakesnow.variables
import pytest
import snowflake.connector

CRATE = Path(__file__).resolve().parent.parent
SQL = CRATE / "sql"
RANDOM_COUNT = os.environ.get("EXPORT_SQLTEST_RANDOM", "24")

fakesnow.variables.Variables.inline_variables = lambda self, sql: sql


def statements(path):
    """The named statements of a SQL file, as crate::statements splits them."""
    out, name, lines = {}, None, []
    for line in path.read_text().splitlines():
        if line.startswith("-- statement:"):
            if name:
                out[name] = finish(lines)
            name, lines = line.split(":", 1)[1].strip(), []
        elif name:
            lines.append(line)
    if name:
        out[name] = finish(lines)
    return out


def finish(lines):
    while lines and (lines[0].lstrip().startswith("--") or not lines[0].strip()):
        lines = lines[1:]
    return "\n".join(lines).rstrip().rstrip(";").rstrip()


TABLES = statements(SQL / "tables.sql")
LOAD = statements(SQL / "load.sql")
VIEWS = statements(SQL / "views.sql")


def emulate(sql):
    sql = re.sub(r"\bARRAY_POSITION\(('(?:[^']|'')*')::VARIANT, ", r"EXPORT_TEST_ARRAY_POSITION(\1, ", sql)
    sql = re.sub(r"\bTO_VARCHAR\(", "EXPORT_TEST_TO_VARCHAR(", sql)
    sql = re.sub(r"\bTRY_BASE64_DECODE_BINARY\(", "EXPORT_TEST_BASE64(", sql)
    return sql


def dynamic_table_as_view(sql):
    return re.sub(
        r"CREATE OR REPLACE DYNAMIC TABLE (\w+)\s+TARGET_LAG = '[^']*'\s+WAREHOUSE = \w+\s+AS",
        r"CREATE OR REPLACE VIEW \1 AS",
        sql,
        count=1,
    )


def copy_as_insert(sql):
    m = re.match(r"COPY INTO (\w+) \((.*?)\)\s*FROM \((.*)\)\s*PATTERN = '[^']*'$", sql, re.S)
    assert m, sql
    table, columns, select = m.groups()
    select = (
        select.replace("@EXPORT_STAGE", "EXPORT_TEST_STAGE")
        .replace("$1:", "c1:")
        .replace("METADATA$FILENAME", "'test.parquet'")
    )
    return f"INSERT INTO {table} ({columns}) {select}"


@pytest.fixture(scope="session")
def scenarios():
    out = subprocess.run(
        ["cargo", "run", "-q", "-p", "celld-export-snowflake", "--example", "scenarios", RANDOM_COUNT],
        check=True,
        capture_output=True,
        cwd=CRATE,
    ).stdout
    return {s["name"]: s for s in json.loads(out)["scenarios"]}


class Warehouse:
    def __init__(self, cur):
        self.cur = cur

    def run(self, sql, params=None):
        self.cur.execute(emulate(sql), params)
        return self.cur.fetchall()

    def rows(self, sql):
        self.cur.execute(emulate(sql))
        names = [d[0].lower() for d in self.cur.description]
        return [dict(zip(names, r)) for r in self.cur.fetchall()]


@pytest.fixture
def warehouse():
    with fakesnow.patch():
        conn = snowflake.connector.connect(database="export", schema="cells")
        duck = conn._duck_conn
        duck.execute(
            "CREATE OR REPLACE TEMP MACRO export_test_array_position(v, arr) AS "
            "list_position(from_json(arr::JSON, '[\"VARCHAR\"]'), v) - 1"
        )
        duck.execute(
            "CREATE OR REPLACE TEMP MACRO export_test_to_varchar(v) AS "
            "CASE WHEN json_type(v::JSON) = 'VARCHAR' THEN json_extract_string(v::JSON, '$') "
            "ELSE v::VARCHAR END"
        )
        duck.execute(
            "CREATE OR REPLACE TEMP MACRO export_test_base64(s) AS from_base64(s)"
        )
        w = Warehouse(conn.cursor())
        for sql in TABLES.values():
            w.run(sql)
        yield w
        conn.close()


def tombstone(w, t, cleared=False):
    w.run(
        "INSERT INTO EXPORT_TOMBSTONES (script, class, cell, facet, incarnation, erased_at, cleared_at) "
        "VALUES (%s, %s, %s, %s, %s, CURRENT_TIMESTAMP(), "
        + ("CURRENT_TIMESTAMP()" if cleared else "NULL")
        + ")",
        (t["script"], t["class"], t["cell"], t["facet"], t["incarnation"]),
    )


def load(w, scenario):
    for t in scenario["tombstones"]:
        tombstone(w, t)
    w.run("CREATE TABLE EXPORT_TEST_STAGE (c1 VARIANT)")
    for row in scenario["stage_rows"]:
        w.run("INSERT INTO EXPORT_TEST_STAGE SELECT PARSE_JSON(%s)", (json.dumps(row),))
    w.run(copy_as_insert(LOAD["copy_into_landing"]))
    w.run("CREATE VIEW EXPORT_LANDING_NEW AS SELECT * FROM EXPORT_LANDING")
    w.run(LOAD["route_changes"])
    w.run(LOAD["route_meta"])
    for sql in VIEWS.values():
        w.run(sql)
    for dt in scenario["dynamic_tables"]:
        w.run(dynamic_table_as_view(dt["sql"]))


def stream_key(r):
    return (r["script"], r["class"], r["cell"], r["facet"], int(r["incarnation"]))


def canonical(v):
    if isinstance(v, str):
        v = json.loads(v)
    return json.dumps(v, sort_keys=True)


def check(w, scenario):
    expected = scenario["expected"]["streams"]
    by_stream = {stream_key(s["stream"]): s for s in expected}

    # Streams that still exist, and where a `deleted` record cut them.
    live = {
        stream_key(r): r["deleted_at"]
        for r in w.rows("SELECT * FROM CELL_STREAMS WHERE NOT removed")
    }
    assert live == {k: s["deleted_at"] for k, s in by_stream.items()}

    # Every Dynamic Table row is the consumer's row, and nothing is missing.
    want = {}
    for k, s in by_stream.items():
        for t in s["tables"]:
            for r in t["rows"]:
                want[(k, t["table"], t["generation"], canonical(r["key"]))] = canonical(r["row"])
    got = {}
    for dt in scenario["dynamic_tables"]:
        for r in w.rows(f"SELECT * FROM {dt['name']}"):
            k = (
                (r["_cf_script"], r["_cf_class"], r["_cf_cell"], r["_cf_facet"], int(r["_cf_incarnation"])),
                dt["table"],
                int(r["_cf_generation"]),
                canonical(r["_cf_key"]),
            )
            assert k not in got, f"two rows for one key: {k}"
            got[k] = canonical(r["_cf_row"])
    assert got == want

    # Certified positions, per stream and epoch.
    certified = {}
    for r in w.rows("SELECT * FROM CELL_CERTIFIED"):
        certified.setdefault(stream_key(r), []).append(r["position_key"])
    assert {k: sorted(v) for k, v in certified.items()} == {
        k: sorted(s["certified"]) for k, s in by_stream.items() if s["certified"]
    }

    # Gaps, and the table generations a bulk record left unknown.
    gaps, bulk = [], set()
    for r in w.rows("SELECT * FROM EXPORT_GAPS"):
        if r["gap_kind"] == "bulk":
            bulk.add((stream_key(r), r["table_name"], int(r["generation"])))
        else:
            gaps.append((stream_key(r), r["gap_kind"], int(r["bound_epoch"]), int(r["bound_txid"])))
    assert sorted(gaps) == sorted(
        (k, g[0], g[1], g[2]) for k, s in by_stream.items() for g in s["gaps"]
    )
    assert bulk == {(k, t, g) for k, s in by_stream.items() for t, g in s["uncertain"]}


def scenario_names():
    names = ["basic", "fragments", "snapshots", "generations", "deletions", "certification", "tombstones"]
    return names + [f"random_{n}" for n in range(1, int(RANDOM_COUNT) + 1)]


@pytest.mark.parametrize("name", scenario_names())
def test_sql_matches_reference_consumer(warehouse, scenarios, name):
    load(warehouse, scenarios[name])
    check(warehouse, scenarios[name])


def test_loading_twice_changes_nothing(warehouse, scenarios):
    s = scenarios["random_1"]
    load(warehouse, s)
    # The same files again, as a replayed segment or a retried COPY would.
    warehouse.run(copy_as_insert(LOAD["copy_into_landing"]))
    warehouse.run("DELETE FROM CELL_CHANGES")
    warehouse.run("DELETE FROM CELL_META")
    warehouse.run(LOAD["route_changes"])
    warehouse.run(LOAD["route_meta"])
    assert warehouse.run("SELECT COUNT(*) FROM EXPORT_LANDING")[0][0] == 2 * len(s["stage_rows"])
    check(warehouse, s)


def test_typed_projection(warehouse, scenarios):
    s = scenarios["basic"]
    load(warehouse, s)
    dt = next(d for d in s["dynamic_tables"] if d["table"] == "items")
    assert dt["columns"] == [
        ["id", "Integer"], ["name", "Text"], ["price", "Real"], ["data", "Blob"], ["extra", "Variant"],
    ]
    rows = {int(r["id"]): r for r in warehouse.rows(f'SELECT "id", "name", "price", "data", "extra" FROM {dt["name"]}')}
    assert sorted(rows) == [1, 2]
    assert rows[1]["name"] == "apple"
    assert rows[1]["price"] == -0.25
    assert bytes(rows[1]["data"]) == b""
    assert json.loads(rows[1]["extra"]) == 2.0
    assert rows[2]["name"] == "it's \"quoted\" \\ ünï"
    assert rows[2]["price"] == float("inf")
    assert rows[2]["data"] is None
    assert json.loads(rows[2]["extra"]) == 7


def test_erasure(warehouse, scenarios):
    s = scenarios["tombstones"]
    load(warehouse, s)
    # Tombstoned before loading: never routed.
    cells = {r["cell"] for r in warehouse.rows("SELECT DISTINCT cell FROM CELL_CHANGES")}
    assert cells == {"r2", "r3"}
    # A cleared tombstone does nothing; a new one hides the stream at once
    # and the erase task deletes its rows.
    r3 = {"script": "app", "class": "Room", "cell": "r3", "facet": "", "incarnation": None}
    tombstone(warehouse, r3, cleared=True)
    assert warehouse.rows("SELECT * FROM CELL_STREAMS WHERE cell = 'r3' AND NOT removed")
    tombstone(warehouse, r3)
    assert not warehouse.rows("SELECT * FROM CELL_STREAMS WHERE cell = 'r3' AND NOT removed")
    assert not warehouse.rows("SELECT * FROM CELL_CHANGES_CURRENT WHERE cell = 'r3'")
    warehouse.run(LOAD["erase_tombstoned"])
    warehouse.run(LOAD["erase_tombstoned_meta"])
    assert not warehouse.rows("SELECT * FROM CELL_CHANGES WHERE cell = 'r3'")
    assert not warehouse.rows("SELECT * FROM CELL_META WHERE cell = 'r3'")
    assert warehouse.rows("SELECT * FROM CELL_CHANGES WHERE cell = 'r2'")
