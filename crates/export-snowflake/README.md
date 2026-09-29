# celld-export-snowflake

The change export's Snowflake side: see `docs/design/change-export.md`,
"The Snowflake loader" and "Erasure". This crate is the SQL, the Rust that
renders it, and the loader that deploys it and lands records through
Snowpipe Streaming. Without the `sql-api` feature it connects to nothing;
`blob-stream` adds the consumer of the change-export topic.

| file | what |
| --- | --- |
| `sql/tables.sql` | `EXPORT_LANDING`, `CELL_CHANGES`, `CELL_META`, `EXPORT_TOMBSTONES`, `EXPORT_RECONCILER_FINDINGS`, `EXPORT_DYNAMIC_TABLES` |
| `sql/load.sql` | `EXPORT_LANDING_PIPE`, the Snowpipe Streaming pipe into `EXPORT_LANDING`, and the tasks that route records and erase tombstoned streams |
| `sql/views.sql` | `CELL_STREAMS`, `CELL_CHANGES_CURRENT`, `CELL_META_CURRENT`, `CELL_SNAPSHOTS`, `CELL_GENERATIONS`, `CELL_CERTIFIED`, `EXPORT_GAPS` |
| `sql/dynamic_table.sql` | the Dynamic Table per `(script, class, table)` |
| `src/landing.rs` | `LandingRow`: a record as one `EXPORT_LANDING` row |
| `src/consume.rs` | `Batch` and `Land`: records batched to land, and the offsets a landed batch covers |
| `src/loader.rs` | `Loader`: deploy, Dynamic Table sync, routing, erasure, the read side |
| `src/sql_api.rs` | (`sql-api`) a `Warehouse` on Snowflake's SQL API with key-pair auth |
| `src/streaming.rs` | (`sql-api`) `Streaming`: appends to the landing pipe's elastic channel over Snowpipe Streaming's REST API |
| `src/blob_stream.rs` | (`blob-stream`) the consumer loop: read the topic, land batches, commit offsets, sync the Dynamic Tables |
| `src/bin/loader.rs` | (`sql-api`) the `celld-export-loader` binary |

## How records flow

1. celld's blob-stream sink writes each record's JSON as one message to the
   change-export topic.
2. `celld-export-loader run`, a member of the consumer group `snowflake`,
   batches the messages and appends each batch as JSON lines to
   `EXPORT_LANDING_PIPE` through Snowpipe Streaming's elastic channel. The
   pipe's `COPY` casts each line into a `LandingRow`: one column per
   envelope field, `body` (the kind-specific fields as a JSON string) and
   `source` (`blob-stream/<partition>/<offset>`). Snowpipe Streaming bills
   per GB ingested; no warehouse runs for it. Once every row of a batch is
   acknowledged, which means it is durable, the loader commits the batch's
   offsets. A crash replays at most the uncommitted batches, and every reader
   drops the duplicates. The channel does not order rows, and nothing needs
   it to: completeness comes from positions and watermarks.
3. The route task reads new landed rows through a stream and, in one
   transaction, puts `rows` and `snapshot` records into `CELL_CHANGES` and
   the rest into `CELL_META`, dropping tombstoned streams. Setup resumes the
   route and erase tasks, which Snowflake creates suspended.
4. The views derive current state from the two tables, dropping duplicates
   and incomplete fragments, the way the reference consumer in
   `crates/export-format` does. `EXPORT_GAPS` is what the repair driver polls.
5. Each Dynamic Table keeps the newest row per key for one table across a
   class, typed from the union of the table's `schema` records.

Snapshots, repair and backfill go through the same sink, so they reach the
topic like live records. `ingest` lands a file of records (such as
`celld export inspect` prints from a bucket sink) the same way, for records
that never went through the topic.

## Decisions this crate makes

- A root cell's `facet` is `''` in the tables, so stream columns join by
  equality.
- Positions are also stored as `position_key`, three zero-padded 20-digit
  parts joined by `.`, so positions compare as strings.
- A tombstone with a NULL incarnation erases every incarnation of the scope.
- A typed column that cannot hold a value is NULL there; `_CF_ROW` and
  `_CF_COLUMNS` carry every row exactly as exported. NUMERIC affinity, no
  declared type, and a type that changes across generations are VARIANT.
- `EXPORT_GAPS` also lists table generations a `bulk` record left unknown,
  since those need a repair snapshot too, including a generation whose
  only record so far is the `bulk`.

## The loader

`celld-export-loader` runs one command per invocation:

| command | what |
| --- | --- |
| `deploy` | create every object that is missing, resume the two tasks (Snowflake creates a task suspended), and sync the Dynamic Tables |
| `sync` | render each table's Dynamic Table from the union of its `schema` records in `CELL_META`, and create or replace only those whose statement changed (`EXPORT_DYNAMIC_TABLES` holds what was deployed; replacing one restarts it with a full refresh) |
| `run [SECONDS]` | (`blob-stream`) `deploy`, then consume the topic until SIGINT or SIGTERM, landing batches through Snowpipe Streaming and running `sync` every SECONDS (default 60) |
| `ingest FILE` | land the records in FILE (JSON lines as `celld export inspect` prints them; `-` for stdin) through Snowpipe Streaming, then route them by running the route task's body, which returns once they are routed (`EXECUTE TASK` only schedules a run). A line that is not a record is reported and fails the command after the rest land |
| `erase SCRIPT CLASS CELL [--facet P] [--incarnation N] [--reason R]` | add a tombstone, unless an open one matches, and delete the stream's rows by running the erase task's body |
| `query SQL [BIND...]` | run any statement with each `?` bound to a JSON value, as the reconciler's statements (#49) are, and print the rows |
| `gaps`, `certified` | print `EXPORT_GAPS` or `CELL_CERTIFIED`: the read side the repair driver, `verify` and the reconciler need |

Settings are environment variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PRIVATE_KEY_FILE` (PKCS#8 or PKCS#1 PEM; encrypted PKCS#8 with
`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`), `SNOWFLAKE_DATABASE`,
`SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE` (the tasks' and Dynamic Tables'
warehouse), optionally `SNOWFLAKE_ROLE` and `SNOWFLAKE_URL`.
`EXPORT_TARGET_LAG` (default `1 minute`) and `EXPORT_DYNAMIC_TABLE_PREFIX`
(default `CF`) shape the Dynamic Tables, which are named
`CF_<SCRIPT>_<CLASS>_<TABLE>_<hash>`. A batch lands at
`EXPORT_BATCH_RECORDS` records (default 10000), `EXPORT_BATCH_BYTES`
(default 8 MiB; appends are split at Snowpipe Streaming's 4 MB limit), or,
for `run`, `EXPORT_BATCH_MS` after its first record (default 5000).

`run` also needs `EXPORT_BLOB_STREAM_CONFIG`, a blob-stream
`ConsumerIteratorBootstrapConfig` in YAML or JSON
(`examples/consumer.yaml`), and a member id that stays the same across
restarts, from `EXPORT_MEMBER_ID` or else `HOSTNAME`. The config's group
defaults to `EXPORT_GROUP`, else `snowflake`. A batch that fails to land is
retried with backoff (1s doubling to 60s) and its offsets stay uncommitted,
so a Snowflake outage stalls the consumer rather than losing records. A
message that is not a record is logged and skipped; its stream is not
certified past it, and `EXPORT_GAPS` lists it for repair.

`deploy` creates objects `IF NOT EXISTS`, so it never changes one that
exists. After an upgrade changes a table, the pipe, the stream or a task,
drop that object by hand and deploy again.

## Tests

`cargo test -p celld-export-snowflake --all-features` checks the rendering,
the loader against a recording fake, and the key-pair JWT. `sqltest/run.sh`
runs the rest against fakesnow (a Snowflake emulator on DuckDB); it needs
Python 3 and the packages in `sqltest/requirements.txt`, and CI runs it in
`.github/workflows/export-snowflake.yml`.

- `test_sql.py` runs every statement fakesnow supports against synthetic
  records from `examples/scenarios.rs`, and asserts that every view and
  Dynamic Table matches what the reference consumer derives from the same
  records. Its docstring lists what it has to emulate.
- `test_loader.py` runs the `celld-export-loader` binary against
  `sqlapi.py`, an emulation of Snowflake's SQL API and Snowpipe Streaming's
  REST API on fakesnow: `deploy` from nothing, `ingest` from a file, from
  stdin and again as a replay (in appends of at most seven rows, one of them
  refused once and retried under its request id), `sync`, the same
  comparison with the reference consumer, then `sync` and `deploy` again to
  show they change nothing. It also covers the route task, `erase`, the read
  side, a rejected statement, a rejected append, and a key Snowflake would
  refuse. `sqlapi.py`'s docstring lists what it emulates.

The consumer loop (`src/blob_stream/tests.rs`) runs against a fake
iterator: batches land on linger or when full, a failed batch is retried
and its offsets are not committed until it lands, stopping while the
warehouse is down commits nothing, and revoked partitions are let go only
after their batch lands.

What the emulator cannot tell us, and a real account has to: that the
pipe, stream and tasks deploy as written, that Snowpipe Streaming accepts
the appends and the pipe casts them as expected, and whether Snowflake
refreshes the Dynamic Tables incrementally or falls back to full
refreshes.

## Verifying on a real account

With `real_account.py`, in the sqltest virtualenv
(`target/export-snowflake-sqltest-venv`):

1. As an administrator: a database, a warehouse, and a role with `USAGE`
   on both, `CREATE SCHEMA` on the database, and `EXECUTE TASK` on the
   account. Owning the schema lets the role create the pipe and append to
   it through Snowpipe Streaming.
2. A user for the loader with that role and a key pair:
   `openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out rsa_key.p8`,
   `openssl pkey -in rsa_key.p8 -pubout`, and
   `ALTER USER celld_loader SET RSA_PUBLIC_KEY = '<the key without its PEM lines>'`.
3. A fresh schema per scenario, say `VERIFY_BASIC`, and the settings above
   pointing at it.
4. `cargo run -p celld-export-snowflake --features sql-api --bin celld-export-loader -- deploy`.
5. `python crates/export-snowflake/sqltest/real_account.py files OUT`, then
   `celld-export-loader ingest OUT/basic.jsonl`. It prints how many records
   it landed and routed; `SELECT COUNT(*) FROM CELL_CHANGES` should match
   the scenario's rows.
6. `celld-export-loader sync`, then `real_account.py check basic`. It says
   whether every view and Dynamic Table matches the reference consumer,
   and prints each Dynamic Table's refresh mode: `INCREMENTAL` is what the
   design expects; `FULL` comes with Snowflake's reason.
7. `celld-export-loader ingest OUT/edge.jsonl`, then
   `SELECT incarnation, fragment FROM CELL_META WHERE script = 'edge'`.
   It must print `18446744073709551615` and `4294967295`: a negative or
   rounded number means the pipe's casts land large incarnations and
   positions wrong.
8. Repeat 3 to 6 with `generations`, `deletions` and `random_1` for more
   coverage, and try `erase` on the loaded data. To try `run`, point
   `EXPORT_BLOB_STREAM_CONFIG` at a topic celld exports to, build with
   `--features sql-api,blob-stream`, and watch `EXPORT_LANDING` fill.
