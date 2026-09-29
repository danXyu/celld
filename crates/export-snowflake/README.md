# celld-export-snowflake

The change export's Snowflake side: see `docs/design/change-export.md`,
"The Snowflake loader" and "Erasure". This crate is the SQL, the Rust that
renders it, and the loader that deploys and drives it. Without the
`sql-api` feature it connects to nothing.

| file | what |
| --- | --- |
| `sql/tables.sql` | `EXPORT_LANDING`, `CELL_CHANGES`, `CELL_META`, `EXPORT_TOMBSTONES`, `EXPORT_RECONCILER_FINDINGS`, `EXPORT_DYNAMIC_TABLES` |
| `sql/load.sql` | the stage, `COPY INTO EXPORT_LANDING`, the pipe, and the tasks that route records and erase tombstoned streams |
| `sql/views.sql` | `CELL_STREAMS`, `CELL_CHANGES_CURRENT`, `CELL_META_CURRENT`, `CELL_SNAPSHOTS`, `CELL_GENERATIONS`, `CELL_CERTIFIED`, `EXPORT_GAPS` |
| `sql/dynamic_table.sql` | the Dynamic Table per `(script, class, table)` |
| `src/loader.rs` | `Loader`: deploy, Dynamic Table sync, repair loads, erasure, the read side |
| `src/sql_api.rs` | (`sql-api`) a `Warehouse` on Snowflake's SQL API with key-pair auth |
| `src/bin/loader.rs` | (`sql-api`) the `celld-export-loader` binary |

## How records flow

1. The bucket sink writes Parquet files whose columns are `StageRow`'s: one
   per envelope field, and `body`, the kind-specific fields as a JSON string.
2. `COPY INTO EXPORT_LANDING` (by the pipe, or by hand for repair and
   backfill files) lands them unchanged. A COPY transformation cannot filter
   or split, hence the landing table.
3. The route task reads new landed rows through a stream and, in one
   transaction, puts `rows` and `snapshot` records into `CELL_CHANGES` and
   the rest into `CELL_META`, dropping tombstoned streams. Setup resumes the
   route and erase tasks, which Snowflake creates suspended.
4. The views derive current state from the two tables, dropping duplicates
   and incomplete fragments, the way the reference consumer in
   `crates/export-format` does. `EXPORT_GAPS` is what the repair driver polls.
5. Each Dynamic Table keeps the newest row per key for one table across a
   class, typed from the union of the table's `schema` records.

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
| `deploy` | create every object that is missing, resume the two tasks (Snowflake creates a task suspended), print the pipe's notification channel, and sync the Dynamic Tables |
| `sync` | render each table's Dynamic Table from the union of its `schema` records in `CELL_META`, and create or replace only those whose statement changed (`EXPORT_DYNAMIC_TABLES` holds what was deployed; replacing one restarts it with a full refresh) |
| `run [SECONDS]` | `deploy`, then `sync` every SECONDS |
| `load PREFIX` | `COPY` the stage files under PREFIX (snapshot, repair, backfill) into `EXPORT_LANDING` and route them by running the route task's body, which returns once they are routed (`EXECUTE TASK` only schedules a run); COPY's load history skips files already loaded |
| `erase SCRIPT CLASS CELL [--facet P] [--incarnation N] [--reason R]` | add a tombstone, unless an open one matches, and delete the stream's rows by running the erase task's body |
| `query SQL [BIND...]` | run any statement with each `?` bound to a JSON value, as the reconciler's statements (#49) are, and print the rows |
| `gaps`, `certified` | print `EXPORT_GAPS` or `CELL_CERTIFIED`: the read side the repair driver, `verify` and the reconciler need |

Settings are environment variables: `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`,
`SNOWFLAKE_PRIVATE_KEY_FILE` (PKCS#8 or PKCS#1 PEM; encrypted PKCS#8 with
`SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`), `SNOWFLAKE_DATABASE`,
`SNOWFLAKE_SCHEMA`, `SNOWFLAKE_WAREHOUSE` (also the tasks' and Dynamic
Tables' warehouse), optionally `SNOWFLAKE_ROLE` and `SNOWFLAKE_URL`; and
for `deploy` and `run`, `EXPORT_STAGE_URL` and
`EXPORT_STORAGE_INTEGRATION`. `EXPORT_TARGET_LAG` (default `1 minute`) and
`EXPORT_DYNAMIC_TABLE_PREFIX` (default `CF`) shape the Dynamic Tables, which
are named `CF_<SCRIPT>_<CLASS>_<TABLE>_<hash>`.

`deploy` creates objects `IF NOT EXISTS`, so it never changes one that
exists. After an upgrade changes a table, the pipe, the stream or a task,
drop that object by hand and deploy again. Dropping the pipe loses its
load history, so the pipe may load files again; every reader dedups.

Not here yet: consuming blob-stream (group `snowflake`) and writing its
batches to the stage wait on the blob-stream sink. Until then records reach
the tables only through the bucket sink's files, by the pipe or by `load`.

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
  `sqlapi.py`, an emulation of Snowflake's SQL API on fakesnow: `deploy`
  from nothing, half the records through the pipe and the route task, half
  through `load`, `sync`, the same comparison with the reference consumer,
  then `sync` and `deploy` again to show they change nothing. It also
  covers `erase`, the read side, a rejected statement, and a key Snowflake
  would refuse. `sqlapi.py`'s docstring lists what it emulates.

What the emulator cannot tell us, and a real account has to: that the
stage, pipe, stream and tasks deploy as written, that the pipe reads the
sink's Parquet as the COPY expects, and whether Snowflake refreshes the
Dynamic Tables incrementally or falls back to full refreshes.

## Verifying on a real account

With `real_account.py`, in the sqltest virtualenv
(`target/export-snowflake-sqltest-venv`):

1. As an administrator: a database, a warehouse, a storage integration for
   the bucket's `export/changes/` prefix (`CREATE STORAGE INTEGRATION ...
   TYPE = EXTERNAL_STAGE STORAGE_PROVIDER = 'S3' STORAGE_AWS_ROLE_ARN = ...
   STORAGE_ALLOWED_LOCATIONS = ('s3://BUCKET/export/changes/')`, then the
   IAM trust `DESC INTEGRATION` asks for), and a role with `USAGE` on all
   three, `CREATE SCHEMA` on the database, and `EXECUTE TASK` on the
   account.
2. A user for the loader with that role and a key pair:
   `openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -out rsa_key.p8`,
   `openssl pkey -in rsa_key.p8 -pubout`, and
   `ALTER USER celld_loader SET RSA_PUBLIC_KEY = '<the key without its PEM lines>'`.
3. A fresh schema per scenario, say `VERIFY_BASIC`, and the settings above
   pointing at it, with `EXPORT_STAGE_URL=s3://BUCKET/export/changes/verify/basic/`.
4. `cargo run -p celld-export-snowflake --features sql-api --bin celld-export-loader -- deploy`.
   Point the bucket's object-created notifications for that prefix at the
   channel it prints.
5. `python crates/export-snowflake/sqltest/real_account.py files OUT`, then
   upload `OUT/basic.parquet` under the stage prefix. Within a minute or
   two `SELECT COUNT(*) FROM EXPORT_LANDING` should be non-zero (the pipe),
   and a minute later `CELL_CHANGES` (the route task; `SHOW TASKS` and
   `TASK_HISTORY()` show it running).
6. `celld-export-loader sync`, then `real_account.py check basic`. It says
   whether every view and Dynamic Table matches the reference consumer,
   and prints each Dynamic Table's refresh mode: `INCREMENTAL` is what the
   design expects; `FULL` comes with Snowflake's reason.
7. Upload `OUT/edge.parquet` anywhere under the stage and run
   `SELECT $1:incarnation, $1:fragment FROM @EXPORT_STAGE/<path> (FILE_FORMAT => EXPORT_PARQUET)`.
   It must print `18446744073709551615` and `4294967295`. The sink writes
   unsigned integers, and the file format reads Parquet without logical
   types, so a negative number here means large incarnations and positions
   land wrong.
8. Repeat 3 to 6 with `generations`, `deletions` and `random_1` for more
   coverage, and try `erase` and `load` on the loaded data.
