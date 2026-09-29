# celld-export-snowflake

The change export's Snowflake side: see `docs/design/change-export.md`,
"The Snowflake loader" and "Erasure". This crate is SQL and the Rust that
renders it; it connects to nothing. The loader deploys it and runs it.

| file | what |
| --- | --- |
| `sql/tables.sql` | `EXPORT_LANDING`, `CELL_CHANGES`, `CELL_META`, `EXPORT_TOMBSTONES`, `EXPORT_RECONCILER_FINDINGS` |
| `sql/load.sql` | the stage, `COPY INTO EXPORT_LANDING`, the pipe, and the tasks that route records and erase tombstoned streams |
| `sql/views.sql` | `CELL_STREAMS`, `CELL_CHANGES_CURRENT`, `CELL_META_CURRENT`, `CELL_SNAPSHOTS`, `CELL_GENERATIONS`, `CELL_CERTIFIED`, `EXPORT_GAPS` |
| `sql/dynamic_table.sql` | the Dynamic Table per `(script, class, table)` |

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

## Tests

`cargo test -p celld-export-snowflake` checks the rendering. The SQL itself is
checked by `sqltest/run.sh`, which runs every statement that fakesnow (a
Snowflake emulator on DuckDB) supports against synthetic records from
`examples/scenarios.rs`, and asserts that every view and Dynamic Table
matches what the reference consumer derives from the same records.
`sqltest/test_sql.py` lists what it has to emulate. It needs Python 3 and
the packages in `sqltest/requirements.txt`, and is not yet part of CI.

What the emulator cannot tell us, and a real account has to: that the
stage, pipe, stream and tasks deploy as written, and whether Snowflake
refreshes the Dynamic Tables incrementally or falls back to full refreshes.
