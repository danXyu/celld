-- Change export: the Snowflake tables. See docs/design/change-export.md,
-- "The Snowflake loader" and "Erasure", and this crate's README.
--
-- Run in the export schema. Positions are three NUMBER(20,0) columns, since
-- every part is a u64, plus POSITION_KEY: the three parts zero-padded to 20
-- digits and joined with '.', so that comparing two keys as strings compares
-- the positions. A root cell's FACET is '' so that stream columns join with
-- plain equality.

-- statement: export_landing
-- Every record as the stage file carries it, before routing. COPY INTO lands
-- here because a COPY transformation cannot filter or split, and records go
-- to two tables and past the tombstones. The route task reads it through an
-- append-only stream and deletes rows older than a week.
CREATE TRANSIENT TABLE IF NOT EXISTS EXPORT_LANDING (
    kind STRING NOT NULL,
    script STRING NOT NULL,
    class STRING NOT NULL,
    cell STRING NOT NULL,
    cell_name STRING,
    facet STRING,
    incarnation NUMBER(20, 0) NOT NULL,
    epoch NUMBER(20, 0) NOT NULL,
    txid NUMBER(20, 0) NOT NULL,
    commit NUMBER(20, 0) NOT NULL,
    committed_at NUMBER(20, 0) NOT NULL,
    node STRING NOT NULL,
    origin STRING NOT NULL,
    fragment NUMBER(10, 0) NOT NULL,
    fragments NUMBER(10, 0) NOT NULL,
    body STRING NOT NULL,
    file_name STRING,
    landed_at TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
)
DATA_RETENTION_TIME_IN_DAYS = 0;

-- statement: cell_changes
-- One row per `rows` or `snapshot` record fragment. Append-only; duplicates
-- are expected, and the views drop them by the record's dedup key.
CREATE TABLE IF NOT EXISTS CELL_CHANGES (
    script STRING NOT NULL,
    class STRING NOT NULL,
    cell STRING NOT NULL,
    facet STRING NOT NULL,
    incarnation NUMBER(20, 0) NOT NULL,
    cell_name STRING,
    epoch NUMBER(20, 0) NOT NULL,
    txid NUMBER(20, 0) NOT NULL,
    commit NUMBER(20, 0) NOT NULL,
    position_key STRING NOT NULL,
    committed_at TIMESTAMP_NTZ(3) NOT NULL,
    node STRING NOT NULL,
    origin STRING NOT NULL,
    fragment NUMBER(10, 0) NOT NULL,
    fragments NUMBER(10, 0) NOT NULL,
    kind STRING NOT NULL,
    snapshot_id STRING,
    table_name STRING NOT NULL,
    generation NUMBER(20, 0) NOT NULL,
    columns ARRAY NOT NULL,
    key_columns ARRAY NOT NULL,
    -- [[op, key, row], ...] exactly as the record carries it.
    row_changes ARRAY NOT NULL,
    file_name STRING,
    loaded_at TIMESTAMP_LTZ NOT NULL
)
CLUSTER BY (TO_DATE(committed_at))
DATA_RETENTION_TIME_IN_DAYS = 1;

-- statement: cell_meta
-- Every record that is not `rows` or `snapshot`. BODY holds the record's
-- kind-specific fields as an object, as the record encodes them.
CREATE TABLE IF NOT EXISTS CELL_META (
    script STRING NOT NULL,
    class STRING NOT NULL,
    cell STRING NOT NULL,
    facet STRING NOT NULL,
    incarnation NUMBER(20, 0) NOT NULL,
    cell_name STRING,
    epoch NUMBER(20, 0) NOT NULL,
    txid NUMBER(20, 0) NOT NULL,
    commit NUMBER(20, 0) NOT NULL,
    position_key STRING NOT NULL,
    committed_at TIMESTAMP_NTZ(3) NOT NULL,
    node STRING NOT NULL,
    origin STRING NOT NULL,
    fragment NUMBER(10, 0) NOT NULL,
    fragments NUMBER(10, 0) NOT NULL,
    kind STRING NOT NULL,
    body VARIANT NOT NULL,
    file_name STRING,
    loaded_at TIMESTAMP_LTZ NOT NULL
)
DATA_RETENTION_TIME_IN_DAYS = 1;

-- statement: export_tombstones
-- Erased streams. A NULL INCARNATION erases every incarnation of the scope,
-- which is what erasing a Durable Object by name means; recreating it after
-- erasure takes an operator setting CLEARED_AT. Routing drops records of a
-- tombstoned stream, the views hide it, and the erase task deletes its rows.
CREATE TABLE IF NOT EXISTS EXPORT_TOMBSTONES (
    script STRING NOT NULL,
    class STRING NOT NULL,
    cell STRING NOT NULL,
    facet STRING NOT NULL,
    incarnation NUMBER(20, 0),
    erased_at TIMESTAMP_LTZ NOT NULL,
    reason STRING,
    cleared_at TIMESTAMP_LTZ
);

-- statement: export_reconciler_findings
-- What the reconciler found that the stream does not say: a gap, a missing
-- `deleted`, or a stream the consumer has never seen. EXPORT_GAPS lists the
-- open ones; the repair driver sets RESOLVED_AT.
CREATE TABLE IF NOT EXISTS EXPORT_RECONCILER_FINDINGS (
    script STRING NOT NULL,
    class STRING NOT NULL,
    cell STRING NOT NULL,
    facet STRING NOT NULL,
    incarnation NUMBER(20, 0) NOT NULL,
    finding STRING NOT NULL,
    head_epoch NUMBER(20, 0),
    head_txid NUMBER(20, 0),
    detail VARIANT,
    found_at TIMESTAMP_LTZ NOT NULL,
    resolved_at TIMESTAMP_LTZ
);
