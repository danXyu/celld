-- Change export: loading files the bucket sink writes into the tables.
--
-- A stage file is Parquet with one column per envelope field, named as the
-- record's JSON fields are (`kind`, `script`, `class`, `cell`, `cell_name`,
-- `facet`, `incarnation`, `epoch`, `txid`, `commit`, `committed_at`, `node`,
-- `origin`, `fragment`, `fragments`), and `body`: the record's other fields
-- as a JSON object string. `StageRow` in this crate is that layout.
--
-- `Deployment::statements` fills in {{STAGE_URL}}, {{STORAGE_INTEGRATION}}
-- and {{WAREHOUSE}}, and a {{NAME}} naming another statement in this file
-- with that statement's text, so the pipe and the tasks run exactly the
-- statements the tests run.

-- statement: export_file_format
-- Without logical types a Parquet TIMESTAMP(MILLIS) column reads as its
-- int64, which is what `committed_at` lands as.
CREATE FILE FORMAT IF NOT EXISTS EXPORT_PARQUET TYPE = PARQUET USE_LOGICAL_TYPE = FALSE;

-- statement: export_stage
CREATE STAGE IF NOT EXISTS EXPORT_STAGE
    URL = '{{STAGE_URL}}'
    STORAGE_INTEGRATION = {{STORAGE_INTEGRATION}}
    FILE_FORMAT = EXPORT_PARQUET;

-- statement: copy_into_landing
-- Snapshot, repair and backfill files load this way too. COPY's load history
-- skips a file already loaded; a file loaded twice anyway is harmless, since
-- every reader dedups.
COPY INTO EXPORT_LANDING (
    kind, script, class, cell, cell_name, facet, incarnation,
    epoch, txid, commit, committed_at, node, origin, fragment, fragments,
    body, file_name
)
FROM (
    SELECT
        $1:kind::STRING,
        $1:script::STRING,
        $1:class::STRING,
        $1:cell::STRING,
        $1:cell_name::STRING,
        $1:facet::STRING,
        $1:incarnation::NUMBER(20, 0),
        $1:epoch::NUMBER(20, 0),
        $1:txid::NUMBER(20, 0),
        $1:commit::NUMBER(20, 0),
        $1:committed_at::NUMBER(20, 0),
        $1:node::STRING,
        $1:origin::STRING,
        $1:fragment::NUMBER(10, 0),
        $1:fragments::NUMBER(10, 0),
        $1:body::STRING,
        METADATA$FILENAME
    FROM @EXPORT_STAGE
)
PATTERN = '.*[.]parquet';

-- statement: export_pipe
-- Auto-ingest from the stage's bucket notifications.
CREATE PIPE IF NOT EXISTS EXPORT_PIPE AUTO_INGEST = TRUE AS
{{COPY_INTO_LANDING}};

-- statement: export_landing_new
CREATE STREAM IF NOT EXISTS EXPORT_LANDING_NEW
    ON TABLE EXPORT_LANDING APPEND_ONLY = TRUE;

-- statement: route_changes
-- The two routing inserts run in one transaction in the route task, so both
-- read the same stream contents and the stream advances once.
INSERT INTO CELL_CHANGES (
    script, class, cell, facet, incarnation, cell_name,
    epoch, txid, commit, position_key, committed_at, node, origin,
    fragment, fragments, kind, snapshot_id, table_name, generation,
    columns, key_columns, row_changes, file_name, loaded_at
)
SELECT
    l.script, l.class, l.cell, COALESCE(l.facet, ''), l.incarnation, l.cell_name,
    l.epoch, l.txid, l.commit,
    LPAD(l.epoch::STRING, 20, '0') || '.' || LPAD(l.txid::STRING, 20, '0')
        || '.' || LPAD(l.commit::STRING, 20, '0'),
    TO_TIMESTAMP_NTZ(l.committed_at, 3), l.node, l.origin,
    l.fragment, l.fragments, l.kind,
    l.doc:snapshot_id::STRING,
    l.doc:table::STRING,
    l.doc:generation::NUMBER(20, 0),
    l.doc:columns::ARRAY,
    l.doc:key_columns::ARRAY,
    l.doc:rows::ARRAY,
    l.file_name,
    CURRENT_TIMESTAMP()
FROM (SELECT n.*, PARSE_JSON(n.body) AS doc FROM EXPORT_LANDING_NEW n) l
WHERE l.kind IN ('rows', 'snapshot')
  AND NOT EXISTS (
      SELECT 1 FROM EXPORT_TOMBSTONES t
      WHERE t.cleared_at IS NULL
        AND t.script = l.script AND t.class = l.class AND t.cell = l.cell
        AND t.facet = COALESCE(l.facet, '')
        AND (t.incarnation IS NULL OR t.incarnation = l.incarnation)
  );

-- statement: route_meta
INSERT INTO CELL_META (
    script, class, cell, facet, incarnation, cell_name,
    epoch, txid, commit, position_key, committed_at, node, origin,
    fragment, fragments, kind, body, file_name, loaded_at
)
SELECT
    l.script, l.class, l.cell, COALESCE(l.facet, ''), l.incarnation, l.cell_name,
    l.epoch, l.txid, l.commit,
    LPAD(l.epoch::STRING, 20, '0') || '.' || LPAD(l.txid::STRING, 20, '0')
        || '.' || LPAD(l.commit::STRING, 20, '0'),
    TO_TIMESTAMP_NTZ(l.committed_at, 3), l.node, l.origin,
    l.fragment, l.fragments, l.kind,
    PARSE_JSON(l.body),
    l.file_name,
    CURRENT_TIMESTAMP()
FROM EXPORT_LANDING_NEW l
WHERE l.kind NOT IN ('rows', 'snapshot')
  AND NOT EXISTS (
      SELECT 1 FROM EXPORT_TOMBSTONES t
      WHERE t.cleared_at IS NULL
        AND t.script = l.script AND t.class = l.class AND t.cell = l.cell
        AND t.facet = COALESCE(l.facet, '')
        AND (t.incarnation IS NULL OR t.incarnation = l.incarnation)
  );

-- statement: expire_landing
DELETE FROM EXPORT_LANDING WHERE landed_at < DATEADD(day, -7, CURRENT_TIMESTAMP());

-- statement: export_route_task
CREATE TASK IF NOT EXISTS EXPORT_ROUTE
    WAREHOUSE = {{WAREHOUSE}}
    SCHEDULE = '1 MINUTE'
    WHEN SYSTEM$STREAM_HAS_DATA('EXPORT_LANDING_NEW')
AS
EXECUTE IMMEDIATE $$
BEGIN
    BEGIN TRANSACTION;
    {{ROUTE_CHANGES}};
    {{ROUTE_META}};
    COMMIT;
    {{EXPIRE_LANDING}};
END;
$$;

-- statement: erase_tombstoned
-- The erase task's statements: delete a tombstoned stream's rows. Time
-- travel on these tables is one day, so the rows are gone a day later.
DELETE FROM CELL_CHANGES c USING EXPORT_TOMBSTONES t
WHERE t.cleared_at IS NULL
  AND t.script = c.script AND t.class = c.class AND t.cell = c.cell
  AND t.facet = c.facet
  AND (t.incarnation IS NULL OR t.incarnation = c.incarnation);

-- statement: erase_tombstoned_meta
DELETE FROM CELL_META c USING EXPORT_TOMBSTONES t
WHERE t.cleared_at IS NULL
  AND t.script = c.script AND t.class = c.class AND t.cell = c.cell
  AND t.facet = c.facet
  AND (t.incarnation IS NULL OR t.incarnation = c.incarnation);

-- statement: export_erase_task
CREATE TASK IF NOT EXISTS EXPORT_ERASE
    WAREHOUSE = {{WAREHOUSE}}
    SCHEDULE = 'USING CRON 0 * * * * UTC'
AS
EXECUTE IMMEDIATE $$
BEGIN
    {{ERASE_TOMBSTONED}};
    {{ERASE_TOMBSTONED_META}};
END;
$$;
