-- Change export: the Dynamic Table for one (script, class, table).
--
-- `DynamicTable::render` fills this in. {{SCRIPT}}, {{CLASS}} and {{TABLE}}
-- are string literals; {{COLUMNS}} is the typed projection generated from
-- the union of the table's `schema` records across the class: one column
-- per name any generation had, typed by SQLite's affinity rules for its
-- declared type, VARIANT where generations disagree. A value that does not
-- fit its column's type is NULL there; _CF_COLUMNS and _CF_ROW always carry
-- the row exactly as exported.
--
-- Per stream and table generation it takes the winning snapshot's rows and
-- the `rows` records above that snapshot, keeps the newest change per key
-- (by position, then repair before live, then fragment and row order), and
-- drops deletes, closed generations, and removed streams.

-- statement: dynamic_table
CREATE OR REPLACE DYNAMIC TABLE {{NAME}}
    TARGET_LAG = '{{TARGET_LAG}}'
    WAREHOUSE = {{WAREHOUSE}}
AS
WITH generations AS (
    SELECT *
    FROM CELL_GENERATIONS
    WHERE script = {{SCRIPT}} AND class = {{CLASS}} AND table_name = {{TABLE}}
      AND NOT closed
),
changes AS (
    SELECT
        c.*,
        CASE c.origin WHEN 'repair' THEN 2 WHEN 'snapshot' THEN 1 ELSE 0 END AS origin_rank
    FROM CELL_CHANGES_CURRENT c
    JOIN generations g
      ON g.script = c.script AND g.class = c.class AND g.cell = c.cell
     AND g.facet = c.facet AND g.incarnation = c.incarnation
     AND g.table_name = c.table_name AND g.generation = c.generation
    WHERE c.script = {{SCRIPT}} AND c.class = {{CLASS}} AND c.table_name = {{TABLE}}
      AND (
          (c.kind = 'rows'
              AND (g.cut_rank IS NULL OR c.position_key > g.cut_position_key))
          OR (c.kind = 'snapshot'
              AND c.position_key || ':'
                  || CASE c.origin WHEN 'repair' THEN '2' WHEN 'snapshot' THEN '1' ELSE '0' END
                  || ':' || c.snapshot_id = g.cut_rank)
      )
),
latest AS (
    SELECT
        c.script, c.class, c.cell, c.facet, c.incarnation, c.cell_name,
        c.generation, c.epoch, c.txid, c.commit, c.committed_at, c.origin,
        c.columns,
        ch.value[0]::STRING AS op,
        ch.value[1] AS row_key,
        ch.value[2] AS image
    FROM changes c, LATERAL FLATTEN(input => c.row_changes) ch
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.script, c.class, c.cell, c.facet, c.incarnation,
            c.generation, TO_JSON(ch.value[1])
        ORDER BY c.position_key DESC, c.origin_rank DESC, c.fragment DESC, ch.index DESC
    ) = 1
)
SELECT
    script AS _CF_SCRIPT,
    class AS _CF_CLASS,
    cell AS _CF_CELL,
    facet AS _CF_FACET,
    incarnation AS _CF_INCARNATION,
    cell_name AS _CF_CELL_NAME,
    generation AS _CF_GENERATION,
    epoch AS _CF_EPOCH,
    txid AS _CF_TXID,
    commit AS _CF_COMMIT,
    committed_at AS _CF_COMMITTED_AT,
    origin AS _CF_ORIGIN,
    row_key AS _CF_KEY,
    columns AS _CF_COLUMNS,
    image AS _CF_ROW{{COLUMNS}}
FROM latest
WHERE op <> 'D';
