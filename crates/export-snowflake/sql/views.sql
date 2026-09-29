-- Change export: what the consumer derives from CELL_CHANGES and CELL_META.
--
-- These views are the design's precedence rules in SQL, and the test suite
-- checks them against the reference consumer in crates/export-format. Each
-- one names the part of `consumer.rs` it follows. They read the tables
-- directly, so they are always current; the loader may materialize the
-- expensive ones.
--
-- "Stream" below means the five columns SCRIPT, CLASS, CELL, FACET and
-- INCARNATION. A CUT_RANK is POSITION_KEY, ':', the origin's rank (live 0,
-- snapshot 1, repair 2) and ':', then the snapshot id: comparing two as
-- strings compares (position, origin, snapshot id), the order in which one
-- snapshot beats another.

-- statement: cell_streams
-- Every stream seen, with the position a `deleted` record naming the stream
-- removed it at, and whether it is gone entirely: named by a facet `deleted`
-- record on its root's stream, or erased. `facet_deletions` in consumer.rs.
CREATE OR REPLACE VIEW CELL_STREAMS AS
WITH streams AS (
    SELECT DISTINCT script, class, cell, facet, incarnation FROM CELL_META
    UNION
    SELECT DISTINCT script, class, cell, facet, incarnation FROM CELL_CHANGES
),
deletions AS (
    SELECT
        script, class, cell, facet, incarnation, position_key,
        body:target_facet::STRING AS target_facet,
        body:target_incarnation::NUMBER(20, 0) AS target_incarnation,
        body:through_incarnation::NUMBER(20, 0) AS through_incarnation,
        COALESCE(body:subtree::BOOLEAN, FALSE) AS subtree
    FROM CELL_META
    WHERE kind = 'deleted'
),
stream_deleted AS (
    SELECT script, class, cell, facet, incarnation, MAX(position_key) AS deleted_at
    FROM deletions
    WHERE target_facet IS NULL
    GROUP BY script, class, cell, facet, incarnation
),
facet_deleted AS (
    SELECT DISTINCT s.script, s.class, s.cell, s.facet, s.incarnation
    FROM streams s
    JOIN deletions d
      ON d.script = s.script AND d.class = s.class AND d.cell = s.cell
    WHERE d.target_facet IS NOT NULL
      AND CASE
          -- A node's delete bounds the ordered incarnations it removed, at
          -- the path and, with SUBTREE, below it: a facet recreated after
          -- the delete has a larger incarnation and stays.
          WHEN d.through_incarnation IS NOT NULL THEN
              (s.facet = d.target_facet
                  OR (d.subtree AND STARTSWITH(s.facet, d.target_facet || '/')))
              AND s.incarnation <= d.through_incarnation
          ELSE
              (s.facet = d.target_facet
                  AND (d.target_incarnation IS NULL OR d.target_incarnation = s.incarnation))
              OR (d.subtree AND STARTSWITH(s.facet, d.target_facet || '/'))
      END
),
erased AS (
    SELECT DISTINCT s.script, s.class, s.cell, s.facet, s.incarnation
    FROM streams s
    JOIN EXPORT_TOMBSTONES t
      ON t.script = s.script AND t.class = s.class AND t.cell = s.cell
     AND t.facet = s.facet
     AND (t.incarnation IS NULL OR t.incarnation = s.incarnation)
    WHERE t.cleared_at IS NULL
)
SELECT
    s.script, s.class, s.cell, s.facet, s.incarnation,
    sd.deleted_at,
    f.cell IS NOT NULL AS facet_deleted,
    e.cell IS NOT NULL AS erased,
    (f.cell IS NOT NULL OR e.cell IS NOT NULL) AS removed
FROM streams s
LEFT JOIN stream_deleted sd
  ON sd.script = s.script AND sd.class = s.class AND sd.cell = s.cell
 AND sd.facet = s.facet AND sd.incarnation = s.incarnation
LEFT JOIN facet_deleted f
  ON f.script = s.script AND f.class = s.class AND f.cell = s.cell
 AND f.facet = s.facet AND f.incarnation = s.incarnation
LEFT JOIN erased e
  ON e.script = s.script AND e.class = s.class AND e.cell = s.cell
 AND e.facet = s.facet AND e.incarnation = s.incarnation;

-- statement: cell_changes_current
-- Whole `rows` and `snapshot` records, one row per fragment, each fragment
-- once, of streams that still exist and above any `deleted` position. A
-- record missing a fragment is left out until the fragment arrives.
CREATE OR REPLACE VIEW CELL_CHANGES_CURRENT AS
WITH fragments AS (
    SELECT c.*
    FROM CELL_CHANGES c
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY c.script, c.class, c.cell, c.facet, c.incarnation,
            c.position_key, c.kind, c.origin, c.table_name, c.generation,
            c.snapshot_id, c.fragment
        ORDER BY c.loaded_at, c.file_name
    ) = 1
),
whole AS (
    SELECT f.*
    FROM fragments f
    QUALIFY COUNT(*) OVER (
        PARTITION BY f.script, f.class, f.cell, f.facet, f.incarnation,
            f.position_key, f.kind, f.origin, f.table_name, f.generation,
            f.snapshot_id
    ) = f.fragments
)
SELECT w.*
FROM whole w
JOIN CELL_STREAMS s
  ON s.script = w.script AND s.class = w.class AND s.cell = w.cell
 AND s.facet = w.facet AND s.incarnation = w.incarnation
WHERE NOT s.removed
  AND (s.deleted_at IS NULL OR w.position_key > s.deleted_at);

-- statement: cell_meta_current
-- CELL_META with each record once (by `RecordKey` in dedup.rs), of streams
-- that still exist and above any `deleted` position.
CREATE OR REPLACE VIEW CELL_META_CURRENT AS
WITH records AS (
    SELECT m.*
    FROM CELL_META m
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY m.script, m.class, m.cell, m.facet, m.incarnation,
            m.position_key, m.kind, m.origin,
            m.body:table::STRING, m.body:generation::STRING,
            m.body:snapshot_id::STRING,
            m.body:target_facet::STRING,
            COALESCE(m.body:target_incarnation::STRING, m.body:through_incarnation::STRING),
            TO_JSON(m.body:tables)
        ORDER BY m.loaded_at, m.file_name
    ) = 1
)
SELECT r.*
FROM records r
JOIN CELL_STREAMS s
  ON s.script = r.script AND s.class = r.class AND s.cell = r.cell
 AND s.facet = r.facet AND s.incarnation = r.incarnation
WHERE NOT s.removed
  AND (s.deleted_at IS NULL OR r.position_key > s.deleted_at);

-- statement: cell_snapshots
-- For each stream and scope, the winning complete snapshot: one whose
-- `snapshot_end` has all its `snapshot` records present. SCOPE is 'stream'
-- (TABLE_NAME and GENERATION NULL) or 'tables', one row per table
-- generation a table-scoped snapshot covered. The cuts in consumer.rs.
CREATE OR REPLACE VIEW CELL_SNAPSHOTS AS
WITH ends AS (
    SELECT
        m.script, m.class, m.cell, m.facet, m.incarnation,
        m.epoch, m.txid, m.commit, m.position_key, m.origin,
        m.body:snapshot_id::STRING AS snapshot_id,
        m.body:scope::STRING AS scope,
        m.body:records::NUMBER(20, 0) AS records,
        m.body:tables AS tables
    FROM CELL_META_CURRENT m
    WHERE m.kind = 'snapshot_end'
),
counts AS (
    SELECT
        script, class, cell, facet, incarnation, position_key, snapshot_id,
        COUNT(*) AS records
    FROM CELL_CHANGES_CURRENT
    WHERE kind = 'snapshot' AND fragment = 1
    GROUP BY script, class, cell, facet, incarnation, position_key, snapshot_id
),
complete AS (
    SELECT
        e.*,
        e.position_key || ':'
            || CASE e.origin WHEN 'repair' THEN '2' WHEN 'snapshot' THEN '1' ELSE '0' END
            || ':' || e.snapshot_id AS cut_rank
    FROM ends e
    LEFT JOIN counts c
      ON c.script = e.script AND c.class = e.class AND c.cell = e.cell
     AND c.facet = e.facet AND c.incarnation = e.incarnation
     AND c.position_key = e.position_key AND c.snapshot_id = e.snapshot_id
    WHERE COALESCE(c.records, 0) = e.records
),
scoped AS (
    SELECT
        script, class, cell, facet, incarnation, scope,
        CAST(NULL AS STRING) AS table_name,
        CAST(NULL AS NUMBER(20, 0)) AS generation,
        epoch, txid, commit, position_key, origin, snapshot_id, cut_rank
    FROM complete
    WHERE scope = 'stream'
    UNION ALL
    SELECT
        c.script, c.class, c.cell, c.facet, c.incarnation, c.scope,
        t.value:table::STRING,
        t.value:generation::NUMBER(20, 0),
        c.epoch, c.txid, c.commit, c.position_key, c.origin, c.snapshot_id, c.cut_rank
    FROM complete c, LATERAL FLATTEN(input => c.tables) t
    WHERE c.scope = 'tables'
)
SELECT *
FROM scoped
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY script, class, cell, facet, incarnation, scope, table_name, generation
    ORDER BY cut_rank DESC
) = 1;

-- statement: cell_generations
-- Every table generation of every stream: where it opened, whether it is
-- closed (dropped, renamed away, or followed by a higher generation of the
-- same name), the snapshot cut its rows start from, and whether a `bulk`
-- after that cut left it unknown until the next snapshot.
CREATE OR REPLACE VIEW CELL_GENERATIONS AS
WITH schemas AS (
    SELECT
        script, class, cell, facet, incarnation, position_key,
        body:table::STRING AS table_name,
        body:generation::NUMBER(20, 0) AS generation,
        COALESCE(body:dropped::BOOLEAN, FALSE) AS dropped,
        body:renamed_from::STRING AS renamed_from
    FROM CELL_META_CURRENT
    WHERE kind = 'schema'
),
seen AS (
    SELECT script, class, cell, facet, incarnation, table_name, generation, position_key
    FROM CELL_CHANGES_CURRENT
    UNION ALL
    SELECT script, class, cell, facet, incarnation, table_name, generation, position_key
    FROM schemas
    UNION ALL
    -- A generation whose first record is `bulk` exists too: it needs a
    -- snapshot even when no schema or row record for it has arrived.
    SELECT
        m.script, m.class, m.cell, m.facet, m.incarnation,
        t.value:table::STRING, t.value:generation::NUMBER(20, 0), m.position_key
    FROM CELL_META_CURRENT m, LATERAL FLATTEN(input => m.body:tables) t
    WHERE m.kind = 'bulk'
),
opened AS (
    SELECT
        script, class, cell, facet, incarnation, table_name, generation,
        MIN(position_key) AS opened_at
    FROM seen
    GROUP BY script, class, cell, facet, incarnation, table_name, generation
),
newest AS (
    SELECT script, class, cell, facet, incarnation, table_name, MAX(generation) AS generation
    FROM opened
    GROUP BY script, class, cell, facet, incarnation, table_name
),
dropped AS (
    SELECT DISTINCT script, class, cell, facet, incarnation, table_name, generation
    FROM schemas
    WHERE dropped
),
renamed_away AS (
    SELECT DISTINCT o.script, o.class, o.cell, o.facet, o.incarnation, o.table_name, o.generation
    FROM opened o
    JOIN schemas s
      ON s.script = o.script AND s.class = o.class AND s.cell = o.cell
     AND s.facet = o.facet AND s.incarnation = o.incarnation
     AND s.renamed_from = o.table_name AND s.position_key >= o.opened_at
),
bulk AS (
    SELECT
        m.script, m.class, m.cell, m.facet, m.incarnation,
        t.value:table::STRING AS table_name,
        t.value:generation::NUMBER(20, 0) AS generation,
        MAX(m.position_key) AS last_bulk
    FROM CELL_META_CURRENT m, LATERAL FLATTEN(input => m.body:tables) t
    WHERE m.kind = 'bulk'
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),
cuts AS (
    SELECT
        o.script, o.class, o.cell, o.facet, o.incarnation, o.table_name, o.generation,
        NULLIF(GREATEST(COALESCE(st.cut_rank, ''), COALESCE(tb.cut_rank, '')), '') AS cut_rank
    FROM opened o
    LEFT JOIN CELL_SNAPSHOTS st
      ON st.script = o.script AND st.class = o.class AND st.cell = o.cell
     AND st.facet = o.facet AND st.incarnation = o.incarnation
     AND st.scope = 'stream'
    LEFT JOIN CELL_SNAPSHOTS tb
      ON tb.script = o.script AND tb.class = o.class AND tb.cell = o.cell
     AND tb.facet = o.facet AND tb.incarnation = o.incarnation
     AND tb.scope = 'tables'
     AND tb.table_name = o.table_name AND tb.generation = o.generation
)
SELECT
    o.script, o.class, o.cell, o.facet, o.incarnation, o.table_name, o.generation,
    o.opened_at,
    (d.table_name IS NOT NULL OR n.generation > o.generation OR r.table_name IS NOT NULL)
        AS closed,
    c.cut_rank,
    LEFT(c.cut_rank, 62) AS cut_position_key,
    (b.last_bulk IS NOT NULL AND (c.cut_rank IS NULL OR b.last_bulk > LEFT(c.cut_rank, 62)))
        AS uncertain
FROM opened o
JOIN newest n
  ON n.script = o.script AND n.class = o.class AND n.cell = o.cell
 AND n.facet = o.facet AND n.incarnation = o.incarnation AND n.table_name = o.table_name
JOIN cuts c
  ON c.script = o.script AND c.class = o.class AND c.cell = o.cell
 AND c.facet = o.facet AND c.incarnation = o.incarnation
 AND c.table_name = o.table_name AND c.generation = o.generation
LEFT JOIN dropped d
  ON d.script = o.script AND d.class = o.class AND d.cell = o.cell
 AND d.facet = o.facet AND d.incarnation = o.incarnation
 AND d.table_name = o.table_name AND d.generation = o.generation
LEFT JOIN renamed_away r
  ON r.script = o.script AND r.class = o.class AND r.cell = o.cell
 AND r.facet = o.facet AND r.incarnation = o.incarnation
 AND r.table_name = o.table_name AND r.generation = o.generation
LEFT JOIN bulk b
  ON b.script = o.script AND b.class = o.class AND b.cell = o.cell
 AND b.facet = o.facet AND b.incarnation = o.incarnation
 AND b.table_name = o.table_name AND b.generation = o.generation;

-- statement: cell_certified
-- Per stream and epoch, the highest position a chain of watermarks certifies:
-- the epoch's first watermark (no `from`), then each whose `from` is the
-- previous one's `through`, each only while its counts match the records
-- held in its range. `certify` in consumer.rs.
CREATE OR REPLACE VIEW CELL_CERTIFIED AS
WITH RECURSIVE sent AS (
    -- What the node's sink sent: every whole record but repair output,
    -- watermarks, and the `recovered` records another node emits for the
    -- stream.
    SELECT script, class, cell, facet, incarnation, position_key
    FROM CELL_CHANGES_CURRENT
    WHERE origin <> 'repair' AND fragment = 1
    UNION ALL
    SELECT script, class, cell, facet, incarnation, position_key
    FROM CELL_META_CURRENT
    WHERE origin <> 'repair' AND kind NOT IN ('watermark', 'recovered')
),
marks AS (
    SELECT
        script, class, cell, facet, incarnation,
        body:through:epoch::NUMBER(20, 0) AS epoch,
        body:through:txid::NUMBER(20, 0) AS txid,
        body:through:commit::NUMBER(20, 0) AS commit,
        LPAD(body:through:epoch::STRING, 20, '0') || '.'
            || LPAD(body:through:txid::STRING, 20, '0') || '.'
            || LPAD(body:through:commit::STRING, 20, '0') AS through_key,
        IFF(body:from:epoch::STRING IS NULL, NULL,
            LPAD(body:from:epoch::STRING, 20, '0') || '.'
                || LPAD(body:from:txid::STRING, 20, '0') || '.'
                || LPAD(body:from:commit::STRING, 20, '0')) AS from_key,
        LPAD(body:through:epoch::STRING, 20, '0') || '.' || REPEAT('0', 20) || '.'
            || REPEAT('0', 20) AS floor_key,
        body:commits::NUMBER(20, 0) AS commits,
        body:records::NUMBER(20, 0) AS records
    FROM CELL_META_CURRENT
    WHERE kind = 'watermark'
),
valid AS (
    SELECT
        m.script, m.class, m.cell, m.facet, m.incarnation,
        m.epoch, m.txid, m.commit, m.through_key, m.from_key
    FROM marks m
    LEFT JOIN sent s
      ON s.script = m.script AND s.class = m.class AND s.cell = m.cell
     AND s.facet = m.facet AND s.incarnation = m.incarnation
     AND s.position_key <= m.through_key
     AND (s.position_key > m.from_key
          OR (m.from_key IS NULL AND s.position_key >= m.floor_key))
    GROUP BY
        m.script, m.class, m.cell, m.facet, m.incarnation,
        m.epoch, m.txid, m.commit, m.through_key, m.from_key, m.commits, m.records
    HAVING COUNT(s.position_key) = m.records
       AND COUNT(DISTINCT s.position_key) = m.commits
),
chain (script, class, cell, facet, incarnation, epoch, txid, commit, through_key) AS (
    SELECT script, class, cell, facet, incarnation, epoch, txid, commit, through_key
    FROM valid
    WHERE from_key IS NULL
    UNION ALL
    SELECT v.script, v.class, v.cell, v.facet, v.incarnation, v.epoch, v.txid, v.commit, v.through_key
    FROM valid v
    JOIN chain c
      ON c.script = v.script AND c.class = v.class AND c.cell = v.cell
     AND c.facet = v.facet AND c.incarnation = v.incarnation
     AND c.epoch = v.epoch AND v.from_key = c.through_key AND v.through_key > c.through_key
)
SELECT
    script, class, cell, facet, incarnation, epoch, txid, commit,
    through_key AS position_key
FROM chain
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY script, class, cell, facet, incarnation, epoch
    ORDER BY through_key DESC
) = 1;

-- statement: export_gaps
-- What the repair driver polls: holes no stream-wide snapshot has covered
-- yet. GAP_KIND is 'gap' (a gap record), 'link' or 'recovered' (a
-- predecessor or recovered head beyond the certified position of its
-- epoch), 'bulk' (a table generation a bulk record left unknown), or
-- 'reconciler' (an open reconciler finding). BOUND_EPOCH and BOUND_TXID are
-- where a repair snapshot must reach to cover it.
CREATE OR REPLACE VIEW EXPORT_GAPS AS
WITH stream_cut AS (
    SELECT script, class, cell, facet, incarnation, LEFT(position_key, 41) AS cut_key
    FROM CELL_SNAPSHOTS
    WHERE scope = 'stream'
),
bounded AS (
    SELECT
        m.script, m.class, m.cell, m.facet, m.incarnation,
        m.epoch, m.txid, m.commit, m.position_key, m.kind AS gap_kind,
        CASE m.kind
            WHEN 'gap' THEN m.body:to:epoch::NUMBER(20, 0)
            WHEN 'link' THEN m.body:prev_epoch::NUMBER(20, 0)
            ELSE m.body:head:epoch::NUMBER(20, 0)
        END AS bound_epoch,
        CASE m.kind
            WHEN 'gap' THEN m.body:to:txid::NUMBER(20, 0)
            WHEN 'link' THEN m.body:prev_txid::NUMBER(20, 0)
            ELSE m.body:head:txid::NUMBER(20, 0)
        END AS bound_txid,
        m.body:reason::STRING AS reason,
        m.body AS detail
    FROM CELL_META_CURRENT m
    WHERE m.kind IN ('gap', 'link', 'recovered')
),
found AS (
    SELECT
        b.*,
        cc.txid AS certified_txid
    FROM bounded b
    LEFT JOIN CELL_CERTIFIED cc
      ON cc.script = b.script AND cc.class = b.class AND cc.cell = b.cell
     AND cc.facet = b.facet AND cc.incarnation = b.incarnation
     AND cc.epoch = b.bound_epoch
    LEFT JOIN stream_cut sc
      ON sc.script = b.script AND sc.class = b.class AND sc.cell = b.cell
     AND sc.facet = b.facet AND sc.incarnation = b.incarnation
    WHERE b.bound_epoch IS NOT NULL AND b.bound_txid IS NOT NULL
      AND (sc.cut_key IS NULL
           OR sc.cut_key < LPAD(b.bound_epoch::STRING, 20, '0') || '.'
                           || LPAD(b.bound_txid::STRING, 20, '0'))
      AND (b.gap_kind = 'gap' OR b.bound_txid > COALESCE(cc.txid, 0))
)
SELECT
    script, class, cell, facet, incarnation, gap_kind,
    epoch, txid, commit, bound_epoch, bound_txid, certified_txid,
    CAST(NULL AS STRING) AS table_name, CAST(NULL AS NUMBER(20, 0)) AS generation,
    reason, detail
FROM found
UNION ALL
SELECT
    script, class, cell, facet, incarnation, 'bulk',
    NULL, NULL, NULL, NULL, NULL, NULL,
    table_name, generation,
    'bulk', NULL
FROM CELL_GENERATIONS
WHERE uncertain AND NOT closed
UNION ALL
SELECT
    script, class, cell, facet, incarnation, 'reconciler',
    NULL, NULL, NULL, head_epoch, head_txid, NULL,
    NULL, NULL,
    finding, detail
FROM EXPORT_RECONCILER_FINDINGS
WHERE resolved_at IS NULL;
