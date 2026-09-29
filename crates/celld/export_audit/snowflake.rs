//! The statements a Snowflake [`super::ConsumerView`] runs, against the
//! tables and views in `crates/export-snowflake/sql` (`EXPORT_TOMBSTONES`,
//! `EXPORT_RECONCILER_FINDINGS`, `CELL_STREAMS`, `CELL_CERTIFIED`,
//! `CELL_SNAPSHOTS`, `CELL_CHANGES_CURRENT`, `CELL_META_CURRENT`).
//!
//! The loader owns the connection; this module owns what the audit asks
//! and writes, so both sides keep one shape. Binds are positional (`?`), in
//! the order the matching `*_binds` function returns them. A root stream's
//! `FACET` is the empty string, as the loader's routing stores it.

use serde_json::{json, Value as Json};

use super::{Finding, Tombstone};

/// Every stream the consumer still holds, with its `deleted` position.
pub const SELECT_STREAMS: &str = "\
SELECT script, class, cell, facet, incarnation, deleted_at
FROM CELL_STREAMS
WHERE NOT removed";

/// Per stream and epoch, the certified position.
pub const SELECT_CERTIFIED: &str = "\
SELECT script, class, cell, facet, incarnation, epoch, txid, commit
FROM CELL_CERTIFIED";

/// Per stream, the winning stream-wide snapshot.
pub const SELECT_STREAM_SNAPSHOTS: &str = "\
SELECT script, class, cell, facet, incarnation, epoch, txid, commit
FROM CELL_SNAPSHOTS
WHERE scope = 'stream'";

/// Per stream and epoch, the nodes that produced live records, and per
/// stream the newest commit time.
pub const SELECT_ACTIVITY: &str = "\
SELECT script, class, cell, facet, incarnation, epoch,
       ARRAY_AGG(DISTINCT node) AS nodes,
       MAX(DATE_PART(epoch_millisecond, committed_at)) AS last_committed_ms
FROM CELL_META_CURRENT
WHERE origin = 'live' AND kind <> 'recovered'
GROUP BY script, class, cell, facet, incarnation, epoch";

/// Per dead session, the `recovered` records held.
pub const SELECT_RECOVERED: &str = "\
SELECT body:session::STRING AS session,
       MAX(body:cells::NUMBER(20, 0)) AS expected,
       COUNT(DISTINCT cell || ':' || body:head:epoch::STRING) AS held,
       BOOLOR_AGG(COALESCE(body:loss::BOOLEAN, FALSE)) AS loss
FROM CELL_META_CURRENT
WHERE kind = 'recovered'
GROUP BY 1";

/// One stream's `rows` and `snapshot` records at or below a position key,
/// for [`super::ConsumerView::state_at`]: the loader feeds them to the
/// reference consumer. Binds: script, class, cell, facet, incarnation,
/// position key.
pub const SELECT_CHANGES_AT: &str = "\
SELECT *
FROM CELL_CHANGES_CURRENT
WHERE script = ? AND class = ? AND cell = ? AND facet = ? AND incarnation = ?
  AND position_key <= ?";

pub const INSERT_FINDING: &str = "\
INSERT INTO EXPORT_RECONCILER_FINDINGS
    (script, class, cell, facet, incarnation, finding, head_epoch, head_txid, detail, found_at)
SELECT ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), CURRENT_TIMESTAMP()";

pub const INSERT_TOMBSTONE: &str = "\
INSERT INTO EXPORT_TOMBSTONES
    (script, class, cell, facet, incarnation, erased_at, reason)
SELECT ?, ?, ?, ?, ?, TO_TIMESTAMP_LTZ(?, 3), ?";

/// Clearing matches the tombstone exactly, a NULL incarnation included.
pub const CLEAR_TOMBSTONE: &str = "\
UPDATE EXPORT_TOMBSTONES
SET cleared_at = TO_TIMESTAMP_LTZ(?, 3)
WHERE script = ? AND class = ? AND cell = ? AND facet = ?
  AND EQUAL_NULL(incarnation, ?)
  AND cleared_at IS NULL";

/// The position key the loader's views compare positions by.
pub fn position_key(epoch: u64, txid: u64, commit: u64) -> String {
    format!("{epoch:020}.{txid:020}.{commit:020}")
}

pub fn finding_binds(finding: &Finding) -> Vec<Json> {
    let s = &finding.stream;
    vec![
        json!(s.script),
        json!(s.class),
        json!(s.cell),
        json!(s.facet.clone().unwrap_or_default()),
        json!(s.incarnation),
        json!(finding.kind.as_str()),
        json!(finding.head.map(|h| h.epoch)),
        json!(finding.head.map(|h| h.txid)),
        json!(serde_json::to_string(&json!({
            "scope": finding.scope,
            "from": finding.from,
            "certified": finding.certified,
            "epochs": finding.epochs,
            "detail": finding.detail,
        }))
        .expect("findings encode")),
    ]
}

pub fn tombstone_binds(t: &Tombstone) -> Vec<Json> {
    vec![
        json!(t.script),
        json!(t.class),
        json!(t.cell),
        json!(t.facet.clone().unwrap_or_default()),
        json!(t.incarnation),
        json!(t.erased_at_ms),
        json!(t.reason),
    ]
}

pub fn clear_binds(t: &Tombstone) -> Vec<Json> {
    vec![
        json!(t.cleared_at_ms),
        json!(t.script),
        json!(t.class),
        json!(t.cell),
        json!(t.facet.clone().unwrap_or_default()),
        json!(t.incarnation),
    ]
}
