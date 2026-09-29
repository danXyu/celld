//! Dedup keys. The stream is at-least-once and a flush can be retried, so a
//! consumer drops a record, or a row, whose key it already holds.
//!
//! The design's key is `(stream, position, table, generation, key,
//! fragment)`, which is the per-row key of a table that flattens `rows`
//! records ([`RowDedupKey`]). A record-level consumer uses [`DedupKey`],
//! which adds what tells two non-row records at one position apart: the
//! kind, the origin, and the snapshot id.

use crate::record::{Body, Kind, Origin, Position, Record, StreamId, TableGen};
use crate::value::Value;

/// Identifies a whole record, all fragments together.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RecordKey {
    pub stream: StreamId,
    pub position: Position,
    pub kind: Kind,
    pub origin: Origin,
    /// Table and generation for `rows`, `snapshot`, and `schema`.
    pub table: Option<(String, u64)>,
    /// For `snapshot` and `snapshot_end`: two repairs at one position are two
    /// snapshots.
    pub snapshot_id: Option<String>,
    /// For `deleted`: the stream it names, when not its own.
    pub target: Option<(String, u64)>,
    /// For `bulk`: the generations it marks, so markers for two tables of
    /// one commit stay distinct.
    pub bulk_tables: Vec<TableGen>,
}

impl RecordKey {
    pub fn of(r: &Record) -> Self {
        let (table, snapshot_id, target) = match &r.body {
            Body::Rows(b) => (Some((b.data.table.clone(), b.data.generation)), None, None),
            Body::Snapshot(b) => (
                Some((b.data.table.clone(), b.data.generation)),
                Some(b.snapshot_id.clone()),
                None,
            ),
            Body::Schema(b) => (Some((b.table.clone(), b.generation)), None, None),
            Body::SnapshotEnd(b) => (None, Some(b.snapshot_id.clone()), None),
            Body::Deleted(b) => (
                None,
                None,
                b.facet
                    .clone()
                    .map(|f| (f, b.incarnation.or(b.through_incarnation).unwrap_or(0))),
            ),
            _ => (None, None, None),
        };
        let bulk_tables = match &r.body {
            Body::Bulk(b) => {
                let mut t = b.tables.clone();
                t.sort();
                t
            }
            _ => Vec::new(),
        };
        Self {
            stream: r.envelope.stream.clone(),
            position: r.envelope.position,
            kind: r.kind(),
            origin: r.envelope.origin,
            table,
            snapshot_id,
            target,
            bulk_tables,
        }
    }
}

/// Identifies one fragment of one record.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DedupKey {
    pub record: RecordKey,
    pub fragment: u32,
    pub fragments: u32,
}

impl DedupKey {
    pub fn of(r: &Record) -> Self {
        Self {
            record: RecordKey::of(r),
            fragment: r.envelope.fragment,
            fragments: r.envelope.fragments,
        }
    }
}

/// The design's per-row key, for a consumer that stores one row per change.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RowDedupKey {
    pub stream: StreamId,
    pub position: Position,
    pub table: String,
    pub generation: u64,
    pub key: Vec<Value>,
    pub fragment: u32,
}

/// The per-row keys of a `rows` or `snapshot` record; empty for other kinds.
pub fn row_keys(r: &Record) -> Vec<RowDedupKey> {
    let Some(data) = r.body.table_rows() else {
        return Vec::new();
    };
    data.rows
        .iter()
        .map(|row| RowDedupKey {
            stream: r.envelope.stream.clone(),
            position: r.envelope.position,
            table: data.table.clone(),
            generation: data.generation,
            key: row.key().to_vec(),
            fragment: r.envelope.fragment,
        })
        .collect()
}
