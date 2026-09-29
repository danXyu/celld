//! The export record: one JSON object per record, an envelope every record
//! carries and a body per `kind`.

use serde::{Deserialize, Serialize};

use crate::value::Value;

/// Which state a record belongs to. For a root cell `facet` is `None` and
/// `incarnation` is the cell's first epoch; for a facet it is the root's
/// scope, the facet path, and the random incarnation stamped in the facet's
/// `_cf_METADATA` when it was first created.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamId {
    pub script: String,
    pub class: String,
    pub cell: String,
    #[serde(default)]
    pub facet: Option<String>,
    pub incarnation: u64,
}

impl StreamId {
    /// True when `self` is the facet at `path` of the same root, or a facet
    /// below it. A `None` path names the root, which contains every facet.
    pub fn is_at_or_under(
        &self,
        script: &str,
        class: &str,
        cell: &str,
        path: Option<&str>,
    ) -> bool {
        if self.script != script || self.class != class || self.cell != cell {
            return false;
        }
        match (path, self.facet.as_deref()) {
            (None, _) => true,
            (Some(_), None) => false,
            (Some(p), Some(f)) => f == p || (f.starts_with(p) && f[p.len()..].starts_with('/')),
        }
    }
}

/// `(epoch, txid, commit)`. Totally orders a stream: the cell epoch, the LTX
/// transaction id holding the commit's last WAL frame, and the commit's
/// sequence in the epoch.
#[derive(
    Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub struct Position {
    pub epoch: u64,
    pub txid: u64,
    pub commit: u64,
}

impl Position {
    pub const fn new(epoch: u64, txid: u64, commit: u64) -> Self {
        Self {
            epoch,
            txid,
            commit,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Origin {
    Live,
    Snapshot,
    Repair,
}

/// Fields every record carries besides `kind`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Envelope {
    #[serde(flatten)]
    pub stream: StreamId,
    /// `_cf_METADATA.actor_name` when present. Descriptive, not identity.
    #[serde(default)]
    pub cell_name: Option<String>,
    #[serde(flatten)]
    pub position: Position,
    /// Milliseconds since the Unix epoch, from the cell thread at the commit.
    pub committed_at: i64,
    pub node: String,
    pub origin: Origin,
    /// `i` of `fragments`, from one.
    pub fragment: u32,
    pub fragments: u32,
}

/// One record: the envelope and the kind-specific body, encoded as one flat
/// JSON object with a `kind` field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Record {
    #[serde(flatten)]
    pub envelope: Envelope,
    #[serde(flatten)]
    pub body: Body,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    Rows,
    Snapshot,
    SnapshotEnd,
    Schema,
    Link,
    Recovered,
    Deleted,
    Watermark,
    Bulk,
    Gap,
}

impl Kind {
    pub const ALL: [Kind; 10] = [
        Kind::Rows,
        Kind::Snapshot,
        Kind::SnapshotEnd,
        Kind::Schema,
        Kind::Link,
        Kind::Recovered,
        Kind::Deleted,
        Kind::Watermark,
        Kind::Bulk,
        Kind::Gap,
    ];
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Body {
    Rows(RowsBody),
    Snapshot(SnapshotBody),
    SnapshotEnd(SnapshotEndBody),
    Schema(SchemaBody),
    Link(LinkBody),
    Recovered(RecoveredBody),
    Deleted(DeletedBody),
    Watermark(WatermarkBody),
    Bulk(BulkBody),
    Gap(GapBody),
}

impl Body {
    pub fn kind(&self) -> Kind {
        match self {
            Body::Rows(_) => Kind::Rows,
            Body::Snapshot(_) => Kind::Snapshot,
            Body::SnapshotEnd(_) => Kind::SnapshotEnd,
            Body::Schema(_) => Kind::Schema,
            Body::Link(_) => Kind::Link,
            Body::Recovered(_) => Kind::Recovered,
            Body::Deleted(_) => Kind::Deleted,
            Body::Watermark(_) => Kind::Watermark,
            Body::Bulk(_) => Kind::Bulk,
            Body::Gap(_) => Kind::Gap,
        }
    }

    /// The table data of a `rows` or `snapshot` record.
    pub fn table_rows(&self) -> Option<&TableRows> {
        match self {
            Body::Rows(b) => Some(&b.data),
            Body::Snapshot(b) => Some(&b.data),
            _ => None,
        }
    }

    pub(crate) fn table_rows_mut(&mut self) -> Option<&mut TableRows> {
        match self {
            Body::Rows(b) => Some(&mut b.data),
            Body::Snapshot(b) => Some(&mut b.data),
            _ => None,
        }
    }
}

/// `I`, `U`, or `D`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub enum Op {
    #[serde(rename = "I")]
    Insert,
    #[serde(rename = "U")]
    Update,
    #[serde(rename = "D")]
    Delete,
}

/// `[op, key, row]`. `key` holds the values of `key_columns` in order; `row`
/// holds the values of `columns` in order: the full after-image for `I` and
/// `U`, the full before-image for `D`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowChange(pub Op, pub Vec<Value>, pub Vec<Value>);

impl RowChange {
    pub fn op(&self) -> Op {
        self.0
    }
    pub fn key(&self) -> &[Value] {
        &self.1
    }
    pub fn row(&self) -> &[Value] {
        &self.2
    }
}

/// A table at a generation. Rows of different generations never merge.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TableGen {
    pub table: String,
    pub generation: u64,
}

/// The key column list of a table without a declared primary key: the key is
/// the rowid, which the session tracks through
/// `SQLITE_SESSION_OBJCONFIG_ROWID`.
pub const ROWID_KEY_COLUMN: &str = "_rowid_";

/// The part `rows` and `snapshot` share: one table of one commit.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableRows {
    pub table: String,
    pub generation: u64,
    pub columns: Vec<String>,
    /// The declared primary key in declared order, or [`ROWID_KEY_COLUMN`].
    pub key_columns: Vec<String>,
    pub rows: Vec<RowChange>,
}

impl TableRows {
    pub fn table_gen(&self) -> TableGen {
        TableGen {
            table: self.table.clone(),
            generation: self.generation,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowsBody {
    #[serde(flatten)]
    pub data: TableRows,
}

/// Same shape as `rows` with `op` always `I`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotBody {
    pub snapshot_id: String,
    #[serde(flatten)]
    pub data: TableRows,
}

/// What a snapshot replaces. `stream` replaces every table of the stream,
/// so a table generation the snapshot does not list is emptied; `tables`
/// replaces only the listed generations, as the inline snapshot on DDL does.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotScope {
    Stream,
    Tables,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotEndBody {
    pub snapshot_id: String,
    pub scope: SnapshotScope,
    /// The table generations the snapshot covered.
    pub tables: Vec<TableGen>,
    /// The number of `snapshot` records (after reassembly) it closes.
    pub records: u64,
}

/// One column as `PRAGMA table_xinfo` reports it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    /// The declared type, empty when none.
    #[serde(rename = "type")]
    pub decl_type: String,
    /// Position in the primary key from one, or zero.
    #[serde(default)]
    pub pk: u32,
    #[serde(default)]
    pub not_null: bool,
    /// Generated columns (`hidden` 2 or 3 in `table_xinfo`).
    #[serde(default)]
    pub generated: bool,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SchemaBody {
    pub table: String,
    pub generation: u64,
    pub sql: String,
    pub columns: Vec<ColumnDef>,
    /// Closes the generation; the consumer deletes its rows.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub dropped: bool,
    /// Set when this generation opened by renaming another table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed_from: Option<String>,
    /// A virtual table, which the export does not cover.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub unsupported: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkMode {
    /// Nothing restored; the cell starts empty.
    Fresh,
    /// A whole image restored; the epoch's txids start after it.
    Clone,
    /// The predecessor chain paged in; txids continue it.
    Paged,
    /// The same epoch reopened from its local database (a clean reload);
    /// the predecessor is this epoch's own earlier residency.
    Resume,
}

/// Emitted by an activation before the cell serves. A fresh cell has no
/// predecessor.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LinkBody {
    pub start_txid: u64,
    #[serde(default)]
    pub prev_epoch: Option<u64>,
    #[serde(default)]
    pub prev_txid: Option<u64>,
    pub mode: LinkMode,
}

/// Emitted by dead-node recovery for a cell it folded.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveredBody {
    pub session: String,
    pub head: Position,
}

/// Names a stream that no longer exists. With `facet` and `incarnation`
/// absent it names the record's own stream and takes effect at its position.
/// With them present it is emitted on the root's stream and names the facet
/// stream, with `subtree` extending it to every facet below that path.
///
/// A node's facet delete sets `through_incarnation` instead of
/// `incarnation`: it removes every stream at the path (and below it, with
/// `subtree`) whose incarnation is at or below the bound. Facet
/// incarnations are ordered, so a facet recreated after the delete, at the
/// path or below it, has a larger incarnation and is not removed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeletedBody {
    #[serde(
        default,
        rename = "target_facet",
        skip_serializing_if = "Option::is_none"
    )]
    pub facet: Option<String>,
    #[serde(
        default,
        rename = "target_incarnation",
        skip_serializing_if = "Option::is_none"
    )]
    pub incarnation: Option<u64>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub subtree: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub through_incarnation: Option<u64>,
}

/// Certifies a delivered position. `commits` and `records` count what lies
/// in `(from, through]`: distinct commit positions, and whole live records
/// other than watermarks. `from` is the previous watermark's `through`, or
/// absent for the first watermark a node emits for the stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WatermarkBody {
    #[serde(default)]
    pub from: Option<Position>,
    pub through: Position,
    pub commits: u64,
    pub records: u64,
}

/// The listed tables changed at this position in a way the stream does not
/// carry; the consumer's copy of them is unknown until a snapshot covers
/// them.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BulkBody {
    pub tables: Vec<TableGen>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct GapBody {
    pub from: Position,
    pub to: Position,
    pub reason: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("record is not valid export JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("fragment {fragment} of {fragments} is out of range")]
    Fragment { fragment: u32, fragments: u32 },
}

impl Record {
    pub fn kind(&self) -> Kind {
        self.body.kind()
    }

    pub fn stream(&self) -> &StreamId {
        &self.envelope.stream
    }

    pub fn position(&self) -> Position {
        self.envelope.position
    }

    pub fn to_json(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("export records always encode")
    }

    pub fn from_json(bytes: &[u8]) -> Result<Self, DecodeError> {
        let r: Record = serde_json::from_slice(bytes)?;
        let (fragment, fragments) = (r.envelope.fragment, r.envelope.fragments);
        if fragments == 0 || fragment == 0 || fragment > fragments {
            return Err(DecodeError::Fragment {
                fragment,
                fragments,
            });
        }
        Ok(r)
    }
}
