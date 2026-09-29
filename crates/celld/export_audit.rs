// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Change export's audit side: the reconciler, `verify` and `erase`
//! (`docs/design/change-export.md`, "Completeness", "Snapshots and repair"
//! and "Erasure").
//!
//! All three compare the bucket with what a consumer holds, so they need the
//! consumer's read side. [`ConsumerView`] is that read side as a trait:
//!
//! - [`BucketConsumer`] is the reference consumer from `celld-export-format`
//!   fed with every record the bucket sink wrote under `export/changes/`. It
//!   is the consumer of a fleet that runs only the bucket sink, and the
//!   oracle the tests use.
//! - The Snowflake loader (`celld-export-loader`) implements it over
//!   `CELL_CERTIFIED`, `CELL_STREAMS`, `CELL_SNAPSHOTS` and
//!   `CELL_CHANGES_CURRENT`, and writes findings and tombstones with the
//!   statements in [`snowflake`]. That is where the reconciler runs on its
//!   schedule (`CELLD_EXPORT_RECONCILE`).
//!
//! Nothing here writes to a cell's objects. The reconciler reads object
//! names, `verify` restores read-only through [`crate::export_restore`], and
//! what the audit writes goes under `export/` only: tombstones, reports, and
//! `gap` and `deleted` records in the bucket sink's own layout.
#![allow(clippy::disallowed_methods)] // Offline operator path, outside Actor execution.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Context as _;
use async_trait::async_trait;
use celld_export_format::{Body, Consumer, Origin, Position, Record, StreamId, StreamState};
use serde::Serialize;

use crate::bucket::Bucket;
use crate::export_sink::CHANGES_PREFIX;

mod cache;
pub mod cli;
pub mod inventory;
pub mod reconcile;
pub mod snowflake;
pub mod tombstone;
pub mod verify;

pub use tombstone::{is_tombstoned, Tombstone};

/// Where the audit keeps its reports.
pub const REPORTS_PREFIX: &str = "export/reconcile";

/// The `node` the audit's own records carry.
pub const AUDIT_NODE: &str = "reconciler";

/// What the reconciler needs to know about one consumer stream.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct StreamSummary {
    pub id: StreamId,
    /// Per epoch, the highest certified position.
    pub certified: BTreeMap<u64, Position>,
    /// The complete stream-wide snapshot the state starts from.
    pub snapshot_at: Option<Position>,
    /// A `deleted` record naming the stream itself removed it here.
    pub deleted_at: Option<Position>,
    /// Per epoch, the nodes that produced the stream's live records.
    pub nodes: BTreeMap<u64, BTreeSet<String>>,
    /// The newest `committed_at` of any of its records, in unix ms.
    pub last_committed_ms: i64,
}

impl StreamSummary {
    /// A stream with nothing certified yet.
    pub fn new(id: StreamId) -> Self {
        Self {
            id,
            certified: BTreeMap::new(),
            snapshot_at: None,
            deleted_at: None,
            nodes: BTreeMap::new(),
            last_committed_ms: 0,
        }
    }

    /// True when a stream-wide snapshot at or past `(epoch, txid)` replaced
    /// everything up to it.
    pub fn covered(&self, epoch: u64, txid: u64) -> bool {
        self.snapshot_at
            .is_some_and(|s| (s.epoch, s.txid) >= (epoch, txid))
    }

    /// The highest certified position of any epoch.
    pub fn certified_head(&self) -> Option<Position> {
        self.certified.values().max().copied()
    }
}

/// The `recovered` records a consumer holds for one dead session.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RecoveredSession {
    pub session: String,
    /// The `cells` count the records carry: how many recovery sent.
    pub expected: u64,
    /// How many distinct cell epochs the consumer holds.
    pub held: u64,
    pub loss: bool,
}

/// What the reconciler found that the stream does not say.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FindingKind {
    /// The bucket holds changes the consumer has not certified.
    Gap,
    /// The consumer certified changes the cell no longer has: past the end
    /// the chain gives a closed epoch, in an epoch the chain skipped, or past
    /// the head after a declared loss.
    Lost,
    /// A facet the consumer holds has no objects in the bucket, while its
    /// root does: it was deleted and the `deleted` record never arrived.
    MissingDeleted,
    /// The bucket holds a cell the consumer has never seen. Backfill it.
    UnknownStream,
    /// The bucket holds objects for a cell that form no restorable chain: no
    /// snapshot to start from, or a hole. Neither a repair nor a deletion can
    /// be inferred; an operator has to look.
    Unrestorable,
}

impl FindingKind {
    pub fn as_str(self) -> &'static str {
        match self {
            FindingKind::Gap => "gap",
            FindingKind::Lost => "lost",
            FindingKind::MissingDeleted => "missing_deleted",
            FindingKind::UnknownStream => "unknown_stream",
            FindingKind::Unrestorable => "unrestorable",
        }
    }
}

/// One reconciler finding, in the shape of a row of
/// `EXPORT_RECONCILER_FINDINGS`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Finding {
    /// The consumer stream. For [`FindingKind::UnknownStream`] the consumer
    /// has none, so the script is empty, the incarnation 0, and a facet is
    /// named by its bucket path, `facets/<hash>...`.
    pub stream: StreamId,
    pub kind: FindingKind,
    /// The bucket scope the finding is about.
    pub scope: String,
    /// The bucket head, where a repair snapshot must reach.
    pub head: Option<Position>,
    /// Where the difference starts.
    pub from: Option<Position>,
    /// What the consumer certified in the epoch concerned.
    pub certified: Option<Position>,
    /// The epochs concerned.
    pub epochs: Vec<u64>,
    pub detail: String,
}

/// A consumer's read side, and the two writes the audit makes to it.
#[async_trait]
pub trait ConsumerView: Send + Sync {
    /// Every stream the consumer holds that is not removed: not erased and
    /// not named by a facet `deleted` record.
    async fn streams(&self) -> anyhow::Result<Vec<StreamSummary>>;

    /// The stream's state from its records at or below `at` only, so it can
    /// be compared with a restore at `at` while newer changes keep arriving.
    async fn state_at(
        &self,
        stream: &StreamId,
        at: Position,
    ) -> anyhow::Result<Option<StreamState>>;

    /// The `recovered` records held, per dead session.
    async fn recovered(&self) -> anyhow::Result<Vec<RecoveredSession>>;

    /// Store the reconciler's findings where the repair driver polls them.
    async fn record_findings(&self, findings: &[Finding]) -> anyhow::Result<()>;

    /// Store a tombstone, or its clearing, in the consumer's own table. The
    /// bucket object is written separately and first.
    async fn tombstone(&self, tombstone: &Tombstone) -> anyhow::Result<()>;
}

/// The reference consumer over the bucket sink's records.
pub struct BucketConsumer {
    bucket: Bucket,
    cache: cache::Cache,
    tombstones: Vec<Tombstone>,
    only_cell: Option<String>,
    max_cell_history: usize,
}

impl BucketConsumer {
    /// Index bucket history on disk and evaluate cells on demand.
    pub async fn load(bucket: Bucket) -> anyhow::Result<Self> {
        Self::load_cached(bucket, None, "temporary", None).await
    }

    /// Reuse unchanged objects from an operator cache. `identity` must include
    /// the endpoint, bucket and prefix; a cache cannot be reused across scopes.
    pub async fn load_cached(
        bucket: Bucket,
        path: Option<&std::path::Path>,
        identity: &str,
        only_cell: Option<String>,
    ) -> anyhow::Result<Self> {
        let cache = cache::Cache::open(path, identity)?;
        let tombstones = tombstone::load(&bucket).await?;
        cache.refresh(&bucket, &tombstones).await?;
        Ok(Self {
            bucket,
            cache,
            tombstones,
            only_cell,
            max_cell_history: cache::MAX_CELL_HISTORY,
        })
    }

    pub fn from_records(
        bucket: Bucket,
        records: Vec<Record>,
        tombstones: &[Tombstone],
    ) -> anyhow::Result<Self> {
        let cache = cache::Cache::open(None, "test")?;
        cache.insert_records(records)?;
        Ok(Self {
            bucket,
            cache,
            tombstones: tombstones.to_vec(),
            only_cell: None,
            max_cell_history: cache::MAX_CELL_HISTORY,
        })
    }

    /// Maximum encoded history loaded for any one cell (default 64 MiB).
    pub fn with_history_limit(mut self, bytes: usize) -> anyhow::Result<Self> {
        anyhow::ensure!(bytes > 0, "--max-cell-history must be positive");
        self.max_cell_history = bytes;
        Ok(self)
    }

    pub fn bucket(&self) -> &Bucket {
        &self.bucket
    }
}

#[async_trait]
impl ConsumerView for BucketConsumer {
    async fn streams(&self) -> anyhow::Result<Vec<StreamSummary>> {
        let mut summaries = Vec::new();
        for cell in self.cache.cells(self.only_cell.as_deref())? {
            let records = self
                .cache
                .records(&cell, &self.tombstones, self.max_cell_history)?;
            let mut extra: BTreeMap<StreamId, (BTreeMap<u64, BTreeSet<String>>, i64, bool)> =
                BTreeMap::new();
            for r in &records {
                let entry = extra
                    .entry(r.stream().clone())
                    .or_insert_with(|| (BTreeMap::new(), 0, true));
                entry.2 &= matches!(r.body, Body::Recovered(_));
                entry.1 = entry.1.max(r.envelope.committed_at);
                if r.envelope.origin == Origin::Live && !matches!(r.body, Body::Recovered(_)) {
                    entry
                        .0
                        .entry(r.position().epoch)
                        .or_default()
                        .insert(r.envelope.node.clone());
                }
            }
            let recovery_only: BTreeSet<_> = extra
                .iter()
                .filter(|(id, (_, _, only))| id.script.is_empty() && id.incarnation == 0 && *only)
                .map(|(id, _)| id.clone())
                .collect();
            let mut consumer = Consumer::new();
            consumer
                .ingest_all(records)
                .map_err(|e| anyhow::anyhow!("reassemble export records: {e}"))?;
            for (id, state) in consumer.state() {
                if recovery_only.contains(&id) {
                    continue;
                }
                let (nodes, last, _) = extra.remove(&id).unwrap_or_default();
                summaries.push(StreamSummary {
                    id,
                    certified: state.certified,
                    snapshot_at: state.snapshot_at,
                    deleted_at: state.deleted_at,
                    nodes,
                    last_committed_ms: last,
                });
            }
        }
        Ok(summaries)
    }

    async fn state_at(
        &self,
        stream: &StreamId,
        at: Position,
    ) -> anyhow::Result<Option<StreamState>> {
        let records = self
            .cache
            .records(&stream.cell, &self.tombstones, self.max_cell_history)?;
        let mut consumer = Consumer::new();
        // Keep recovery placeholders and root deletion records so adoption and
        // facet deletion have the same semantics as the full consumer.
        consumer
            .ingest_all(records.into_iter().filter(|r| r.position() <= at))
            .map_err(|e| anyhow::anyhow!("reassemble export records: {e}"))?;
        Ok(consumer.stream(stream))
    }

    async fn recovered(&self) -> anyhow::Result<Vec<RecoveredSession>> {
        type Held = BTreeSet<(String, Option<String>, u64)>;
        let mut sessions: BTreeMap<String, (u64, Held, bool)> = BTreeMap::new();
        let db = self.cache.db.lock().unwrap();
        let mut stmt =
            db.prepare("SELECT data FROM records WHERE recovered=1 AND (?1 IS NULL OR cell=?1)")?;
        let mut rows = stmt.query([self.only_cell.as_deref()])?;
        while let Some(row) = rows.next()? {
            let r: Record = serde_json::from_slice(row.get_ref(0)?.as_blob()?)?;
            if self.tombstones.iter().any(|t| t.matches(r.stream())) {
                continue;
            }
            if let Body::Recovered(b) = &r.body {
                let s = sessions.entry(b.session.clone()).or_default();
                s.0 = s.0.max(b.cells);
                s.1.insert((
                    r.stream().cell.clone(),
                    r.stream().facet.clone(),
                    b.head.epoch,
                ));
                s.2 |= b.loss;
            }
        }
        Ok(sessions
            .into_iter()
            .map(|(session, (expected, held, loss))| RecoveredSession {
                session,
                expected,
                held: held.len() as u64,
                loss,
            })
            .collect())
    }

    async fn record_findings(&self, findings: &[Finding]) -> anyhow::Result<()> {
        if findings.is_empty() {
            return Ok(());
        }
        let key = format!("{REPORTS_PREFIX}/{}.json", crate::asyncrt::wall_ms());
        self.bucket
            .put(&key, serde_json::to_vec_pretty(findings)?)
            .await
            .with_context(|| format!("write reconciler report {key}"))
    }

    async fn tombstone(&self, _: &Tombstone) -> anyhow::Result<()> {
        // The bucket object is this consumer's tombstone table.
        Ok(())
    }
}

/// Write the audit's own records (`gap`, `deleted`) as one bucket-sink
/// object, where every consumer of the bucket sink and the Snowflake
/// `COPY INTO` path pick them up. Returns the key.
pub async fn emit(bucket: &Bucket, records: Vec<Record>) -> anyhow::Result<Option<String>> {
    if records.is_empty() {
        return Ok(None);
    }
    let bytes = crate::export_sink::encode_records(records)?;
    let key = crate::parquet_batch::put(
        bucket,
        CHANGES_PREFIX,
        AUDIT_NODE,
        crate::asyncrt::wall_ms() * 1000,
        bytes,
        &[("celld-origin", "reconciler")],
    )
    .await
    .map_err(|error| error.error.context(format!("write {}", error.key)))?;
    Ok(Some(key))
}

#[cfg(test)]
mod tests;
