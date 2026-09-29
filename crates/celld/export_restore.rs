// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Read-only restore of a cell from the bucket, at its head or at the first
//! cut at or after a position, for change export's repair, backfill and
//! verify (`docs/design/change-export.md`, "Snapshots and repair").
//!
//! This opens bucket objects offline, which the operator commands otherwise
//! never do (`operator_cell`): a write there lands behind the fence and the
//! owner's next flush overwrites it. So nothing here can write. Every epoch's
//! client is wrapped in [`ReadOnly`], whose write and delete calls fail, and
//! the restored image is a private temporary file opened `immutable`.
//!
//! The restorable positions are the cuts the bucket holds. A restore lands at
//! the requested position or the first cut above it and reports the position
//! it actually restored. That is also true of the head: the head here is the
//! newest cut in the bucket, which is not the fleet's head. `log/` bundle
//! tails that recovery has not folded yet, and rows the fleet acknowledged
//! but has not flushed, are in no per-cell object, so a restore cannot see
//! them. A caller that snapshots "at the head" must carry the reported
//! position forward and leave the rest to the next live change or the next
//! reconcile, not claim the stream is whole through the fleet's position.
#![allow(clippy::disallowed_methods)] // Offline operator path, outside Actor execution.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{anyhow, ensure, Context};
use async_trait::async_trait;
use celld_ltx::client::{
    epochs::EpochChain,
    object_store::{ObjectStoreClient, ObjectStoreConfig},
    ReplicaClient,
};
use celld_ltx::error::{Error as LtxError, Result as LtxResult};
use celld_ltx::ltx::FileInfo;
use celld_ltx::{replica, TXID};
use object_store::path::Path as ObjectPath;
use rusqlite::{Connection, OpenFlags};
use tokio::sync::Semaphore;

use crate::bucket::Bucket;

/// Object downloads in flight for one restore.
const DOWNLOAD_CONCURRENCY: usize = 8;

/// A point in a cell's history. Txids restart with an epoch that opens with
/// a snapshot, so positions order by epoch first. The export position's
/// commit index is finer than any cut: a cut at a txid holds every commit in
/// it, so a restore at or after `(epoch, txid)` is at or after every commit
/// of that txid too.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Position {
    pub epoch: u64,
    pub txid: u64,
}

impl std::fmt::Display for Position {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "e{}:{}", self.epoch, self.txid)
    }
}

/// Where to restore.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Target {
    /// The newest cut in the bucket.
    Head,
    /// The first cut at or after this position.
    AtOrAfter(Position),
}

/// A restored image in a private directory, removed on drop.
pub struct Restored {
    /// The position the image holds.
    pub position: Position,
    /// The newest cut in the bucket when the restore planned. Equal to
    /// `position` for [`Target::Head`]. Not the fleet's head.
    pub bucket_head: Position,
    path: PathBuf,
    _dir: tempfile::TempDir,
}

impl Restored {
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// A read-only connection for scanning tables. The image is private and
    /// never changes, so it opens `immutable`: no locks, and no `-wal` or
    /// `-shm` beside it even when its header says WAL.
    pub fn open(&self) -> anyhow::Result<Connection> {
        open_read_only(&self.path)
    }
}

/// Restore `scope` from `bucket` without writing to it.
pub async fn restore(bucket: &Bucket, scope: &str, target: Target) -> anyhow::Result<Restored> {
    ensure!(
        celld_logic::cell::valid_cell_scope(scope),
        "invalid cell scope {scope:?}"
    );
    let chain = chain(bucket, scope).await?;
    let spans = chain.spans();
    let cuts = replica::restorable_cuts(&chain)
        .await
        .map_err(|error| anyhow!("list cuts of {scope}: {error}"))?;
    let head = cuts
        .last()
        .map(|cut| position_of(&spans, *cut))
        .with_context(|| format!("{scope} has no restorable cut in the bucket"))?;
    // The head plans as every other head restore does, which also refuses a
    // bucket whose newest objects start past a hole.
    let (position, plan) = match target {
        Target::Head => (head, replica::calc_restore_plan(&chain, TXID(0)).await),
        Target::AtOrAfter(at) => {
            let position = choose_cut(&spans, &cuts, at).with_context(|| {
                format!("{scope} has no cut at or after {at}; bucket head {head}")
            })?;
            let plan = replica::calc_restore_plan(&chain, TXID(position.txid)).await;
            (position, plan)
        }
    };
    let plan = plan.map_err(|error| anyhow!("plan {scope} at {position}: {error}"))?;
    ensure!(
        plan.iter().map(|f| f.max_txid).max() == Some(TXID(position.txid)),
        "plan for {scope} does not end at {position}"
    );
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("db.sqlite");
    replica::restore_from_plan_with_download_slots(
        &chain,
        &path,
        plan,
        Arc::new(Semaphore::new(DOWNLOAD_CONCURRENCY)),
    )
    .await
    .map_err(|error| anyhow!("restore {scope} at {position}: {error}"))?;
    Ok(Restored {
        position,
        bucket_head: head,
        path,
        _dir: dir,
    })
}

/// The chain over every epoch of `scope` in the bucket, each read-only.
async fn chain(bucket: &Bucket, scope: &str) -> anyhow::Result<EpochChain<ReadOnly>> {
    let base = ObjectPath::from(format!("{}cells/{scope}/ltx", bucket.prefix));
    let listing = bucket.store.list_with_delimiter(Some(&base)).await?;
    let mut epochs: Vec<u64> = listing
        .common_prefixes
        .iter()
        .filter_map(|p| p.filename()?.strip_prefix('e')?.parse().ok())
        .collect();
    epochs.sort_unstable();
    ensure!(!epochs.is_empty(), "{scope} has nothing in the bucket");
    let clients = epochs
        .into_iter()
        .map(|epoch| {
            let config = ObjectStoreConfig {
                path: format!("{}cells/{scope}/ltx/e{epoch}", bucket.prefix),
                ..Default::default()
            };
            let client = ObjectStoreClient::with_store(config, bucket.store.clone());
            (epoch, ReadOnly(client))
        })
        .collect();
    EpochChain::build(clients)
        .await
        .map_err(|error| anyhow!("chain epochs of {scope}: {error}"))
}

/// The position of a chain txid: the epoch of the span that serves it.
/// `spans` is [`EpochChain::spans`], oldest first; a paged epoch continues
/// its predecessor's txids, so the span is the newest that starts at or
/// below the txid.
fn position_of(spans: &[(u64, TXID)], txid: TXID) -> Position {
    let epoch = spans
        .iter()
        .rev()
        .find(|(_, lo)| *lo <= txid)
        .or(spans.first())
        .map_or(0, |(epoch, _)| *epoch);
    Position {
        epoch,
        txid: txid.0,
    }
}

/// The first cut whose position is at or after `at`. A position in an
/// epoch older than the chain, or one the chain skipped, lands on the first
/// cut of the next epoch the chain holds: that is after it, and the state
/// between is repaired by replacement, not replayed.
fn choose_cut(spans: &[(u64, TXID)], cuts: &[TXID], at: Position) -> Option<Position> {
    cuts.iter()
        .map(|cut| position_of(spans, *cut))
        .find(|position| *position >= at)
}

fn open_read_only(path: &Path) -> anyhow::Result<Connection> {
    let uri = format!("file:{}?immutable=1", uri_path(path)?);
    let db = Connection::open_with_flags(
        uri,
        OpenFlags::SQLITE_OPEN_READ_ONLY
            | OpenFlags::SQLITE_OPEN_URI
            | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    db.execute_batch("PRAGMA query_only=ON; PRAGMA trusted_schema=OFF;")?;
    Ok(db)
}

/// `path` escaped for a SQLite URI, which gives `?`, `#` and `%` meaning.
fn uri_path(path: &Path) -> anyhow::Result<String> {
    let path = path.to_str().context("restore path is not UTF-8")?;
    let mut out = String::with_capacity(path.len());
    for c in path.chars() {
        match c {
            '?' => out.push_str("%3f"),
            '#' => out.push_str("%23"),
            '%' => out.push_str("%25"),
            c => out.push(c),
        }
    }
    Ok(out)
}

/// A replica client that can only read. The chain routes every call by txid
/// to one of these, so no path through a restore can reach a write.
pub struct ReadOnly(ObjectStoreClient);

fn refused(what: &str) -> LtxError {
    LtxError::Other(format!("export restore is read-only: refused {what}").into())
}

#[async_trait]
impl ReplicaClient for ReadOnly {
    async fn ltx_files(&self, level: i32, seek: TXID) -> LtxResult<Vec<FileInfo>> {
        self.0.ltx_files(level, seek).await
    }

    async fn ltx_files_bounded(
        &self,
        level: i32,
        seek: TXID,
        limit: usize,
    ) -> LtxResult<Vec<FileInfo>> {
        self.0.ltx_files_bounded(level, seek, limit).await
    }

    async fn open_ltx_file(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
    ) -> LtxResult<Vec<u8>> {
        self.0.open_ltx_file(level, min_txid, max_txid).await
    }

    async fn read_range(
        &self,
        level: i32,
        min_txid: TXID,
        max_txid: TXID,
        offset: u64,
        len: u64,
    ) -> LtxResult<Vec<u8>> {
        self.0
            .read_range(level, min_txid, max_txid, offset, len)
            .await
    }

    async fn write_ltx_file(&self, _: i32, _: TXID, _: TXID, _: &[u8]) -> LtxResult<FileInfo> {
        Err(refused("write"))
    }

    async fn write_ltx_file_from_file(
        &self,
        _: i32,
        _: TXID,
        _: TXID,
        _: celld_ltx::host::HostFile,
        _: celld_ltx::LtxHost,
    ) -> LtxResult<FileInfo> {
        Err(refused("upload"))
    }

    async fn delete_ltx_files(&self, _: &[FileInfo]) -> LtxResult<()> {
        Err(refused("delete"))
    }

    async fn delete_all(&self) -> LtxResult<()> {
        Err(refused("delete"))
    }
}

#[cfg(test)]
mod tests;
