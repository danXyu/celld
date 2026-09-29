//! Each cell's head as the bucket's object names give it.
//!
//! The reconciler reads names only, from a `LIST` walk or an S3 Inventory
//! listing, never the objects. A cell's head is not simply its largest
//! object name: a paged activation continues its predecessor's txids in a new
//! epoch, and a fenced owner's late objects can land in an epoch the chain
//! does not follow. So the names are grouped per cell epoch and handed to
//! [`EpochChain::build`], the same clip-and-link rule a restore uses, over a
//! client that answers listings from memory. The head is then the newest
//! restorable cut of that chain, which is what `export_restore` restores at
//! [`crate::export_restore::Target::Head`].

use std::collections::BTreeMap;

use async_trait::async_trait;
use celld_ltx::client::{epochs::EpochChain, ReplicaClient};
use celld_ltx::compaction_level::SNAPSHOT_LEVEL;
use celld_ltx::error::{Error as LtxError, Result as LtxResult};
use celld_ltx::ltx::{parse_filename, FileInfo};
use celld_ltx::{replica, TXID};

/// Where a bucket's LTX objects live: `cells/<scope>/ltx/e<epoch>/ltx/<level>/<min>-<max>.ltx`.
const CELLS_PREFIX: &str = "cells/";

/// One dead-node recovery that declared a bounded loss:
/// `log/<node>/<generation>.e<epoch>.loss.json`.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Loss {
    /// `<node>/<generation>`.
    pub session: String,
    /// The node-log epoch, not a cell epoch.
    pub epoch: u64,
}

impl Loss {
    /// The node whose writes the loss may have dropped.
    pub fn node(&self) -> &str {
        self.session
            .rsplit_once('/')
            .map_or(self.session.as_str(), |(node, _)| node)
    }
}

/// The object names the reconciler needs, grouped.
#[derive(Default, Debug)]
pub struct Inventory {
    /// Per cell scope (a root, or `<root>/facets/<hash>...`), per epoch, the
    /// LTX files by level.
    cells: BTreeMap<String, BTreeMap<u64, Vec<Vec<FileInfo>>>>,
    /// Per cell scope, the newest object's last-modified time in unix ms.
    modified: BTreeMap<String, i64>,
    losses: Vec<Loss>,
}

/// A cell epoch as the chain serves it: `lo..=hi`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Span {
    pub epoch: u64,
    pub lo: u64,
    pub hi: u64,
}

/// What the bucket holds for one cell scope.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BucketHead {
    pub scope: String,
    /// `(epoch, txid)` of the newest restorable cut.
    pub epoch: u64,
    pub txid: u64,
    /// The chain's epochs, oldest first. The last span ends at the head;
    /// every other ends just short of the next one's first txid.
    pub spans: Vec<Span>,
    /// The newest object of the scope, in unix ms.
    pub modified_ms: i64,
}

impl BucketHead {
    /// The root cell scope, before any `/facets/`.
    pub fn root(&self) -> &str {
        root_of(&self.scope)
    }

    pub fn is_facet(&self) -> bool {
        self.scope.contains("/facets/")
    }

    pub fn span(&self, epoch: u64) -> Option<&Span> {
        self.spans.iter().find(|s| s.epoch == epoch)
    }
}

pub fn root_of(scope: &str) -> &str {
    scope.split_once("/facets/").map_or(scope, |(root, _)| root)
}

/// The class of a root scope, `Class:id`.
pub fn class_of(scope: &str) -> &str {
    let root = root_of(scope);
    root.split_once(':').map_or(root, |(class, _)| class)
}

impl Inventory {
    pub fn new() -> Self {
        Self::default()
    }

    /// Take one object name, unprefixed, with its last-modified time.
    /// Names that are neither an LTX object nor a loss record are ignored.
    pub fn add(&mut self, key: &str, modified_ms: i64) {
        if let Some(loss) = parse_loss(key) {
            self.losses.push(loss);
            return;
        }
        let Some((scope, epoch, level, file)) = parse_ltx(key) else {
            return;
        };
        let levels = self
            .cells
            .entry(scope.to_string())
            .or_default()
            .entry(epoch)
            .or_insert_with(|| vec![Vec::new(); SNAPSHOT_LEVEL as usize + 1]);
        levels[level as usize].push(file);
        let newest = self
            .modified
            .entry(scope.to_string())
            .or_insert(modified_ms);
        *newest = (*newest).max(modified_ms);
    }

    pub fn losses(&self) -> &[Loss] {
        &self.losses
    }

    pub fn scopes(&self) -> impl Iterator<Item = &str> {
        self.cells.keys().map(String::as_str)
    }

    pub fn contains(&self, scope: &str) -> bool {
        self.cells.contains_key(scope)
    }

    /// Every scope's head. A scope whose objects form no chain (nothing
    /// that opens with a snapshot, or only a hole) has no head and is left
    /// out: a restore would refuse it too.
    pub async fn heads(&self) -> BTreeMap<String, BucketHead> {
        let mut out = BTreeMap::new();
        for scope in self.cells.keys() {
            if let Some(head) = self.head(scope).await {
                out.insert(scope.clone(), head);
            }
        }
        out
    }

    pub async fn head(&self, scope: &str) -> Option<BucketHead> {
        let epochs = self.cells.get(scope)?;
        let clients = epochs
            .iter()
            .map(|(epoch, levels)| (*epoch, Listed(levels.clone())))
            .collect();
        let chain = EpochChain::build(clients).await.ok()?;
        let cut = *replica::restorable_cuts(&chain).await.ok()?.last()?;
        let bounds = chain.spans();
        let serving = bounds
            .iter()
            .rev()
            .find(|(_, lo)| *lo <= cut)
            .or(bounds.first())?;
        let spans = bounds
            .iter()
            .enumerate()
            .map(|(i, (epoch, lo))| Span {
                epoch: *epoch,
                lo: lo.0,
                hi: bounds.get(i + 1).map_or(cut.0, |(_, next)| next.0 - 1),
            })
            .filter(|s| s.lo <= s.hi)
            .collect();
        Some(BucketHead {
            scope: scope.to_string(),
            epoch: serving.0,
            txid: cut.0,
            spans,
            modified_ms: self.modified.get(scope).copied().unwrap_or(0),
        })
    }
}

/// `cells/<scope>/ltx/e<epoch>/ltx/<level>/<min>-<max>.ltx`.
fn parse_ltx(key: &str) -> Option<(&str, u64, i32, FileInfo)> {
    let rest = key.strip_prefix(CELLS_PREFIX)?;
    let (scope, rest) = rest.split_once("/ltx/e")?;
    let (epoch, rest) = rest.split_once("/ltx/")?;
    let (level, name) = rest.split_once('/')?;
    let epoch: u64 = epoch.parse().ok()?;
    let level: i32 = level.parse().ok()?;
    if !(0..=SNAPSHOT_LEVEL).contains(&level) || name.contains('/') {
        return None;
    }
    let (min_txid, max_txid) = parse_filename(name).ok()?;
    Some((
        scope,
        epoch,
        level,
        FileInfo {
            level,
            min_txid,
            max_txid,
            ..Default::default()
        },
    ))
}

/// `log/<node>/<generation>.e<epoch>.loss.json`. A bundle's own loss record
/// (`.bundle-<name>.loss.json`) is not a session loss and is ignored.
fn parse_loss(key: &str) -> Option<Loss> {
    let stem = key.strip_prefix("log/")?.strip_suffix(".loss.json")?;
    let (session, epoch) = stem.rsplit_once(".e")?;
    let epoch = epoch.parse().ok()?;
    session.contains('/').then(|| Loss {
        session: session.to_string(),
        epoch,
    })
}

/// A replica client over listings already in memory. The chain only lists;
/// a read would mean the reconciler tried to restore, which it never does.
struct Listed(Vec<Vec<FileInfo>>);

fn listing_only() -> LtxError {
    LtxError::Other("the reconciler reads object names only".into())
}

#[async_trait]
impl ReplicaClient for Listed {
    async fn ltx_files(&self, level: i32, seek: TXID) -> LtxResult<Vec<FileInfo>> {
        let mut files: Vec<FileInfo> = self
            .0
            .get(level as usize)
            .into_iter()
            .flatten()
            .filter(|f| f.min_txid >= seek)
            .cloned()
            .collect();
        files.sort_by_key(|f| (f.min_txid, f.max_txid));
        Ok(files)
    }

    async fn ltx_files_bounded(
        &self,
        level: i32,
        seek: TXID,
        limit: usize,
    ) -> LtxResult<Vec<FileInfo>> {
        let mut files = self.ltx_files(level, seek).await?;
        files.truncate(limit);
        Ok(files)
    }

    async fn open_ltx_file(&self, _: i32, _: TXID, _: TXID) -> LtxResult<Vec<u8>> {
        Err(listing_only())
    }

    async fn read_range(&self, _: i32, _: TXID, _: TXID, _: u64, _: u64) -> LtxResult<Vec<u8>> {
        Err(listing_only())
    }

    async fn write_ltx_file(&self, _: i32, _: TXID, _: TXID, _: &[u8]) -> LtxResult<FileInfo> {
        Err(listing_only())
    }

    async fn write_ltx_file_from_file(
        &self,
        _: i32,
        _: TXID,
        _: TXID,
        _: celld_ltx::host::HostFile,
        _: celld_ltx::LtxHost,
    ) -> LtxResult<FileInfo> {
        Err(listing_only())
    }

    async fn delete_ltx_files(&self, _: &[FileInfo]) -> LtxResult<()> {
        Err(listing_only())
    }

    async fn delete_all(&self) -> LtxResult<()> {
        Err(listing_only())
    }
}
