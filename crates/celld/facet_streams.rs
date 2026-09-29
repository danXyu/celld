// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The replication streams of Durable Object facets, shared by both engines.
//!
//! A facet is a database and a replication stream of its own, as a sibling
//! file is in workerd's own server. Its stream nests under the root's
//! coordinates (`engine_api::facet_cell`), is activated on first use with the
//! restore its root's activation used, and stops with the root. A facet has
//! no ownership record and no fence of its own; the root's cover it, since a
//! facet runs only inside its root.
//!
//! # Incarnations
//!
//! Change export names a facet's stream by the root, the facet's path, and
//! an **incarnation** the facet's `_cf_METADATA` keeps from its first open
//! (`docs/design/change-export.md`, "Facets"). A delete removes a facet and
//! every facet below it, including ones that are not resident and whose
//! incarnations this node never read, so incarnations are ordered rather
//! than random: every incarnation handed out before a delete is below the
//! delete's bound, and every one handed out after it is above. A `deleted`
//! record carries that bound, and a consumer removes exactly the streams at
//! or under the path whose incarnation is at or below it; a facet recreated
//! later, nested or not, survives.
//!
//! An incarnation is the root's epoch in the top [`INCARNATION_EPOCH_BITS`]
//! bits and a counter in the rest. Epochs grow with every activation, which
//! orders incarnations across nodes whatever their clocks. Within an epoch
//! the counter starts from the wall clock, in milliseconds, when the root
//! registers, so a root that restarts in place at the same epoch (a clean
//! reload) counts on from above what it handed out before, as long as it
//! handed out fewer than one incarnation per millisecond of its residency.

use anyhow::anyhow;
use anyhow::Context as _;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;

pub(crate) use crate::host_channels::FacetFile;
use crate::ltx_replication::Replication;

/// The backoff of a facet stop that failed after its root stopped.
const FACET_STOP_RETRY_FIRST: std::time::Duration = std::time::Duration::from_millis(50);
const FACET_STOP_RETRY_MAX: std::time::Duration = std::time::Duration::from_secs(5);

/// The bits of an incarnation that hold the root's epoch.
pub(crate) const INCARNATION_EPOCH_BITS: u32 = 24;
const INCARNATION_COUNTER_BITS: u32 = 64 - INCARNATION_EPOCH_BITS;
const INCARNATION_COUNTER_MAX: u64 = (1 << INCARNATION_COUNTER_BITS) - 1;
/// 2026-01-01T00:00:00Z: the counter's wall-clock seed counts milliseconds
/// from here, which leaves it room for about 34 years.
const INCARNATION_CLOCK_BASE_MS: i64 = 1_767_225_600_000;

/// The incarnation counter of one root at one epoch.
#[derive(Clone, Copy, Debug)]
struct Incarnations {
    epoch: u64,
    next: u64,
}

impl Incarnations {
    fn new(epoch: u64, wall_ms: i64) -> Self {
        let seed = wall_ms.saturating_sub(INCARNATION_CLOCK_BASE_MS).max(0) as u64;
        Self {
            epoch,
            next: seed.min(INCARNATION_COUNTER_MAX),
        }
    }

    /// The next incarnation, above every one this counter handed out. An
    /// epoch past the epoch bits keeps the largest epoch value, and a
    /// counter at its limit stays there: both are beyond any cell's life.
    fn take(&mut self) -> u64 {
        let epoch = self.epoch.min((1 << INCARNATION_EPOCH_BITS) - 1);
        let value = (epoch << INCARNATION_COUNTER_BITS) | self.next;
        self.next = (self.next + 1).min(INCARNATION_COUNTER_MAX);
        value
    }
}

/// What a facet delete removed, for change export: the facet's stream and
/// every stream below it with an incarnation at or below `through`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FacetDeleted {
    /// The deleted facet's stream (`engine_api::facet_cell`).
    pub stream: String,
    pub through: u64,
}

/// A resident root object's facets.
struct FacetRoot {
    epoch: u64,
    incarnations: Incarnations,
    spec: Option<celld_logic::RestoreSpec>,
    streams: BTreeSet<String>,
    /// The root began to stop. Its facets neither open nor delete until the
    /// stop fails before any facet stopped (`resume`): a facet stopped at
    /// this epoch would otherwise restart its lineage inside an epoch its
    /// eviction sealed, and an open that finished after the stop's snapshot
    /// would outlive the root.
    stopping: bool,
    /// Serializes the root's first opens: two activations of one stream
    /// would unlink the file the first one's connection writes.
    opening: Arc<tokio::sync::Mutex<()>>,
}

/// The facet streams of every resident root on this node.
#[derive(Clone, Default)]
pub(crate) struct FacetStreams(Arc<Mutex<HashMap<String, FacetRoot>>>);

impl FacetStreams {
    /// A root's activation: its facets activate later with `spec`.
    pub(crate) fn register(&self, root: &str, spec: &celld_logic::RestoreSpec) {
        self.0.lock().expect("facet streams poisoned").insert(
            root.to_string(),
            FacetRoot {
                epoch: spec.epoch,
                incarnations: Incarnations::new(spec.epoch, crate::asyncrt::wall_ms()),
                spec: Some(spec.clone()),
                streams: BTreeSet::new(),
                stopping: false,
                opening: Arc::default(),
            },
        );
    }

    /// Activate a facet's stream, once per root activation, and answer its
    /// database file, whether replication restored it from a replica, and
    /// the incarnation it takes if it has none. The incarnation is taken
    /// once the stream is in the root's set, under the same lock a delete
    /// takes, so a delete either removes the stream or precedes the
    /// incarnation. `db_path` is the engine's own placement of a cell's
    /// database.
    pub(crate) async fn open(
        &self,
        replication: Option<&Replication>,
        db_path: impl Fn(&str, u64) -> PathBuf,
        root: &str,
        epoch: u64,
        names: &[String],
    ) -> anyhow::Result<FacetFile> {
        let facet = crate::engine_api::facet_cell(root, names);
        let path = db_path(&facet, epoch);
        let resident = |roots: &mut HashMap<String, FacetRoot>| {
            roots
                .get_mut(root)
                .filter(|entry| entry.epoch == epoch && !entry.stopping)
                .map(|entry| {
                    (
                        entry
                            .streams
                            .contains(&facet)
                            .then(|| entry.incarnations.take()),
                        entry.spec.clone(),
                        entry.opening.clone(),
                    )
                })
                .ok_or_else(|| anyhow!("{root} epoch {epoch} is not resident"))
        };
        let open = |path: PathBuf, incarnation| FacetFile {
            path,
            restored: false,
            incarnation: Some(incarnation),
        };
        let opening = {
            let mut roots = self.0.lock().expect("facet streams poisoned");
            let (incarnation, _, opening) = resident(&mut roots)?;
            if let Some(incarnation) = incarnation {
                return Ok(open(path, incarnation));
            }
            opening
        };
        let _opening = opening.lock().await;
        let spec = {
            let mut roots = self.0.lock().expect("facet streams poisoned");
            let (incarnation, spec, _) = resident(&mut roots)?;
            if let Some(incarnation) = incarnation {
                return Ok(open(path, incarnation));
            }
            spec
        };
        let restored = match (replication, spec) {
            (Some(replication), Some(mut spec)) => {
                // A clean reload resumes the files it closed. A facet that
                // was not open then has none, so it restores as any facet
                // of this epoch does.
                if spec.resume_local && crate::asyncrt::fs().metadata(&path).is_err() {
                    spec.resume_local = false;
                }
                let (restored_path, restored, _) =
                    replication.restore(&facet, &spec, false).await?;
                anyhow::ensure!(
                    restored_path == path,
                    "replication restored {} instead of {}",
                    restored_path.display(),
                    path.display()
                );
                restored
            }
            (Some(_), None) => anyhow::bail!("{root} was activated without a restore"),
            (None, _) => {
                let parent = path.parent().context("facet database has no parent")?;
                crate::asyncrt::fs()
                    .create_dir_all(parent)
                    .with_context(|| format!("create facet directory {}", parent.display()))?;
                false
            }
        };
        let mut roots = self.0.lock().expect("facet streams poisoned");
        let incarnation = match roots
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch && !entry.stopping)
        {
            Some(entry) => {
                entry.streams.insert(facet);
                entry.incarnations.take()
            }
            // The root stopped while the stream activated.
            None => {
                drop(roots);
                if let Some(replication) = replication {
                    replication.ltx().discard(&facet, epoch);
                }
                anyhow::bail!("{root} epoch {epoch} stopped while its facet opened");
            }
        };
        Ok(FacetFile {
            path,
            restored,
            incarnation: Some(incarnation),
        })
    }

    /// Delete a facet's stream and every stream below it, resident or not,
    /// locally and in the bucket. `local` is the facet's local directory.
    /// Only the root's resident owner deletes: a delete that runs after the
    /// root moved would remove what the new owner writes. Answers the bound
    /// on the incarnations it removed (see the module docs).
    pub(crate) async fn delete(
        &self,
        replication: Option<&Replication>,
        local: impl Fn(&str) -> PathBuf,
        root: &str,
        epoch: u64,
        names: &[String],
    ) -> anyhow::Result<FacetDeleted> {
        let facet = crate::engine_api::facet_cell(root, names);
        let below = format!("{facet}/");
        let (doomed, through): (Vec<String>, u64) = {
            let mut roots = self.0.lock().expect("facet streams poisoned");
            let entry = roots
                .get_mut(root)
                .filter(|entry| entry.epoch == epoch && !entry.stopping)
                .ok_or_else(|| anyhow!("{root} epoch {epoch} is not resident"))?;
            let doomed: Vec<String> = entry
                .streams
                .iter()
                .filter(|stream| **stream == facet || stream.starts_with(&below))
                .cloned()
                .collect();
            for stream in &doomed {
                entry.streams.remove(stream);
            }
            (doomed, entry.incarnations.take())
        };
        match replication {
            Some(replication) => {
                for stream in &doomed {
                    replication.ltx().discard(stream, epoch);
                }
                replication.ltx().delete_streams(&facet).await?;
            }
            None => {
                let local = local(&facet);
                match crate::asyncrt::fs().remove_dir_all(&local) {
                    Err(error) if error.kind() != std::io::ErrorKind::NotFound => {
                        return Err(error)
                            .with_context(|| format!("remove facet {}", local.display()));
                    }
                    _ => {}
                }
            }
        }
        Ok(FacetDeleted {
            stream: facet,
            through,
        })
    }

    /// The root begins to stop: see `FacetRoot::stopping`. Answers the
    /// streams the stop must stop, after which no open can add one.
    pub(crate) fn stopping(&self, root: &str, epoch: u64) -> Vec<String> {
        let mut roots = self.0.lock().expect("facet streams poisoned");
        match roots.get_mut(root).filter(|entry| entry.epoch == epoch) {
            Some(entry) => {
                entry.stopping = true;
                entry.streams.iter().cloned().collect()
            }
            None => Vec::new(),
        }
    }

    /// Stop a root and its facets. The root's stream stops first, and only
    /// while it is resident: a retry after the root stopped goes on to the
    /// facets that remain. A root stop that fails or is abandoned leaves
    /// every facet resident and resumes them, so a root that restarts in
    /// place keeps its facets. The facets stop after the root, each
    /// forgotten once it stopped. Nothing moves the root's ownership before
    /// this returns, so every facet still stops before a new owner starts.
    ///
    /// Once the root stopped, a facet stop that fails is retried here until
    /// it succeeds instead of returning: the facet's handoff snapshot is what
    /// carries its acknowledged tail into the bucket, and a stop that failed
    /// would let the caller restart a root whose stream is gone. The actor's
    /// release loop has no overall timeout for the same reason.
    pub(crate) async fn stop_root<R, F, FF>(
        &self,
        root: &str,
        epoch: u64,
        root_resident: bool,
        stop_root: impl FnOnce() -> R,
        mut stop_facet: F,
    ) -> anyhow::Result<()>
    where
        R: std::future::Future<Output = anyhow::Result<()>>,
        F: FnMut(String) -> FF,
        FF: std::future::Future<Output = anyhow::Result<()>>,
    {
        self.stopping(root, epoch);
        if root_resident {
            if let Err(error) = stop_root().await {
                self.resume(root, epoch);
                return Err(error);
            }
        }
        for facet in self.stopping(root, epoch) {
            let mut delay = FACET_STOP_RETRY_FIRST;
            while let Err(error) = stop_facet(facet.clone()).await {
                tracing::warn!(
                    root,
                    epoch,
                    facet,
                    %error,
                    "a facet stop failed after its root stopped; retrying"
                );
                crate::asyncrt::sleep(delay).await;
                delay = (delay * 2).min(FACET_STOP_RETRY_MAX);
            }
            self.stopped(root, epoch, &facet);
        }
        self.forget(root, epoch);
        Ok(())
    }

    /// The root's stop failed before any facet stopped, and the root
    /// restarts in place: its facets open and delete again.
    pub(crate) fn resume(&self, root: &str, epoch: u64) {
        if let Some(entry) = self
            .0
            .lock()
            .expect("facet streams poisoned")
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch)
        {
            entry.stopping = false;
        }
    }

    /// The facet streams of a resident root.
    pub(crate) fn resident(&self, root: &str, epoch: u64) -> Vec<String> {
        self.0
            .lock()
            .expect("facet streams poisoned")
            .get(root)
            .filter(|entry| entry.epoch == epoch)
            .map(|entry| entry.streams.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// A facet stream that stopped. The root's entry stays until the root
    /// itself stops, so a stop that fails and restarts the root in place
    /// can still open its facets.
    pub(crate) fn stopped(&self, root: &str, epoch: u64, stream: &str) {
        if let Some(entry) = self
            .0
            .lock()
            .expect("facet streams poisoned")
            .get_mut(root)
            .filter(|entry| entry.epoch == epoch)
        {
            entry.streams.remove(stream);
        }
    }

    /// The root stopped: its entry goes.
    pub(crate) fn forget(&self, root: &str, epoch: u64) {
        let mut roots = self.0.lock().expect("facet streams poisoned");
        if roots.get(root).is_some_and(|entry| entry.epoch == epoch) {
            roots.remove(root);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR_MS: i64 = 3_600_000;
    const NOW_MS: i64 = INCARNATION_CLOCK_BASE_MS + 1000 * HOUR_MS;

    fn spec(epoch: u64) -> celld_logic::RestoreSpec {
        celld_logic::RestoreSpec {
            epoch,
            fresh: false,
            took_over: false,
            resume_local: false,
            prior: None,
        }
    }

    fn names(path: &[&str]) -> Vec<String> {
        path.iter().map(|name| name.to_string()).collect()
    }

    #[test]
    fn incarnations_grow_within_an_epoch_and_across_epochs() {
        let mut first = Incarnations::new(3, NOW_MS);
        let a = first.take();
        let b = first.take();
        assert!(a < b);
        assert_eq!(a >> INCARNATION_COUNTER_BITS, 3);
        // A later epoch is above, even on a node whose clock is behind.
        let mut later = Incarnations::new(4, NOW_MS - 100 * HOUR_MS);
        assert!(later.take() > b);
        // A root that restarts in place at the same epoch a second later
        // counts on from the clock, above what it handed out before.
        let mut again = Incarnations::new(3, NOW_MS + 1000);
        assert!(again.take() > b);
    }

    #[test]
    fn incarnations_saturate_instead_of_wrapping() {
        let mut past = Incarnations::new(1 << 30, i64::MAX);
        let a = past.take();
        let b = past.take();
        assert_eq!(a, u64::MAX);
        assert_eq!(b, u64::MAX);
        let mut early = Incarnations::new(1, 0);
        assert_eq!(early.take(), 1 << INCARNATION_COUNTER_BITS);
    }

    /// Runs on a runtime that lives for the process, installed as the
    /// host's: the process domain binds to the first runtime it sees, which
    /// a `#[tokio::test]` would not outlive.
    fn run(future: impl std::future::Future<Output = ()>) {
        static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
        let runtime = RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap()
        });
        crate::asyncrt::set_host_handle(runtime.handle().clone());
        runtime.block_on(future);
    }

    #[test]
    fn a_delete_bounds_every_incarnation_before_it_and_none_after() {
        run(a_delete_bounds_every_incarnation());
    }

    async fn a_delete_bounds_every_incarnation() {
        let dir = tempfile::tempdir().unwrap();
        let base = dir.path().to_path_buf();
        let db_path = |cell: &str, epoch: u64| base.join(cell).join(format!("e{epoch}/db"));
        let local = |cell: &str| base.join(cell);
        let facets = FacetStreams::default();
        facets.register("Room:1", &spec(2));

        let open = |path: &'static [&'static str]| {
            let facets = facets.clone();
            async move { facets.open(None, db_path, "Room:1", 2, &names(path)).await }
        };
        let parent = open(&["a"]).await.unwrap().incarnation.unwrap();
        let child = open(&["a", "b"]).await.unwrap().incarnation.unwrap();
        let sibling = open(&["z"]).await.unwrap().incarnation.unwrap();

        let deleted = facets
            .delete(None, local, "Room:1", 2, &names(&["a"]))
            .await
            .unwrap();
        assert_eq!(
            deleted.stream,
            crate::engine_api::facet_cell("Room:1", &names(&["a"]))
        );
        assert!(parent <= deleted.through);
        assert!(child <= deleted.through);
        // A stream outside the path is below the bound too: the path, not
        // the bound, keeps it.
        assert!(sibling <= deleted.through);
        assert_eq!(
            facets.resident("Room:1", 2),
            vec![crate::engine_api::facet_cell("Room:1", &names(&["z"]))]
        );

        // Recreated, at the path and below it: both above the bound.
        let parent_again = open(&["a"]).await.unwrap().incarnation.unwrap();
        let child_again = open(&["a", "b"]).await.unwrap().incarnation.unwrap();
        assert!(parent_again > deleted.through);
        assert!(child_again > deleted.through);

        // The next activation's delete bounds everything the earlier one
        // handed out.
        facets.forget("Room:1", 2);
        facets.register("Room:1", &spec(3));
        let next = facets
            .delete(None, local, "Room:1", 3, &names(&["a"]))
            .await
            .unwrap();
        assert!(child_again < next.through);
    }
}
