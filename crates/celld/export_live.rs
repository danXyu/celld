// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The live change-export path (`docs/design/change-export.md`,
//! "Architecture"): capture on the cell thread, attribution and gated
//! release per cell, the bucket sink, and delivered positions per sink.
//!
//! One [`Exporter`] serves the node. For every resident cell of an exported
//! class it runs one stream task that owns the cell's
//! [`Attribution`] and reads one FIFO. Three producers write that FIFO:
//!
//! - the cell thread, at each safe point: the pulled commit stamped with its
//!   WAL point and the cell's committed-write position, then "caught up";
//! - the LTX capture observer, for every file the capture loop writes;
//! - the actor, with the verdict on the stream's export ticket and the
//!   `max(durable_txid, shipped_txid)` it read when the ticket settled.
//!
//! Attribution relies on the first two reaching it in the order they
//! happened, which one channel per cell gives.
//!
//! A pulled commit makes the stream ask the output gate for an export
//! ticket at the commit's position ([`celld_logic::Event::ExportTicket`]),
//! one ticket at a time. The ticket gets the gate's authority check, fence
//! and ownership read. Once it settles, the commits whose label the proof
//! covers are released in commit order, turned into records, and submitted
//! to the sink. A failed ticket is retried; a fenced node stops.
//!
//! The sink reports one result per record in submission order. The
//! delivery task derives each stream's **delivered position** from those
//! results: the position of the newest commit whose records are all
//! acknowledged, frozen for the rest of the residency by the first drop,
//! which it reports with a `gap`. After each batch of results it submits a
//! `watermark` per stream whose delivered position moved, carrying the
//! commits and records it certifies.
//!
//! A facet delete (`crate::facet_streams`) runs on the host loop, not the
//! root's cell thread, and can fail after the op returned, so only a delete
//! that succeeded reaches the root's FIFO ([`Exporter::facet_deleted`]).
//! The stream gives it a position after every commit that arrived before
//! it and releases it once a ticket asked after it settles, so the
//! `deleted` record passes the same authority check as the root's commits
//! and no commit that arrived after it goes out first ([`Sequencer`]).
//!
//! **Facets.** A facet of an exported root is exported on a stream of its
//! own: its database, connection, and LTX stream are its own, so its commits
//! are captured, attributed against its own captures, and released like a
//! root's. The stream is keyed by the facet's stream name
//! (`engine_api::facet_cell`) and names the root's class and cell, the
//! [`facet_path`] below the root, and the incarnation `crate::facet_streams`
//! stamped in the facet's `_cf_METADATA`. A facet has no gate residency of
//! its own, so its ticket is relayed ([`Exporter::relay`]): the facet's
//! stream is proven durable first, then the root's ticket, at position 0,
//! checks the node's authority and the root's residency the same way a
//! root's ticket does, and the facet's stream releases up to the TXID its
//! own proof covered.
//!
//! Scope of this first wiring:
//!
//! - the shared queue budget is applied to the pending lists. The sink's
//!   buffer counts against it but is bounded by its own early flush, so the
//!   pending commits are what is shed;
//! - commits pulled after the residency's last ticket can no longer be
//!   proven (the cell is leaving) are not released; the next activation's
//!   link or the reconciler reports that tail.
//!
//! **Links.** Each residency's stream starts with a `link` record naming the
//! state the activation restored ([`ActivationLink`]): positions are per
//! epoch, so the link is what joins an epoch to its predecessor, and a
//! predecessor position beyond what the consumer certified is a gap. The
//! link is submitted when the cell's connection attaches, before the cell
//! serves and ahead of every commit of the residency, at `(epoch,
//! start_txid, 0)`: after every position of an earlier residency of the same
//! epoch and before every commit of this one. It is not gated; it states
//! where the residency starts, not a write.
//!
//! **Incarnation.** A root cell's incarnation is the epoch its stream began
//! in, stored in the cell's `_cf_METADATA` row at the first exported open so
//! it moves and restores with the cell and survives `deleteAll`. A cell
//! created before export was turned on takes the epoch export first saw.

use crate::bucket::Bucket;
use crate::export::Config;
use crate::export_sink::{
    BucketSink, BucketSinkConfig, Delivery as SinkDelivery, ExportSink, Outcome, Retention,
    SinkRecord,
};
use crate::facet_streams::FacetDeleted;
use crate::replication::{ActivationLink, ActivationMode};
use crate::storage::export_capture::{CapturedCommit, WalStamp};
use celld_export_format::{
    split, Body, BulkBody, DeletedBody, Envelope, GapBody, LinkBody, LinkMode, Origin, Position,
    Record, RecoveredBody, RowsBody, SchemaBody, SnapshotBody, SnapshotEndBody, SnapshotScope,
    Split, StreamId, TableGen, WatermarkBody,
};
use celld_logic::export::{Attribution, Capture, CapturedWal, Released, WalGeneration, WalPoint};
use celld_logic::RequestError;
use rusqlite::OptionalExtension as _;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;

/// First wait before a failed export ticket is asked again, doubled up to
/// [`TICKET_RETRY_MAX`].
const TICKET_RETRY: Duration = Duration::from_millis(250);
const TICKET_RETRY_MAX: Duration = Duration::from_secs(10);

static EXPORTER: OnceLock<Arc<Exporter>> = OnceLock::new();

/// The node's exporter, when `CELLD_EXPORT=1`.
pub fn installed() -> Option<&'static Arc<Exporter>> {
    EXPORTER.get()
}

/// What the stream asks of the output gate. The actor feeds it to the core
/// as [`celld_logic::Event::ExportTicket`]. A facet's stream asks in its
/// root's name (see [`Exporter::relay`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TicketAsk {
    pub cell: String,
    pub epoch: u64,
    pub position: u64,
    pub ticket: u64,
}

/// One input on a stream's FIFO.
enum Input {
    /// Who the stream is, sent when the cell's connection attaches.
    Identity(Identity),
    /// The cell's `idFromName` name, once it is known.
    Named(String),
    /// A pulled commit and the committed-write position after it. `bytes`
    /// is already reserved against the queue budget.
    Commit {
        stamp: WalStamp,
        position: u64,
        bytes: u64,
        commit: CapturedCommit,
    },
    /// A pulled commit the cell thread shed because the queue was over
    /// budget. It keeps its place in commit order as a gap.
    Shed {
        stamp: WalStamp,
        position: u64,
    },
    /// A commit that netted to no exported row, with its WAL point and the
    /// committed-write position after it. Attributed like any commit, so
    /// the released position (and with it the delivered one) passes its
    /// TXID.
    Wrote {
        stamp: WalStamp,
        position: u64,
    },
    CaughtUp,
    Captured(Capture),
    /// The verdict on a ticket, with the proven TXID read when it settled.
    Proven {
        ticket: u64,
        result: Result<u64, RequestError>,
    },
    /// A facet delete that succeeded on the host loop.
    FacetDeleted {
        body: DeletedBody,
        at_ms: i64,
    },
    /// The stream's facet was deleted: nothing it holds goes out.
    Ended,
}

/// Tickets at or above this bit are relayed for a facet's stream; a stream's
/// own tickets count up from 1.
const RELAY_TICKET: u64 = 1 << 63;

/// The root cell of a stream name, and the facet path below it for a facet.
pub(crate) fn stream_root(cell: &str) -> (&str, Option<&str>) {
    match cell.find("/facets/") {
        Some(at) => {
            let root = &cell[..at];
            (root, facet_path(root, cell))
        }
        None => (cell, None),
    }
}

/// The class of a stream name: its root's, which a facet follows.
fn stream_class(cell: &str) -> &str {
    let (root, _) = stream_root(cell);
    root.split_once(':').map_or(root, |(class, _)| class)
}

/// A facet ticket the root's gate is settling.
struct Relay {
    /// The stream task that asked, which may have been replaced under its
    /// key since (a facet deleted and recreated).
    stream: mpsc::WeakUnboundedSender<Input>,
    ticket: u64,
    /// What the facet's own proof covered.
    txid: u64,
}

/// The `facet` of a facet stream's records and of the `deleted` records
/// naming it: the facet's stream name below its root
/// (`facets/<h>[/facets/<h>...]`, see `engine_api::facet_cell`). Hashed
/// names never contain `/`, so a path's subtree is exactly the paths that
/// extend it by `/`.
pub(crate) fn facet_path<'a>(root: &str, stream: &'a str) -> Option<&'a str> {
    stream
        .strip_prefix(root)?
        .strip_prefix('/')
        .filter(|path| !path.is_empty())
}

/// The stream identity the cell thread reads from the cell itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Identity {
    /// The script of the cell's deployment.
    pub script: String,
    pub incarnation: u64,
    /// `_cf_METADATA.actor_name`, when the cell has one.
    pub cell_name: Option<String>,
}

/// The cell thread's handle on its stream.
#[derive(Clone)]
pub(crate) struct CellStream {
    exporter: Arc<Exporter>,
    tx: mpsc::UnboundedSender<Input>,
    denied: HashSet<String>,
}

impl CellStream {
    /// Tables of this cell's class the operator denied.
    pub(crate) fn denied_tables(&self) -> HashSet<String> {
        self.denied.clone()
    }

    /// Queue a pulled commit, or, when the shared budget cannot hold its
    /// rows, a gap in its place. The budget is charged here, before the
    /// rows wait on the FIFO, so a stream that falls behind cannot hold
    /// more than the budget.
    pub(crate) fn commit(&self, stamp: WalStamp, position: u64, commit: CapturedCommit) {
        let bytes = encoded_bytes(&commit);
        let input = if self.exporter.reserve(bytes) {
            Input::Commit {
                stamp,
                position,
                bytes,
                commit,
            }
        } else {
            self.exporter
                .counters
                .dropped_records
                .fetch_add(1, Ordering::Relaxed);
            Input::Shed { stamp, position }
        };
        if let Err(mpsc::error::SendError(Input::Commit { bytes, .. })) = self.tx.send(input) {
            self.exporter.release_queued(bytes);
        }
    }

    pub(crate) fn caught_up(&self) {
        let _ = self.tx.send(Input::CaughtUp);
    }

    pub(crate) fn wrote(&self, stamp: WalStamp, position: u64) {
        let _ = self.tx.send(Input::Wrote { stamp, position });
    }

    /// Name the stream. Sent once, before the capture session installs, so
    /// the link precedes every commit.
    pub(crate) fn identify(&self, identity: Identity) {
        let _ = self.tx.send(Input::Identity(identity));
    }

    /// The cell's name, persisted after it attached.
    pub(crate) fn named(&self, name: &str) {
        let _ = self.tx.send(Input::Named(name.to_string()));
    }
}

/// Counters for `/state`.
#[derive(Default)]
struct Counters {
    /// Commits on a stream's FIFO, charged when the cell thread queues them.
    queued_bytes: AtomicU64,
    /// Commits waiting in a stream's attribution for a proof.
    pending_bytes: AtomicU64,
    pending_commits: AtomicU64,
    dropped_records: AtomicU64,
    gaps: AtomicU64,
    bulk_commits: AtomicU64,
    attribution_mismatches: AtomicU64,
    delivered_records: AtomicU64,
    watermarks: AtomicU64,
}

/// What the delivery task needs about one submitted record.
struct Submitted {
    key: (String, u64),
    stream: StreamId,
    cell_name: Option<String>,
    position: Position,
    /// The last fragment of a record: the record counts once when it lands.
    whole: bool,
    /// The last record of a commit (or a gap): the delivered position may
    /// move to it.
    closes: bool,
    watermark: bool,
    /// A `recovered` record for another node's residency: it moves no
    /// delivered position here, and a drop freezes nothing.
    recovered: bool,
}

/// What one dead-node recovery folded into the bucket, for its `recovered`
/// records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Recovery {
    /// The dead session, `<node>/<generation>`.
    pub session: String,
    /// Recovery declared a bounded loss for the session.
    pub loss: bool,
    /// Per visited cell epoch, the TXID the bucket holds it through.
    pub cells: Vec<RecoveredCell>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredCell {
    pub cell: String,
    pub epoch: u64,
    pub through: u64,
}

pub struct Exporter {
    node: String,
    config: Config,
    sink: Arc<dyn ExportSink>,
    streams: Mutex<HashMap<(String, u64), mpsc::WeakUnboundedSender<Input>>>,
    ask: Box<dyn Fn(TicketAsk) + Send + Sync>,
    next_seq: AtomicU64,
    /// Records in flight, by sequence. Ordered, so an advance can tell
    /// whether anything submitted before it is still outstanding.
    submitted: Mutex<BTreeMap<u64, Submitted>>,
    /// Released positions waiting for the records submitted before them,
    /// by the sequence they were given. See [`Stream::advance`].
    advances: Mutex<BTreeMap<u64, Submitted>>,
    /// Wakes the delivery task when an advance is registered.
    wake: mpsc::UnboundedSender<Outcome>,
    /// Set while the delivery task handles a batch of results, which can
    /// submit watermarks and gaps of its own.
    delivering: AtomicBool,
    delivery_failed: AtomicBool,
    counters: Counters,
    /// Proves facet streams durable; installed by the cell runtime.
    replication: OnceLock<crate::ltx_replication::Replication>,
    /// Facet tickets waiting on their root's gate, by relayed ticket.
    relays: Mutex<HashMap<u64, Relay>>,
    next_relay: AtomicU64,
    /// Every stream's tickets, below [`RELAY_TICKET`].
    next_ticket: AtomicU64,
}

impl Exporter {
    /// Start the configured sink and the delivery task, and install the
    /// exporter for the node. `ask` hands a ticket to the actor. Must run
    /// inside the node's runtime, before any cell activates.
    pub fn start(
        config: Config,
        bucket: Bucket,
        node: String,
        ask: impl Fn(TicketAsk) + Send + Sync + 'static,
    ) -> anyhow::Result<Arc<Exporter>> {
        // Delivered positions are tracked for one sink; running several at
        // once needs a position per sink.
        anyhow::ensure!(
            config.sinks.count() == 1,
            "CELLD_EXPORT_SINK names several sinks, which is not supported yet; choose one"
        );
        let (outcomes_tx, outcomes_rx) = mpsc::unbounded_channel();
        let wake = outcomes_tx.clone();
        let sink: Arc<dyn ExportSink> = if config.sinks.blob_stream {
            crate::export_blob_stream::start(&config, outcomes_tx)?
        } else if config.sinks.kafka {
            crate::export_kafka::start(&config, outcomes_tx)?
        } else {
            Arc::new(BucketSink::start(
                bucket,
                node.clone(),
                BucketSinkConfig {
                    flush: config.flush,
                    flush_bytes: config.flush_bytes as u64,
                    retention: match config.retention {
                        crate::telemetry::Retention::None => Retention::None,
                        crate::telemetry::Retention::Days(days) => Retention::Days(days),
                    },
                    ..BucketSinkConfig::default()
                },
                outcomes_tx,
            ))
        };
        let exporter = Exporter::new(config, node, sink, Box::new(ask), wake);
        EXPORTER
            .set(exporter.clone())
            .map_err(|_| anyhow::anyhow!("the change exporter is already installed"))?;
        crate::asyncrt::spawn(deliver(exporter.clone(), outcomes_rx));
        Ok(exporter)
    }

    fn new(
        config: Config,
        node: String,
        sink: Arc<dyn ExportSink>,
        ask: Box<dyn Fn(TicketAsk) + Send + Sync>,
        wake: mpsc::UnboundedSender<Outcome>,
    ) -> Arc<Exporter> {
        Arc::new(Exporter {
            node,
            config,
            sink,
            streams: Mutex::new(HashMap::new()),
            ask,
            next_seq: AtomicU64::new(1),
            submitted: Mutex::new(BTreeMap::new()),
            advances: Mutex::new(BTreeMap::new()),
            wake,
            delivering: AtomicBool::new(false),
            delivery_failed: AtomicBool::new(false),
            counters: Counters::default(),
            replication: OnceLock::new(),
            relays: Mutex::new(HashMap::new()),
            next_relay: AtomicU64::new(RELAY_TICKET),
            next_ticket: AtomicU64::new(1),
        })
    }

    /// Capture settings for every isolate on the node.
    pub(crate) fn capture_settings(&self) -> crate::storage::export_capture::Settings {
        crate::storage::export_capture::Settings {
            max_tx_bytes: self.config.max_tx_bytes as u64,
        }
    }

    /// Whether `cell` is exported: a root cell of an exported class, or a
    /// facet of one.
    pub(crate) fn exports(&self, cell: &str) -> bool {
        self.config.exports_class(stream_class(cell))
    }

    /// The node's replication, which proves facet streams. The first cell
    /// runtime installs it; every runtime of the node shares one.
    pub(crate) fn set_replication(&self, replication: crate::ltx_replication::Replication) {
        let _ = self.replication.set(replication);
    }

    /// Open the stream of one residency when its replica opens, and return
    /// the capture observer that feeds it. `None` when the cell is not
    /// exported. `link` is where the activation's state came from; the
    /// stream submits it once the cell attaches.
    pub(crate) fn open_stream(
        self: &Arc<Self>,
        cell: &str,
        epoch: u64,
        link: ActivationLink,
    ) -> Option<impl FnMut(&celld_ltx::CapturedFile) + Send + 'static> {
        if !self.exports(cell) {
            return None;
        }
        let key = (cell.to_string(), epoch);
        let tx = {
            let mut streams = self.streams.lock().unwrap_or_else(|e| e.into_inner());
            match streams
                .get(&key)
                .and_then(mpsc::WeakUnboundedSender::upgrade)
            {
                Some(tx) => tx,
                None => {
                    let (tx, rx) = mpsc::unbounded_channel();
                    streams.insert(key.clone(), tx.downgrade());
                    let stream = Stream::new(self.clone(), key, link, tx.downgrade());
                    crate::asyncrt::spawn(stream.run(rx));
                    tx
                }
            }
        };
        Some(move |file: &celld_ltx::CapturedFile| {
            let _ = tx.send(Input::Captured(capture_of(file)));
        })
    }

    /// The cell thread's handle on the stream of `scope` at `epoch`, when
    /// its replica opened one. The caller names it with
    /// [`CellStream::identify`].
    pub(crate) fn attach(self: &Arc<Self>, scope: &str, epoch: u64) -> Option<CellStream> {
        let tx = self
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(scope.to_string(), epoch))
            .and_then(mpsc::WeakUnboundedSender::upgrade)?;
        let class = stream_class(scope);
        let denied = self
            .config
            .denied_tables
            .iter()
            .filter(|(denied_class, _)| denied_class == class)
            .map(|(_, table)| table.clone())
            .collect();
        Some(CellStream {
            exporter: self.clone(),
            tx,
            denied,
        })
    }

    /// The actor's verdict on a ticket. A relayed ticket's verdict goes to
    /// its facet's stream, with the TXID the facet's proof covered.
    pub(crate) fn proven(
        &self,
        cell: &str,
        epoch: u64,
        ticket: u64,
        result: Result<u64, RequestError>,
    ) {
        if ticket >= RELAY_TICKET {
            let relay = self
                .relays
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&ticket);
            let route = relay.and_then(|relay| Some((relay.stream.upgrade()?, relay)));
            if let Some((tx, relay)) = route {
                let _ = tx.send(Input::Proven {
                    ticket: relay.ticket,
                    result: result.map(|_| relay.txid),
                });
            }
            return;
        }
        let tx = self
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(cell.to_string(), epoch))
            .and_then(mpsc::WeakUnboundedSender::upgrade);
        if let Some(tx) = tx {
            let _ = tx.send(Input::Proven { ticket, result });
        }
    }

    /// Ask for a facet stream's ticket. The facet's stream is proven durable
    /// and its TXID read first, as the actor reads a root's once its proof
    /// lands; then the root's gate takes a ticket at position 0, which
    /// checks the node's authority and the root's residency at `epoch` (a
    /// facet has neither ownership nor a fence of its own) after that proof.
    /// A failed proof fails the ticket, which the stream retries.
    fn relay(self: &Arc<Self>, ask: TicketAsk, stream: mpsc::WeakUnboundedSender<Input>) {
        let exporter = self.clone();
        crate::asyncrt::spawn(async move {
            let TicketAsk {
                cell: facet,
                epoch,
                ticket,
                ..
            } = ask;
            let (root, _) = stream_root(&facet);
            let root = root.to_string();
            let proof =
                match exporter.replication.get() {
                    Some(replication) => replication
                        .await_durable(&facet, epoch, 0)
                        .await
                        .and_then(|_| {
                            replication
                                .export_proven_txid(&facet, epoch)
                                .ok_or_else(|| anyhow::anyhow!("the facet's stream is gone"))
                        }),
                    // No replication proves nothing, as for a root cell.
                    None => Ok(0),
                };
            let txid = match proof {
                Ok(txid) => txid,
                Err(error) => {
                    tracing::debug!(%facet, epoch, %error, "export: facet proof failed; retrying");
                    if let Some(tx) = stream.upgrade() {
                        let _ = tx.send(Input::Proven {
                            ticket,
                            result: Err(RequestError::DurabilityUnproven),
                        });
                    }
                    return;
                }
            };
            let relayed = exporter.next_relay.fetch_add(1, Ordering::Relaxed);
            exporter
                .relays
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(
                    relayed,
                    Relay {
                        stream,
                        ticket,
                        txid,
                    },
                );
            (exporter.ask)(TicketAsk {
                cell: root,
                epoch,
                position: 0,
                ticket: relayed,
            });
        });
    }

    /// A facet delete that succeeded, for the root's stream at `epoch`.
    /// Nothing is recorded when the root is not exported or its stream is
    /// gone; the reconciler reports a consumer stream with no bucket prefix.
    pub(crate) fn facet_deleted(&self, root: &str, epoch: u64, deleted: &FacetDeleted) {
        let Some(path) = facet_path(root, &deleted.stream) else {
            tracing::warn!(root, stream = %deleted.stream, "export: a facet delete names no facet of its root");
            return;
        };
        let tx = {
            let mut streams = self.streams.lock().unwrap_or_else(|e| e.into_inner());
            // The deleted facets' streams end with their residencies; a facet
            // recreated in this epoch opens a stream of its own.
            // Each is told to stop, since a ticket in flight keeps its task
            // alive and its pending commits counted against the budget.
            let below = format!("{}/", deleted.stream);
            streams.retain(|(cell, at), tx| {
                let gone = *at == epoch && (*cell == deleted.stream || cell.starts_with(&below));
                if gone {
                    if let Some(tx) = tx.upgrade() {
                        let _ = tx.send(Input::Ended);
                    }
                }
                !gone
            });
            streams
                .get(&(root.to_string(), epoch))
                .and_then(mpsc::WeakUnboundedSender::upgrade)
        };
        let Some(tx) = tx else {
            return;
        };
        let _ = tx.send(Input::FacetDeleted {
            body: DeletedBody {
                facet: Some(path.to_string()),
                incarnation: None,
                subtree: true,
                through_incarnation: Some(deleted.through),
            },
            at_ms: crate::asyncrt::wall_ms(),
        });
    }

    /// Emit a `recovered` record for every exported cell epoch `recovery`
    /// folded. Dead-node recovery calls this once its uploads are in the
    /// bucket, before it seals the log, so a recoverer that dies first
    /// leaves the emission to the node that takes the recovery over.
    pub(crate) fn recovered(&self, recovery: &Recovery) {
        let records = recovered_records(&self.node, recovery, |cell| self.exports(cell));
        if records.is_empty() {
            return;
        }
        tracing::info!(session = %recovery.session, cells = records.len(), loss = recovery.loss, "export: recovered records");
        self.submit(
            records
                .into_iter()
                .map(|record| {
                    let meta = Submitted {
                        key: (
                            record.envelope.stream.cell.clone(),
                            record.envelope.position.epoch,
                        ),
                        stream: record.envelope.stream.clone(),
                        cell_name: None,
                        position: record.envelope.position,
                        whole: true,
                        closes: false,
                        watermark: false,
                        recovered: true,
                    };
                    (record, meta)
                })
                .collect(),
        );
    }

    /// Charge `bytes` to the shared budget for a commit going onto a
    /// FIFO. `false` when the budget cannot hold it.
    fn reserve(&self, bytes: u64) -> bool {
        let budget = self.config.queue_bytes as u64;
        let held = self.counters.pending_bytes.load(Ordering::Relaxed) + self.sink.buffered_bytes();
        self.counters
            .queued_bytes
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |queued| {
                (queued + held + bytes <= budget).then_some(queued + bytes)
            })
            .is_ok()
    }

    fn release_queued(&self, bytes: u64) {
        self.counters
            .queued_bytes
            .fetch_sub(bytes, Ordering::Relaxed);
    }

    /// Write what the sink holds and the watermarks its acknowledgements
    /// earn, then close the sink. Called at shutdown, under its deadline.
    pub async fn close(&self) {
        // Each flush's results can make the delivery task submit
        // watermarks, which need a flush of their own; stop once a flush
        // leaves nothing submitted and nothing being delivered.
        loop {
            if self.delivery_failed.load(Ordering::SeqCst) {
                break;
            }
            self.sink.flush();
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
            let unresolved = !self
                .submitted
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty()
                || !self
                    .advances
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .is_empty();
            if !unresolved && !self.delivering.load(Ordering::SeqCst) {
                break;
            }
        }
        self.sink.close().await;
    }

    /// The `export` object of `/state`.
    pub fn state(&self) -> serde_json::Value {
        let c = &self.counters;
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        serde_json::json!({
            "queue_bytes": load(&c.queued_bytes)
                + load(&c.pending_bytes)
                + self.sink.buffered_bytes(),
            "pending_commits": load(&c.pending_commits),
            "dropped_records": load(&c.dropped_records),
            "gaps": load(&c.gaps),
            "bulk_commits": load(&c.bulk_commits),
            "attribution_mismatches": load(&c.attribution_mismatches),
            "delivered_records": load(&c.delivered_records),
            "watermarks": load(&c.watermarks),
            "delivery_failed": self.delivery_failed.load(Ordering::SeqCst),
        })
    }

    /// Submit records in order, registering what delivery needs first.
    fn submit(&self, records: Vec<(Record, Submitted)>) {
        if records.is_empty() || self.delivery_failed.load(Ordering::SeqCst) {
            return;
        }
        let mut batch = Vec::with_capacity(records.len());
        {
            let mut submitted = self.submitted.lock().unwrap_or_else(|e| e.into_inner());
            if self.delivery_failed.load(Ordering::SeqCst) {
                return;
            }
            for (record, meta) in records {
                let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
                submitted.insert(seq, meta);
                batch.push(SinkRecord { seq, record });
            }
        }
        let seqs: Vec<u64> = batch.iter().map(|r| r.seq).collect();
        if self.sink.submit(batch).is_err() {
            // The sink is closing: nothing more is delivered this process.
            let mut submitted = self.submitted.lock().unwrap_or_else(|e| e.into_inner());
            for seq in seqs {
                submitted.remove(&seq);
            }
        }
    }

    /// Let the stream's delivered position reach `meta.position` once every
    /// record submitted before now is acknowledged. It carries no record.
    fn advance(&self, meta: Submitted) {
        if self.delivery_failed.load(Ordering::SeqCst) {
            return;
        }
        {
            let _submitted = self.submitted.lock().unwrap_or_else(|e| e.into_inner());
            if self.delivery_failed.load(Ordering::SeqCst) {
                return;
            }
            let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
            self.advances
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(seq, meta);
        }
        let _ = self.wake.send(Outcome {
            sink: "advance",
            results: Vec::new(),
        });
    }

    fn envelope(
        &self,
        stream: &StreamId,
        cell_name: &Option<String>,
        position: Position,
        committed_at: i64,
    ) -> Envelope {
        Envelope {
            stream: stream.clone(),
            cell_name: cell_name.clone(),
            position,
            committed_at,
            node: self.node.clone(),
            origin: Origin::Live,
            fragment: 1,
            fragments: 1,
        }
    }
}

/// The `recovered` records of `recovery`, one per cell epoch `exports`
/// accepts. Recovery knows neither a cell's script nor its first epoch, so
/// the stream names neither (see [`RecoveredBody`]).
fn recovered_records(
    node: &str,
    recovery: &Recovery,
    exports: impl Fn(&str) -> bool,
) -> Vec<Record> {
    let cells: Vec<&RecoveredCell> = recovery
        .cells
        .iter()
        .filter(|cell| exports(&cell.cell))
        .collect();
    let count = cells.len() as u64;
    let now = crate::asyncrt::wall_ms();
    cells
        .into_iter()
        .map(|cell| {
            let (root, facet) = stream_root(&cell.cell);
            let class = stream_class(&cell.cell);
            let head = Position::new(cell.epoch, cell.through, u64::MAX);
            Record {
                envelope: Envelope {
                    stream: StreamId {
                        script: String::new(),
                        class: class.to_string(),
                        cell: root.to_string(),
                        facet: facet.map(str::to_string),
                        incarnation: 0,
                    },
                    cell_name: None,
                    position: head,
                    committed_at: now,
                    node: node.to_string(),
                    origin: Origin::Live,
                    fragment: 1,
                    fragments: 1,
                },
                body: Body::Recovered(RecoveredBody {
                    session: recovery.session.clone(),
                    head,
                    loss: recovery.loss,
                    cells: count,
                }),
            }
        })
        .collect()
}

fn capture_of(file: &celld_ltx::CapturedFile) -> Capture {
    Capture {
        txid: file.txid.0,
        page_size: file.page_size,
        full_image: file.full_image,
        wal: file.wal.map(|wal| CapturedWal {
            generation: WalGeneration {
                salt1: wal.salt1,
                salt2: wal.salt2,
            },
            offset: u64::try_from(wal.offset).unwrap_or(0),
            size: u64::try_from(wal.size).unwrap_or(0),
        }),
    }
}

/// About the encoded size of a commit's rows, snapshots and schema records,
/// for the queue budget. An estimate from the values, so the cell thread
/// does not encode twice.
fn encoded_bytes(commit: &CapturedCommit) -> u64 {
    fn value(value: &celld_export_format::Value) -> usize {
        use celld_export_format::Value;
        match value {
            Value::Null => 4,
            Value::Integer(_) | Value::Real(_) => 24,
            Value::Text(text) => text.len() + 2,
            Value::Blob(blob) => blob.len().div_ceil(3) * 4 + 12,
        }
    }
    let snapshots = commit.snapshot.iter().flat_map(|s| s.tables.iter());
    let tables: usize = commit
        .tables
        .iter()
        .chain(snapshots)
        .map(|table| {
            let names: usize = table.columns.iter().map(|c| c.len() + 3).sum::<usize>()
                + table.key_columns.iter().map(|c| c.len() + 3).sum::<usize>();
            let rows: usize = table
                .rows
                .iter()
                .map(|row| 12 + row.key().iter().chain(row.row()).map(value).sum::<usize>())
                .sum();
            table.table.len() + names + rows + 64
        })
        .sum();
    let schemas: usize = commit
        .schemas
        .iter()
        .map(|schema| {
            let columns: usize = schema
                .columns
                .iter()
                .map(|c| c.name.len() + c.decl_type.len() + 48)
                .sum();
            schema.table.len() + schema.sql.len() + columns + 96
        })
        .sum();
    (tables + schemas + 64 * commit.bulk.len() + 256) as u64
}

/// The attribution and release of one cell residency.
struct Stream {
    exporter: Arc<Exporter>,
    key: (String, u64),
    identity: StreamId,
    cell_name: Option<String>,
    /// Submitted when the cell attaches, then `None`.
    link: Option<ActivationLink>,
    /// `None` stands for a commit with no exported row.
    attribution: Attribution<Option<CapturedCommit>>,
    /// Handed out as `commit` in positions, in release order.
    next_commit: u64,
    /// The TXID of the newest released position, so a gap never sorts
    /// before a commit released ahead of it (or before an advance).
    last_txid: u64,
    /// The newest position this stream has submitted or advanced to.
    delivered: Position,
    next_ticket: u64,
    /// The ticket in flight and the position it asked for.
    outstanding: Option<(u64, u64)>,
    /// The newest committed-write position no ticket has asked for yet.
    wanted: Option<u64>,
    retry: Duration,
    retry_at: Option<u64>,
    /// Keeps the channel open while a ticket is in flight, so the verdict
    /// still finds the stream after the cell's producers are gone.
    keepalive: Option<mpsc::UnboundedSender<Input>>,
    this: mpsc::WeakUnboundedSender<Input>,
    counted_bytes: u64,
    counted_commits: u64,
    /// The newest committed-write position a commit carried, which a
    /// ticket for a facet delete asks for.
    last_position: u64,
    sequencer: Sequencer<Option<CapturedCommit>>,
    /// After the first attribution gap, repair must establish a new baseline.
    gapped: bool,
}

/// A facet delete waiting on its root's stream.
#[derive(Debug, PartialEq)]
struct PendingDelete {
    /// Commits that arrived before it.
    after: u64,
    /// The first ticket asked after it arrived.
    ticket: u64,
    body: DeletedBody,
    at_ms: i64,
}

/// What a stream turns into records, in stream order.
#[derive(Debug, PartialEq)]
enum Out<T> {
    Released(Released<T>),
    Deleted(PendingDelete),
}

/// Orders a root stream's facet deletes among its released commits: a
/// delete goes out after every commit that arrived before it, and once a
/// ticket asked after it has settled. Released commits behind a waiting
/// delete wait with it.
#[derive(Debug)]
struct Sequencer<T> {
    received: u64,
    emitted: u64,
    proven_ticket: u64,
    held: VecDeque<Released<T>>,
    deletes: VecDeque<PendingDelete>,
}

impl<T> Default for Sequencer<T> {
    fn default() -> Self {
        Self {
            received: 0,
            emitted: 0,
            proven_ticket: 0,
            held: VecDeque::new(),
            deletes: VecDeque::new(),
        }
    }
}

impl<T> Sequencer<T> {
    /// A commit arrived from the cell thread.
    fn commit(&mut self) {
        self.received += 1;
    }

    /// A facet delete arrived. `next_ticket` is the number of the next
    /// ticket the stream asks for.
    fn delete(&mut self, next_ticket: u64, body: DeletedBody, at_ms: i64) {
        self.deletes.push_back(PendingDelete {
            after: self.received,
            ticket: next_ticket,
            body,
            at_ms,
        });
    }

    /// A ticket settled with a proof.
    fn proven(&mut self, ticket: u64) {
        self.proven_ticket = self.proven_ticket.max(ticket);
    }

    fn waiting(&self) -> usize {
        self.deletes.len()
    }

    /// Nothing is held back: every released commit has gone out.
    fn idle(&self) -> bool {
        self.held.is_empty() && self.deletes.is_empty()
    }

    /// Take what may go out now. `drained` says the attribution holds no
    /// commit, so every commit that arrived has been released.
    fn take(&mut self, released: Vec<Released<T>>, drained: bool) -> Vec<Out<T>> {
        self.held.extend(released);
        let mut out = Vec::new();
        loop {
            if let Some(delete) = self.deletes.front() {
                let ahead_done = self.emitted >= delete.after || (drained && self.held.is_empty());
                if ahead_done {
                    if self.proven_ticket < delete.ticket {
                        break;
                    }
                    let delete = self.deletes.pop_front().expect("front");
                    out.push(Out::Deleted(delete));
                    continue;
                }
            }
            let Some(entry) = self.held.pop_front() else {
                break;
            };
            self.emitted += match &entry {
                Released::Commit { .. } => 1,
                Released::Gap {
                    unmatched,
                    overflowed,
                    ..
                } => unmatched + overflowed,
            };
            out.push(Out::Released(entry));
        }
        out
    }
}

impl Stream {
    fn new(
        exporter: Arc<Exporter>,
        key: (String, u64),
        link: ActivationLink,
        this: mpsc::WeakUnboundedSender<Input>,
    ) -> Self {
        let (cell, _) = &key;
        let (root, facet) = stream_root(cell);
        let epoch = key.1;
        let identity = StreamId {
            script: String::new(),
            class: stream_class(cell).to_string(),
            cell: root.to_string(),
            facet: facet.map(str::to_string),
            incarnation: 0,
        };
        Stream {
            exporter,
            key,
            identity,
            cell_name: None,
            link: Some(link),
            attribution: Attribution::new(),
            next_commit: 1,
            last_txid: 0,
            delivered: Position::new(epoch, 0, 0),
            next_ticket: 1,
            outstanding: None,
            wanted: None,
            retry: TICKET_RETRY,
            retry_at: None,
            keepalive: None,
            this,
            counted_bytes: 0,
            counted_commits: 0,
            last_position: 0,
            sequencer: Sequencer::default(),
            gapped: false,
        }
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Input>) {
        loop {
            let input = match self.retry_at {
                // `recv` is cancel-safe, so the deadline loses nothing.
                Some(at) => match crate::asyncrt::timeout_at(at, rx.recv()).await {
                    Ok(input) => input,
                    Err(crate::asyncrt::Elapsed) => {
                        self.retry_at = None;
                        self.step(None);
                        continue;
                    }
                },
                None => rx.recv().await,
            };
            let Some(input) = input else {
                break;
            };
            if !self.step(Some(input)) {
                break;
            }
        }
        self.account(0, 0);
        if self.sequencer.waiting() > 0 {
            tracing::warn!(
                cell = %self.key.0,
                epoch = self.key.1,
                deletes = self.sequencer.waiting(),
                "export: the stream ended before its facet deletes were proven; the reconciler reports them"
            );
        }
        let mut streams = self
            .exporter
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if streams
            .get(&self.key)
            .is_some_and(|tx| tx.upgrade().is_none())
        {
            streams.remove(&self.key);
        }
    }

    /// Apply one input and act on it. `false` when the stream must stop.
    fn step(&mut self, input: Option<Input>) -> bool {
        let exporter = self.exporter.clone();
        let counters = &exporter.counters;
        match input {
            None => {}
            Some(Input::Ended) => return false,
            Some(Input::Identity(identity)) => {
                self.identity.script = identity.script;
                self.identity.incarnation = identity.incarnation;
                self.cell_name = identity.cell_name;
                if let Some(link) = self.link.take() {
                    self.submit_link(link);
                }
            }
            Some(Input::Named(name)) => self.cell_name = Some(name),
            Some(Input::Commit {
                stamp,
                position,
                bytes,
                commit,
            }) => {
                // The reservation moves from the FIFO to the pending list,
                // which `account` charges below.
                exporter.release_queued(bytes);
                if !commit.bulk.is_empty() {
                    counters.bulk_commits.fetch_add(1, Ordering::Relaxed);
                }
                self.sequencer.commit();
                self.last_position = self.last_position.max(position);
                match stamp {
                    WalStamp::At {
                        salt1,
                        salt2,
                        frames,
                    } => self.attribution.commit(
                        WalPoint {
                            generation: WalGeneration { salt1, salt2 },
                            frames,
                        },
                        bytes,
                        Some(commit),
                    ),
                    WalStamp::Unplaced => self.attribution.unplaced(bytes, Some(commit)),
                }
                self.wanted = Some(self.wanted.map_or(position, |wanted| wanted.max(position)));
            }
            Some(Input::Shed { stamp, position }) => {
                // A shed commit still takes its place in the stream, as a
                // gap that counts it.
                self.sequencer.commit();
                self.last_position = self.last_position.max(position);
                match stamp {
                    WalStamp::At {
                        salt1,
                        salt2,
                        frames,
                    } => self.attribution.dropped(WalPoint {
                        generation: WalGeneration { salt1, salt2 },
                        frames,
                    }),
                    WalStamp::Unplaced => self.attribution.unplaced(0, None),
                }
                self.wanted = Some(self.wanted.map_or(position, |wanted| wanted.max(position)));
            }
            Some(Input::Wrote {
                stamp:
                    WalStamp::At {
                        salt1,
                        salt2,
                        frames,
                    },
                position,
            }) => {
                self.sequencer.commit();
                self.last_position = self.last_position.max(position);
                self.attribution.commit(
                    WalPoint {
                        generation: WalGeneration { salt1, salt2 },
                        frames,
                    },
                    0,
                    None,
                );
                self.wanted = Some(self.wanted.map_or(position, |wanted| wanted.max(position)));
            }
            // Without a WAL point there is nothing to attribute: the commit's
            // capture was already reported, and the next commit settles it.
            Some(Input::Wrote { .. }) => {}
            Some(Input::CaughtUp) => self.attribution.caught_up(),
            Some(Input::FacetDeleted { body, at_ms }) => {
                self.sequencer.delete(self.next_ticket, body, at_ms);
                // The delete needs a ticket of its own, asked after it.
                self.wanted = Some(
                    self.wanted
                        .map_or(self.last_position, |wanted| wanted.max(self.last_position)),
                );
            }
            Some(Input::Captured(capture)) => self.attribution.captured(&capture),
            Some(Input::Proven { ticket, result }) => {
                let Some((asked, position)) = self.outstanding else {
                    return true;
                };
                if asked != ticket {
                    return true;
                }
                self.outstanding = None;
                self.keepalive = None;
                match result {
                    Ok(txid) => {
                        self.retry = TICKET_RETRY;
                        self.sequencer.proven(ticket);
                        self.attribution.proven(txid);
                    }
                    Err(RequestError::NodeFenced) => {
                        tracing::warn!(cell = %self.key.0, epoch = self.key.1, "export: node fenced; the stream stops");
                        return false;
                    }
                    Err(error) => {
                        tracing::debug!(cell = %self.key.0, epoch = self.key.1, ?error, "export ticket failed; retrying");
                        self.wanted =
                            Some(self.wanted.map_or(position, |wanted| wanted.max(position)));
                        self.retry_at =
                            Some(crate::asyncrt::mono_ms() + self.retry.as_millis() as u64);
                        self.retry = (self.retry * 2).min(TICKET_RETRY_MAX);
                    }
                }
            }
        }
        let before = self.attribution.unmatched_total();
        self.shed();
        let released = self.attribution.take_released();
        let mismatched = self.attribution.unmatched_total() - before;
        counters
            .attribution_mismatches
            .fetch_add(mismatched, Ordering::Relaxed);
        let out = self
            .sequencer
            .take(released, self.attribution.pending_len() == 0);
        self.release(out);
        // Commits held behind a facet delete are submitted later, so the
        // delivered position may not pass them yet.
        if self.sequencer.idle() {
            self.advance();
        }
        self.account(
            self.attribution.pending_bytes(),
            self.attribution.pending_len() as u64,
        );
        if self.outstanding.is_none() && self.retry_at.is_none() {
            if let Some(position) = self.wanted.take() {
                self.ask(position);
            }
        }
        true
    }

    fn ask(&mut self, position: u64) {
        // Tickets are unique across the node's streams, so a verdict for a
        // stream that ended (a deleted facet) cannot settle the ticket of
        // the stream that replaced it under the same key.
        let ticket = self.exporter.next_ticket.fetch_add(1, Ordering::Relaxed);
        self.next_ticket = ticket + 1;
        self.outstanding = Some((ticket, position));
        self.keepalive = self.this.upgrade();
        let ask = TicketAsk {
            cell: self.key.0.clone(),
            epoch: self.key.1,
            position,
            ticket,
        };
        if self.identity.facet.is_some() {
            self.exporter.relay(ask, self.this.clone());
        } else {
            (self.exporter.ask)(ask);
        }
    }

    /// Hold the pending list and the sink's buffer under the shared budget.
    fn shed(&mut self) {
        let budget = self.exporter.config.queue_bytes as u64;
        let others = self
            .exporter
            .counters
            .pending_bytes
            .load(Ordering::Relaxed)
            .saturating_sub(self.counted_bytes);
        let own = self.attribution.pending_bytes();
        let total = others + own + self.exporter.sink.buffered_bytes();
        if total > budget {
            let before = self.attribution.pending_len();
            self.attribution.shed_to(own.saturating_sub(total - budget));
            let dropped = before.saturating_sub(self.attribution.pending_len());
            self.exporter
                .counters
                .dropped_records
                .fetch_add(dropped as u64, Ordering::Relaxed);
        }
    }

    fn account(&mut self, bytes: u64, commits: u64) {
        let counters = &self.exporter.counters;
        counters
            .pending_bytes
            .fetch_add(bytes.wrapping_sub(self.counted_bytes), Ordering::Relaxed);
        counters.pending_commits.fetch_add(
            commits.wrapping_sub(self.counted_commits),
            Ordering::Relaxed,
        );
        self.counted_bytes = bytes;
        self.counted_commits = commits;
    }

    /// Move the delivered position over TXIDs that hold no exported commit.
    ///
    /// A commit's position carries the TXID of the capture that holds it,
    /// but a cell also writes TXIDs no record stands for: writes to tables
    /// the export skips, the capture loop's own bookkeeping. The released
    /// position covers them, and a delivered position that stopped at the
    /// last commit would put every later `link` (whose predecessor is the
    /// restored chain's last TXID) beyond what the consumer certified.
    /// Every later commit is labelled above the released position, and
    /// every later gap sorts after it (see `last_txid`).
    fn advance(&mut self) {
        if self.gapped {
            return;
        }
        let released = self.attribution.released_position();
        let position = Position::new(self.key.1, released, self.next_commit - 1);
        if position <= self.delivered {
            return;
        }
        self.delivered = position;
        self.last_txid = self.last_txid.max(released);
        self.exporter.advance(Submitted {
            key: self.key.clone(),
            stream: self.identity.clone(),
            cell_name: self.cell_name.clone(),
            position,
            whole: false,
            closes: true,
            watermark: false,
            recovered: false,
        });
    }

    /// Submit the residency's `link`, ahead of every commit.
    fn submit_link(&mut self, link: ActivationLink) {
        let position = link_position(self.key.1, &link);
        self.delivered = self.delivered.max(position);
        let envelope = self.exporter.envelope(
            &self.identity,
            &self.cell_name,
            position,
            crate::asyncrt::wall_ms(),
        );
        let mut records = vec![Record {
            envelope: envelope.clone(),
            body: Body::Link(link_body(&link)),
        }];
        // A predecessor whose position the activation could not read may
        // hold records the consumer never got. A link without `prev_txid`
        // reads as gap-free, so the unknown span is reported as a gap over
        // the whole predecessor epoch, at the link's position.
        if let (Some(prev_epoch), None) = (link.prev_epoch, link.prev_txid) {
            self.exporter.counters.gaps.fetch_add(1, Ordering::Relaxed);
            records.push(Record {
                envelope,
                body: Body::Gap(unknown_predecessor(prev_epoch)),
            });
        }
        let last = records.len() - 1;
        let out = records
            .into_iter()
            .enumerate()
            .map(|(index, record)| {
                let meta = Submitted {
                    key: self.key.clone(),
                    stream: self.identity.clone(),
                    cell_name: self.cell_name.clone(),
                    position,
                    whole: true,
                    closes: index == last,
                    watermark: false,
                    recovered: false,
                };
                (record, meta)
            })
            .collect();
        self.exporter.submit(out);
    }

    /// Turn released commits, gaps, and facet deletes into records and
    /// submit them.
    fn release(&mut self, released: Vec<Out<Option<CapturedCommit>>>) {
        let epoch = self.key.1;
        let max_record = self.exporter.config.max_record_bytes;
        let mut out: Vec<(Record, Submitted)> = Vec::new();
        for entry in released {
            if self.gapped {
                break;
            }
            let entry = match entry {
                // A commit with no exported row takes no position; the
                // released position covers its TXID.
                Out::Released(Released::Commit {
                    label,
                    payload: None,
                }) => {
                    self.last_txid = self.last_txid.max(label);
                    continue;
                }
                Out::Released(Released::Commit {
                    label,
                    payload: Some(payload),
                }) => Out::Released(Released::Commit { label, payload }),
                Out::Released(Released::Gap {
                    after,
                    through,
                    unmatched,
                    overflowed,
                }) => Out::Released(Released::Gap {
                    after,
                    through,
                    unmatched,
                    overflowed,
                }),
                Out::Deleted(delete) => Out::Deleted(delete),
            };
            let number = self.next_commit;
            self.next_commit += 1;
            let mut records = Vec::new();
            match entry {
                Out::Deleted(delete) => {
                    let position = Position::new(epoch, self.last_txid, number);
                    records.push(Record {
                        envelope: self.exporter.envelope(
                            &self.identity,
                            &self.cell_name,
                            position,
                            delete.at_ms,
                        ),
                        body: Body::Deleted(delete.body),
                    });
                }
                Out::Released(Released::Commit { label, payload }) => {
                    self.last_txid = self.last_txid.max(label);
                    let position = Position::new(epoch, label, number);
                    let envelope = self.exporter.envelope(
                        &self.identity,
                        &self.cell_name,
                        position,
                        payload.committed_at,
                    );
                    let mut bulk: Vec<TableGen> = payload.bulk;
                    // Definitions first, so a consumer reading the commit in
                    // order meets a generation before its rows.
                    records.extend(
                        payload
                            .schemas
                            .into_iter()
                            .map(|schema: SchemaBody| Record {
                                envelope: envelope.clone(),
                                body: Body::Schema(schema),
                            }),
                    );
                    if let Some(snapshot) = payload.snapshot {
                        let mut covered = Vec::new();
                        let mut count = 0u64;
                        for table in snapshot.tables {
                            let record = Record {
                                envelope: envelope.clone(),
                                body: Body::Snapshot(SnapshotBody {
                                    snapshot_id: snapshot.id.clone(),
                                    data: table,
                                }),
                            };
                            match split(record, max_record) {
                                Split::Fragments(fragments) => {
                                    if let Some(Body::Snapshot(first)) =
                                        fragments.first().map(|f| &f.body)
                                    {
                                        covered.push(first.data.table_gen());
                                    }
                                    count += 1;
                                    records.extend(fragments);
                                }
                                // A row too large for any record: the table
                                // leaves the snapshot and is unknown instead.
                                Split::Bulk(record) => {
                                    if let Body::Bulk(body) = record.body {
                                        bulk.extend(body.tables);
                                    }
                                }
                            }
                        }
                        if !covered.is_empty() {
                            records.push(Record {
                                envelope: envelope.clone(),
                                body: Body::SnapshotEnd(SnapshotEndBody {
                                    snapshot_id: snapshot.id,
                                    scope: SnapshotScope::Tables,
                                    tables: covered,
                                    records: count,
                                }),
                            });
                        }
                    }
                    for table in payload.tables {
                        let record = Record {
                            envelope: envelope.clone(),
                            body: Body::Rows(RowsBody { data: table }),
                        };
                        match split(record, max_record) {
                            Split::Fragments(fragments) => records.extend(fragments),
                            Split::Bulk(record) => {
                                if let Body::Bulk(body) = record.body {
                                    bulk.extend(body.tables);
                                }
                            }
                        }
                    }
                    if !bulk.is_empty() {
                        bulk.sort();
                        bulk.dedup();
                        records.push(Record {
                            envelope,
                            body: Body::Bulk(BulkBody { tables: bulk }),
                        });
                    }
                }
                Out::Released(Released::Gap {
                    after,
                    through,
                    unmatched,
                    overflowed,
                }) => {
                    self.gapped = true;
                    self.exporter.counters.gaps.fetch_add(1, Ordering::Relaxed);
                    let position = Position::new(epoch, self.last_txid.max(after), number);
                    records.push(Record {
                        envelope: self.exporter.envelope(
                            &self.identity,
                            &self.cell_name,
                            position,
                            crate::asyncrt::wall_ms(),
                        ),
                        body: Body::Gap(GapBody {
                            from: Position::new(epoch, after, 0),
                            to: Position::new(epoch, through, u64::MAX),
                            reason: format!(
                                "{unmatched} commits unmatched, {overflowed} over the export budget"
                            ),
                        }),
                    });
                }
            }
            let last = records.len().saturating_sub(1);
            if let Some(record) = records.last() {
                self.delivered = self.delivered.max(record.envelope.position);
            }
            for (index, record) in records.into_iter().enumerate() {
                let meta = Submitted {
                    key: self.key.clone(),
                    stream: self.identity.clone(),
                    cell_name: self.cell_name.clone(),
                    position: record.envelope.position,
                    whole: record.envelope.fragment == record.envelope.fragments,
                    closes: index == last,
                    watermark: false,
                    recovered: false,
                };
                out.push((record, meta));
            }
        }
        self.exporter.submit(out);
    }
}

/// What delivery knows about one stream.
#[derive(Default, Serialize, Deserialize)]
struct Delivered {
    stream: Option<(StreamId, Option<String>)>,
    /// The newest position whose commit is wholly acknowledged.
    position: Option<Position>,
    /// The `through` of the last watermark submitted.
    marked: Option<Position>,
    /// Whole records acknowledged since the last watermark, per position.
    #[serde(with = "acked_entries")]
    acked: BTreeMap<Position, u64>,
    frozen: bool,
}

/// A stream as delivery tracks it: the residency and its incarnation. A
/// facet deleted and recreated in one epoch has the same residency key and
/// a new incarnation, and its positions start over.
type DeliveryKey = (String, u64, u64);

fn delivery_key(meta: &Submitted) -> DeliveryKey {
    (meta.key.0.clone(), meta.key.1, meta.stream.incarnation)
}

// Historical residencies must not remain in RAM or be scanned on every ack.
// Keep a small hot set and spill the rest to a process-local SQLite file. The
// saved counts also preserve the chain when a cell reopens in the same epoch.
const DELIVERY_CACHE_SIZE: usize = 256;

struct DeliveryStates {
    hot: BTreeMap<DeliveryKey, (u64, Delivered)>,
    clock: u64,
    cold: rusqlite::Connection,
}

impl DeliveryStates {
    fn new() -> anyhow::Result<Self> {
        let cold = rusqlite::Connection::open("")?;
        cold.execute_batch(
            "PRAGMA cache_size=-1024; PRAGMA journal_mode=OFF; PRAGMA synchronous=OFF;
            CREATE TABLE delivery (key TEXT PRIMARY KEY, value BLOB NOT NULL);",
        )?;
        Ok(Self {
            hot: BTreeMap::new(),
            clock: 0,
            cold,
        })
    }

    fn get(&mut self, key: DeliveryKey) -> anyhow::Result<&mut Delivered> {
        self.clock += 1;
        if !self.hot.contains_key(&key) {
            if self.hot.len() >= DELIVERY_CACHE_SIZE {
                let oldest = self
                    .hot
                    .iter()
                    .min_by_key(|(_, (used, _))| used)
                    .unwrap()
                    .0
                    .clone();
                let (_, state) = self.hot.remove(&oldest).unwrap();
                self.cold.execute(
                    "INSERT OR REPLACE INTO delivery VALUES (?1, ?2)",
                    rusqlite::params![serde_json::to_string(&oldest)?, serde_json::to_vec(&state)?],
                )?;
            }
            let encoded = serde_json::to_string(&key)?;
            let bytes: Option<Vec<u8>> = self
                .cold
                .query_row(
                    "SELECT value FROM delivery WHERE key=?1",
                    [&encoded],
                    |row| row.get(0),
                )
                .optional()?;
            let state = bytes
                .map(|bytes| serde_json::from_slice(&bytes))
                .transpose()?
                .unwrap_or_default();
            self.cold
                .execute("DELETE FROM delivery WHERE key=?1", [&encoded])?;
            self.hot.insert(key.clone(), (self.clock, state));
        }
        let (used, state) = self.hot.get_mut(&key).unwrap();
        *used = self.clock;
        Ok(state)
    }
}

mod acked_entries {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        map: &BTreeMap<Position, u64>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<BTreeMap<Position, u64>, D::Error> {
        Ok(Vec::<(Position, u64)>::deserialize(deserializer)?
            .into_iter()
            .collect())
    }
}

/// Read the sink's results, advance delivered positions, and submit
/// watermarks and gaps.
async fn deliver(exporter: Arc<Exporter>, outcomes: mpsc::UnboundedReceiver<Outcome>) {
    if let Err(error) = deliver_inner(&exporter, outcomes).await {
        // A failed spill must never reset a watermark's counts. Stop exporting;
        // the next link/reconciliation reports the uncertified tail.
        exporter.delivery_failed.store(true, Ordering::SeqCst);
        exporter
            .submitted
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        exporter
            .advances
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        exporter.delivering.store(false, Ordering::SeqCst);
        tracing::error!(%error, "export: delivery bookkeeping failed; export stopped");
    }
}

async fn deliver_inner(
    exporter: &Exporter,
    mut outcomes: mpsc::UnboundedReceiver<Outcome>,
) -> anyhow::Result<()> {
    let mut streams = DeliveryStates::new()?;
    while let Some(outcome) = outcomes.recv().await {
        exporter.delivering.store(true, Ordering::SeqCst);
        streams.cold.execute_batch("BEGIN;")?;
        let mut follow_up: Vec<(Record, Submitted)> = Vec::new();
        let mut dirty = BTreeSet::new();
        for (seq, result) in outcome.results {
            let Some(meta) = exporter
                .submitted
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&seq)
            else {
                continue;
            };
            if meta.recovered {
                if let SinkDelivery::Dropped { reason } = result {
                    exporter
                        .counters
                        .dropped_records
                        .fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(cell = %meta.key.0, epoch = meta.key.1, %reason, "export: sink dropped a recovered record; the reconciler covers the cell");
                }
                continue;
            }
            let key = delivery_key(&meta);
            dirty.insert(key.clone());
            let state = streams.get(key)?;
            state.stream = Some((meta.stream.clone(), meta.cell_name.clone()));
            if state.frozen {
                continue;
            }
            match result {
                SinkDelivery::Acknowledged { .. } if meta.watermark => {}
                SinkDelivery::Acknowledged { .. } => {
                    exporter
                        .counters
                        .delivered_records
                        .fetch_add(1, Ordering::Relaxed);
                    if meta.whole {
                        *state.acked.entry(meta.position).or_default() += 1;
                    }
                    if meta.closes {
                        state.position = Some(meta.position);
                    }
                }
                SinkDelivery::Dropped { reason } => {
                    exporter
                        .counters
                        .dropped_records
                        .fetch_add(1, Ordering::Relaxed);
                    exporter.counters.gaps.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(cell = %meta.key.0, epoch = meta.key.1, %reason, "export: sink dropped a record; the delivered position freezes");
                    state.frozen = true;
                    let from = state.position.unwrap_or(Position::new(meta.key.1, 0, 0));
                    let record = Record {
                        envelope: exporter.envelope(
                            &meta.stream,
                            &meta.cell_name,
                            meta.position,
                            crate::asyncrt::wall_ms(),
                        ),
                        body: Body::Gap(GapBody {
                            from,
                            to: meta.position,
                            reason: format!("sink dropped a record: {reason}"),
                        }),
                    };
                    follow_up.push((
                        record,
                        Submitted {
                            watermark: true,
                            ..meta
                        },
                    ));
                }
            }
        }
        resolve_advances(exporter, &mut streams, &mut dirty)?;
        for key in dirty {
            let state = streams.get(key.clone())?;
            let Some(through) = state.position else {
                continue;
            };
            if state.frozen || state.marked.is_some_and(|marked| marked >= through) {
                continue;
            }
            let Some((stream, cell_name)) = state.stream.clone() else {
                continue;
            };
            // Records of a commit whose last record is still in flight lie
            // past `through` and wait for the next watermark.
            let later = state.acked.split_off(&Position {
                commit: through.commit + 1,
                ..through
            });
            let certified = std::mem::replace(&mut state.acked, later);
            let body = WatermarkBody {
                from: state.marked,
                through,
                commits: certified.len() as u64,
                records: certified.values().sum(),
            };
            exporter.counters.watermarks.fetch_add(1, Ordering::Relaxed);
            follow_up.push((
                Record {
                    envelope: exporter.envelope(
                        &stream,
                        &cell_name,
                        through,
                        crate::asyncrt::wall_ms(),
                    ),
                    body: Body::Watermark(body),
                },
                Submitted {
                    key: (key.0.clone(), key.1),
                    stream,
                    cell_name,
                    position: through,
                    whole: false,
                    closes: false,
                    watermark: true,
                    recovered: false,
                },
            ));
            state.marked = Some(through);
        }
        streams.cold.execute_batch("COMMIT;")?;
        exporter.submit(follow_up);
        exporter.delivering.store(false, Ordering::SeqCst);
    }
    Ok(())
}

/// Apply the advances that nothing submitted before them still holds up.
fn resolve_advances(
    exporter: &Exporter,
    streams: &mut DeliveryStates,
    dirty: &mut BTreeSet<DeliveryKey>,
) -> anyhow::Result<()> {
    let ready = {
        let submitted = exporter.submitted.lock().unwrap_or_else(|e| e.into_inner());
        let mut advances = exporter.advances.lock().unwrap_or_else(|e| e.into_inner());
        match submitted.keys().next() {
            Some(&oldest) => {
                let later = advances.split_off(&oldest);
                std::mem::replace(&mut *advances, later)
            }
            None => std::mem::take(&mut *advances),
        }
    };
    for meta in ready.into_values() {
        let key = delivery_key(&meta);
        dirty.insert(key.clone());
        let state = streams.get(key)?;
        state.stream = Some((meta.stream, meta.cell_name));
        if !state.frozen && state.position.is_none_or(|at| at < meta.position) {
            state.position = Some(meta.position);
        }
    }
    Ok(())
}

/// Where a residency's link sits: after every position an earlier residency
/// of the same epoch could hold (their txids are below `start_txid`), and
/// before every commit of this one (numbered from one).
fn link_position(epoch: u64, link: &ActivationLink) -> Position {
    Position::new(epoch, link.start_txid, 0)
}

/// The gap a link reports when its predecessor's position is unknown.
fn unknown_predecessor(prev_epoch: u64) -> GapBody {
    GapBody {
        from: Position::new(prev_epoch, 0, 0),
        to: Position::new(prev_epoch, u64::MAX, u64::MAX),
        reason: "the activation could not read its predecessor's position".to_string(),
    }
}

fn link_body(link: &ActivationLink) -> LinkBody {
    LinkBody {
        start_txid: link.start_txid,
        prev_epoch: link.prev_epoch,
        prev_txid: link.prev_txid,
        mode: match link.mode {
            ActivationMode::Fresh => LinkMode::Fresh,
            ActivationMode::Clone => LinkMode::Clone,
            ActivationMode::Paged => LinkMode::Paged,
            ActivationMode::Resume => LinkMode::Resume,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celld_export_format::{Op, RowChange, TableRows, Value};
    use futures_util::future::BoxFuture;

    fn config(queue_bytes: usize) -> Config {
        Config::from_lookup(|name| {
            Ok(match name {
                "CELLD_EXPORT" => Some("1".to_string()),
                "CELLD_EXPORT_QUEUE_BYTES" => Some(queue_bytes.to_string()),
                "CELLD_EXPORT_MAX_RECORD_BYTES" => Some("65536".to_string()),
                _ => None,
            })
        })
        .unwrap()
        .unwrap()
    }

    /// Holds what it is given until a flush, then acknowledges all of it.
    struct FakeSink {
        outcomes: mpsc::UnboundedSender<Outcome>,
        held: Mutex<Vec<SinkRecord>>,
        /// What happened, in order: `rows`, `gap`, `watermark`, `close`.
        log: Mutex<Vec<&'static str>>,
        closed: AtomicBool,
    }

    impl ExportSink for FakeSink {
        fn name(&self) -> &'static str {
            "fake"
        }

        fn submit(&self, records: Vec<SinkRecord>) -> Result<(), crate::export_sink::Closed> {
            if self.closed.load(Ordering::SeqCst) {
                return Err(crate::export_sink::Closed);
            }
            self.held.lock().unwrap().extend(records);
            Ok(())
        }

        fn flush(&self) {
            let held = std::mem::take(&mut *self.held.lock().unwrap());
            if held.is_empty() {
                return;
            }
            let mut results = Vec::new();
            for record in held {
                self.log.lock().unwrap().push(match record.record.body {
                    Body::Watermark(_) => "watermark",
                    Body::Gap(_) => "gap",
                    _ => "rows",
                });
                results.push((
                    record.seq,
                    SinkDelivery::Acknowledged {
                        object: Arc::from("object"),
                    },
                ));
            }
            let _ = self.outcomes.send(Outcome {
                sink: "fake",
                results,
            });
        }

        fn buffered_bytes(&self) -> u64 {
            0
        }

        fn close(&self) -> BoxFuture<'static, ()> {
            self.flush();
            self.closed.store(true, Ordering::SeqCst);
            self.log.lock().unwrap().push("close");
            Box::pin(async {})
        }
    }

    fn exporter(
        queue_bytes: usize,
    ) -> (
        Arc<Exporter>,
        Arc<FakeSink>,
        mpsc::UnboundedReceiver<Outcome>,
    ) {
        let (outcomes, outcomes_rx) = mpsc::unbounded_channel();
        let sink = Arc::new(FakeSink {
            outcomes,
            held: Mutex::new(Vec::new()),
            log: Mutex::new(Vec::new()),
            closed: AtomicBool::new(false),
        });
        let exporter = Exporter::new(
            config(queue_bytes),
            "node-a".to_string(),
            sink.clone(),
            Box::new(|_| {}),
            sink.outcomes.clone(),
        );
        (exporter, sink, outcomes_rx)
    }

    fn commit_of(text_bytes: usize) -> CapturedCommit {
        CapturedCommit {
            seq: 1,
            committed_at: 0,
            tables: vec![TableRows {
                table: "items".to_string(),
                generation: 0,
                columns: vec!["id".to_string(), "body".to_string()],
                key_columns: vec!["id".to_string()],
                rows: vec![RowChange(
                    Op::Insert,
                    vec![Value::Integer(1)],
                    vec![Value::Integer(1), Value::Text("x".repeat(text_bytes))],
                )],
            }],
            bulk: Vec::new(),
            schemas: Vec::new(),
            snapshot: None,
        }
    }

    #[test]
    fn commits_past_the_budget_queue_as_gaps() {
        let budget = 1024 * 1024;
        let (exporter, _sink, _outcomes) = exporter(budget);
        let (tx, mut rx) = mpsc::unbounded_channel();
        let stream = CellStream {
            exporter: exporter.clone(),
            tx,
            denied: HashSet::new(),
        };
        let stamp = WalStamp::At {
            salt1: 1,
            salt2: 2,
            frames: 1,
        };
        for position in 1..=100 {
            stream.commit(stamp, position, commit_of(32 * 1024));
        }
        let (mut queued, mut commits, mut shed) = (0, 0, 0);
        let mut last = 0;
        while let Ok(input) = rx.try_recv() {
            match input {
                Input::Commit {
                    position, bytes, ..
                } => {
                    assert_eq!(shed, 0, "a commit queued after one was shed");
                    queued += bytes;
                    commits += 1;
                    last = position;
                }
                Input::Shed { position, .. } => {
                    shed += 1;
                    last = position;
                }
                _ => unreachable!(),
            }
        }
        assert_eq!(last, 100, "every commit keeps its place on the FIFO");
        assert_eq!(commits + shed, 100);
        assert!(shed > 0 && commits > 0);
        assert!(queued <= budget as u64);
        let state = exporter.state();
        assert_eq!(state["queue_bytes"], queued);
        assert_eq!(state["dropped_records"], shed);

        // Once the stream takes its commits off the FIFO the budget frees.
        exporter.release_queued(queued);
        stream.commit(stamp, 101, commit_of(32 * 1024));
        assert!(matches!(rx.try_recv(), Ok(Input::Commit { .. })));
    }

    #[test]
    fn close_writes_the_watermark_before_closing_the_sink() {
        crate::asyncrt::test_block_on(async {
            let (exporter, sink, outcomes) = exporter(1024 * 1024);
            #[allow(clippy::disallowed_methods)]
            tokio::spawn(deliver(exporter.clone(), outcomes));
            let stream = StreamId {
                script: "app".to_string(),
                class: "Items".to_string(),
                cell: "Items:a".to_string(),
                facet: None,
                incarnation: 0,
            };
            let position = Position::new(1, 0, 1);
            exporter.submit(vec![(
                Record {
                    envelope: exporter.envelope(&stream, &None, position, 0),
                    body: Body::Rows(RowsBody {
                        data: commit_of(8).tables.remove(0),
                    }),
                },
                Submitted {
                    key: ("Items:a".to_string(), 1),
                    stream,
                    cell_name: None,
                    position,
                    whole: true,
                    closes: true,
                    watermark: false,
                    recovered: false,
                },
            )]);
            exporter.close().await;
            assert_eq!(*sink.log.lock().unwrap(), ["rows", "watermark", "close"]);
            assert_eq!(exporter.state()["watermarks"], 1);
        });
    }

    fn delete_body(through: u64) -> DeletedBody {
        DeletedBody {
            facet: Some("facets/aa".into()),
            incarnation: None,
            subtree: true,
            through_incarnation: Some(through),
        }
    }

    fn commit(label: u64) -> Released<u64> {
        Released::Commit {
            label,
            payload: label,
        }
    }

    /// The stream order `take` produced: a commit's payload, or `D` and the
    /// delete's bound.
    fn order(out: Vec<Out<u64>>) -> Vec<String> {
        out.into_iter()
            .map(|out| match out {
                Out::Released(Released::Commit { payload, .. }) => payload.to_string(),
                Out::Released(Released::Gap { through, .. }) => format!("gap..{through}"),
                Out::Deleted(delete) => {
                    format!("D{}", delete.body.through_incarnation.unwrap())
                }
            })
            .collect()
    }

    #[test]
    fn a_facet_delete_follows_the_commits_before_it_and_leads_the_rest() {
        let mut seq = Sequencer::default();
        seq.commit();
        seq.commit();
        seq.delete(5, delete_body(9), 0);
        seq.commit();
        // The first commit is out; the delete waits for the second.
        assert_eq!(order(seq.take(vec![commit(1)], false)), ["1"]);
        seq.proven(5);
        assert_eq!(order(seq.take(vec![], false)), Vec::<String>::new());
        // The commit after the delete, released with the one before it,
        // goes out after the delete.
        assert_eq!(
            order(seq.take(vec![commit(2), commit(3)], true)),
            ["2", "D9", "3"]
        );
        assert_eq!(seq.waiting(), 0);
    }

    #[test]
    fn a_facet_delete_waits_for_a_ticket_asked_after_it() {
        let mut seq = Sequencer::default();
        seq.commit();
        // Ticket 3 was in flight when the delete arrived; 4 is asked next.
        seq.delete(4, delete_body(7), 0);
        seq.commit();
        seq.proven(3);
        // Released commits behind the waiting delete wait with it.
        assert_eq!(order(seq.take(vec![commit(1), commit(2)], true)), ["1"]);
        seq.proven(4);
        assert_eq!(order(seq.take(vec![], true)), ["D7", "2"]);
    }

    #[test]
    fn a_gap_counts_the_commits_it_dropped() {
        let mut seq = Sequencer::default();
        for _ in 0..3 {
            seq.commit();
        }
        seq.delete(1, delete_body(2), 0);
        seq.proven(1);
        let gap = Released::Gap {
            after: 0,
            through: 4,
            unmatched: 1,
            overflowed: 1,
        };
        assert_eq!(order(seq.take(vec![gap], false)), ["gap..4"]);
        assert_eq!(order(seq.take(vec![commit(5)], false)), ["5", "D2"]);
    }

    #[test]
    fn a_facet_delete_on_an_idle_stream_needs_only_its_ticket() {
        let mut seq = Sequencer::<u64>::default();
        seq.delete(1, delete_body(1), 0);
        seq.delete(1, delete_body(2), 0);
        assert!(seq.take(vec![], true).is_empty());
        seq.proven(1);
        assert_eq!(order(seq.take(vec![], true)), ["D1", "D2"]);
    }

    /// A sink that accepts every record and reports nothing.
    struct Silent;

    impl ExportSink for Silent {
        fn name(&self) -> &'static str {
            "silent"
        }
        fn submit(&self, _: Vec<SinkRecord>) -> Result<(), crate::export_sink::Closed> {
            Ok(())
        }
        fn flush(&self) {}
        fn buffered_bytes(&self) -> u64 {
            0
        }
        fn close(&self) -> futures_util::future::BoxFuture<'static, ()> {
            Box::pin(async {})
        }
    }

    /// Run on a process-lived runtime: stream tasks spawn on the host
    /// handle, which a `#[tokio::test]` runtime would not outlive.
    fn run(future: impl std::future::Future<Output = ()>) {
        static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
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
    fn a_deleted_facet_stream_ends_with_its_ticket_in_flight() {
        run(async {
            let config =
                Config::from_lookup(|name| Ok((name == "CELLD_EXPORT").then(|| "1".into())))
                    .unwrap()
                    .unwrap();
            let asks: Arc<Mutex<Vec<TicketAsk>>> = Arc::default();
            let (wake, _outcomes) = mpsc::unbounded_channel();
            let exporter = Exporter::new(
                config,
                "n".into(),
                Arc::new(Silent),
                Box::new({
                    let asks = asks.clone();
                    move |ask| asks.lock().unwrap().push(ask)
                }),
                wake,
            );
            let names = ["child".to_string()];
            let facet = crate::engine_api::facet_cell("Room:1", &names);
            let link = ActivationLink {
                mode: ActivationMode::Fresh,
                start_txid: 1,
                prev_epoch: None,
                prev_txid: None,
            };
            let observer = exporter.open_stream(&facet, 1, link).expect("exported");
            let stream = exporter.attach(&facet, 1).expect("attached");
            let task = exporter
                .streams
                .lock()
                .unwrap()
                .get(&(facet.clone(), 1))
                .cloned()
                .unwrap();
            stream.identify(Identity {
                script: "s".into(),
                incarnation: 5,
                cell_name: None,
            });
            stream.wrote(
                WalStamp::At {
                    salt1: 1,
                    salt2: 2,
                    frames: 3,
                },
                7,
            );
            // The relay proves the facet (no replication here) and asks the
            // root's gate.
            let relayed = loop {
                if let Some(ask) = asks.lock().unwrap().first().cloned() {
                    break ask;
                }
                crate::asyncrt::sleep(Duration::from_millis(5)).await;
            };
            assert_eq!(relayed.cell, "Room:1");
            assert!(relayed.ticket >= RELAY_TICKET);
            exporter.facet_deleted(
                "Room:1",
                1,
                &FacetDeleted {
                    stream: facet.clone(),
                    through: 9,
                },
            );
            drop(stream);
            drop(observer);
            // The task ends although its ticket is still in flight.
            let deadline = crate::asyncrt::mono_ms() + 5_000;
            while task.upgrade().is_some() {
                assert!(
                    crate::asyncrt::mono_ms() < deadline,
                    "the deleted facet's stream task is still running"
                );
                crate::asyncrt::sleep(Duration::from_millis(5)).await;
            }
            // The root's late verdict finds no one and leaves nothing behind.
            exporter.proven("Room:1", 1, relayed.ticket, Ok(0));
            assert!(exporter.relays.lock().unwrap().is_empty());
            assert!(!exporter.streams.lock().unwrap().contains_key(&(facet, 1)));
        });
    }

    #[test]
    fn a_facet_stream_names_its_root_and_path() {
        let names = |path: &[&str]| path.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let root = "Room:1";
        let nested = crate::engine_api::facet_cell(root, &names(&["a", "b"]));
        let (cell, path) = stream_root(&nested);
        assert_eq!(cell, root);
        assert_eq!(path, facet_path(root, &nested));
        assert_eq!(stream_class(&nested), "Room");
        assert_eq!(stream_root(root), (root, None));
        assert_eq!(stream_class(root), "Room");
    }

    #[test]
    fn facet_paths_are_the_stream_below_the_root() {
        let names = |path: &[&str]| path.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        let root = "Room:1";
        let child = crate::engine_api::facet_cell(root, &names(&["a", "b"]));
        let path = facet_path(root, &child).unwrap();
        assert!(path.starts_with("facets/"));
        assert_eq!(path.matches("/facets/").count(), 1);
        assert_eq!(facet_path(root, root), None);
        assert_eq!(facet_path(root, "Room:10/facets/x"), None);
    }

    #[test]
    fn a_facet_deleted_record_carries_its_bound() {
        let record = Record {
            envelope: Envelope {
                stream: StreamId {
                    script: "s".into(),
                    class: "Room".into(),
                    cell: "Room:1".into(),
                    facet: None,
                    incarnation: 0,
                },
                cell_name: None,
                position: Position::new(2, 7, 3),
                committed_at: 0,
                node: "n".into(),
                origin: Origin::Live,
                fragment: 1,
                fragments: 1,
            },
            body: Body::Deleted(delete_body(42)),
        };
        let json: serde_json::Value = serde_json::from_slice(&record.to_json()).unwrap();
        assert_eq!(json["kind"], "deleted");
        assert_eq!(json["target_facet"], "facets/aa");
        assert_eq!(json["subtree"], true);
        assert_eq!(json["through_incarnation"], 42);
        assert!(json.get("target_incarnation").is_none());
        assert_eq!(Record::from_json(&record.to_json()).unwrap(), record);
    }

    #[test]
    fn a_link_sorts_between_residencies() {
        // A clean reload of epoch 7 whose earlier residency reached TXID 41.
        let link = ActivationLink {
            mode: ActivationMode::Resume,
            start_txid: 42,
            prev_epoch: Some(7),
            prev_txid: Some(41),
        };
        let at = link_position(7, &link);
        assert!(
            Position::new(7, 41, u64::MAX) < at,
            "after the earlier residency"
        );
        assert!(
            at < Position::new(7, 42, 1),
            "before this residency's first commit"
        );
        assert_eq!(
            link_body(&link),
            LinkBody {
                start_txid: 42,
                prev_epoch: Some(7),
                prev_txid: Some(41),
                mode: LinkMode::Resume,
            }
        );
    }

    #[test]
    fn an_unknown_predecessor_is_a_gap() {
        use celld_export_format::Consumer;
        let stream = StreamId {
            script: "s".into(),
            class: "C".into(),
            cell: "C:x".into(),
            facet: None,
            incarnation: 1,
        };
        let record = |body| Record {
            envelope: Envelope {
                stream: stream.clone(),
                cell_name: None,
                position: Position::new(8, 1, 0),
                committed_at: 0,
                node: "n".into(),
                origin: Origin::Live,
                fragment: 1,
                fragments: 1,
            },
            body,
        };
        let link = ActivationLink {
            mode: ActivationMode::Clone,
            start_txid: 1,
            prev_epoch: Some(7),
            prev_txid: None,
        };
        let mut consumer = Consumer::new();
        consumer
            .ingest_all([
                record(Body::Link(link_body(&link))),
                record(Body::Gap(unknown_predecessor(7))),
            ])
            .unwrap();
        let state = consumer.state();
        let (_, state) = state.iter().find(|(id, _)| **id == stream).unwrap();
        assert_eq!(state.gaps.len(), 1, "{:?}", state.gaps);
    }

    #[test]
    fn recovered_records_name_each_exported_cell_epoch_and_the_loss() {
        let recovery = Recovery {
            session: "node-a/g1".into(),
            loss: true,
            cells: vec![
                RecoveredCell {
                    cell: "Chat:01".into(),
                    epoch: 4,
                    through: 17,
                },
                RecoveredCell {
                    cell: "Chat:01/facets/child".into(),
                    epoch: 2,
                    through: 3,
                },
                RecoveredCell {
                    cell: "__Queue:02".into(),
                    epoch: 1,
                    through: 9,
                },
            ],
        };
        let records = recovered_records("node-b", &recovery, |cell| {
            cell.starts_with("Chat:") && !cell.contains("/facets/")
        });
        assert_eq!(records.len(), 1);
        let record = &records[0];
        let head = Position::new(4, 17, u64::MAX);
        assert_eq!(
            record.envelope.stream,
            StreamId {
                script: String::new(),
                class: "Chat".into(),
                cell: "Chat:01".into(),
                facet: None,
                incarnation: 0,
            }
        );
        assert_eq!(record.envelope.position, head);
        assert_eq!(record.envelope.node, "node-b");
        assert_eq!(
            record.body,
            Body::Recovered(RecoveredBody {
                session: "node-a/g1".into(),
                head,
                loss: true,
                cells: 1,
            })
        );
    }

    #[test]
    fn recovered_records_name_a_facet_by_its_root_and_path() {
        let recovery = Recovery {
            session: "node-a/g1".into(),
            loss: false,
            cells: vec![RecoveredCell {
                cell: "Chat:01/facets/aa/facets/bb".into(),
                epoch: 2,
                through: 3,
            }],
        };
        let records = recovered_records("node-b", &recovery, |_| true);
        assert_eq!(
            records[0].envelope.stream,
            StreamId {
                script: String::new(),
                class: "Chat".into(),
                cell: "Chat:01".into(),
                facet: Some("facets/aa/facets/bb".into()),
                incarnation: 0,
            }
        );
    }

    #[test]
    fn captured_files_keep_their_wal_range() {
        let file = celld_ltx::CapturedFile {
            txid: celld_ltx::TXID(7),
            kind: celld_ltx::CaptureKind::Sync,
            page_size: 4096,
            commit: 3,
            full_image: false,
            wal: Some(celld_ltx::WalRange {
                salt1: 1,
                salt2: 2,
                offset: 32,
                size: 2 * (4096 + 24),
            }),
        };
        let capture = capture_of(&file);
        assert_eq!(capture.txid, 7);
        assert_eq!(
            capture.wal,
            Some(CapturedWal {
                generation: WalGeneration { salt1: 1, salt2: 2 },
                offset: 32,
                size: 2 * (4096 + 24),
            })
        );
    }
    #[test]
    fn gap_records_are_bounded_during_sink_outage() {
        let budget = 65536;
        let (exporter, sink, _outcomes) = exporter(budget);
        let (tx, _rx) = mpsc::unbounded_channel();
        let mut stream = Stream::new(
            exporter,
            ("Items:a".into(), 1),
            ActivationLink {
                start_txid: 0,
                prev_epoch: None,
                prev_txid: None,
                mode: ActivationMode::Fresh,
            },
            tx.downgrade(),
        );
        for i in 1..=1000 {
            // The per-commit result when the sink is full but durability keeps succeeding.
            stream.release(vec![Out::Released(Released::Gap {
                after: i - 1,
                through: i,
                unmatched: 0,
                overflowed: 1,
            })]);
        }
        stream.advance();
        assert!(stream.exporter.advances.lock().unwrap().is_empty());
        let held = sink.held.lock().unwrap();
        assert_eq!(held.len(), 1);
        let bytes: usize = held.iter().map(|r| r.record.to_json().len()).sum();
        assert!(
            bytes <= budget,
            "{} queued gap records occupy {bytes} bytes for budget {budget}",
            held.len()
        );
    }

    #[test]
    fn delivery_cache_preserves_same_epoch_counts_after_eviction() {
        let mut states = DeliveryStates::new().unwrap();
        let key = ("Items:a".to_string(), 1, 1);
        let at = Position::new(1, 3, 1);
        let state = states.get(key.clone()).unwrap();
        state.marked = Some(at);
        state.position = Some(at);
        state.acked.insert(Position::new(1, 4, 2), 3);
        for epoch in 2..=1000 {
            states.get(("Items:a".into(), epoch, 1)).unwrap();
        }
        assert_eq!(states.hot.len(), DELIVERY_CACHE_SIZE);
        let resumed = states.get(key).unwrap();
        assert_eq!(resumed.marked, Some(at));
        assert_eq!(resumed.acked[&Position::new(1, 4, 2)], 3);
        resumed.frozen = true;
        for epoch in 1001..=2000 {
            states.get(("Items:a".into(), epoch, 1)).unwrap();
        }
        assert!(states.get(("Items:a".into(), 1, 1)).unwrap().frozen);
    }
}

#[cfg(feature = "export-bench")]
#[doc(hidden)]
#[path = "export_live/bench.rs"]
pub mod bench;
