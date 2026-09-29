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
//! Scope of this first wiring:
//!
//! - root cells only. A facet's stream has its own replication and no gate
//!   residency of its own, so facets are not exported yet; their `deleted`
//!   records are, on the root's stream, naming the facet by
//!   [`facet_path`];
//! - the stream's incarnation is 0 and `cell_name` is absent, until
//!   activation links (which know a cell's first epoch) land;
//! - the shared queue budget is applied to the pending lists. The sink's
//!   buffer counts against it but is bounded by its own early flush, so the
//!   pending commits are what is shed;
//! - commits pulled after the residency's last ticket can no longer be
//!   proven (the cell is leaving) are not released; the next activation's
//!   link or the reconciler reports that tail.

use crate::bucket::Bucket;
use crate::export::Config;
use crate::export_sink::{
    BucketSink, BucketSinkConfig, Delivery as SinkDelivery, ExportSink, Outcome, Retention,
    SinkRecord,
};
use crate::facet_streams::FacetDeleted;
use crate::storage::export_capture::{CapturedCommit, WalStamp};
use celld_export_format::{
    split, Body, BulkBody, DeletedBody, Envelope, GapBody, Origin, Position, Record, RowsBody,
    Split, StreamId, TableGen, WatermarkBody,
};
use celld_logic::export::{Attribution, Capture, CapturedWal, Released, WalGeneration, WalPoint};
use celld_logic::RequestError;
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
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
/// as [`celld_logic::Event::ExportTicket`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TicketAsk {
    pub cell: String,
    pub epoch: u64,
    pub position: u64,
    pub ticket: u64,
}

/// One input on a stream's FIFO.
enum Input {
    /// The script of the cell's deployment, sent when the cell's
    /// connection attaches.
    Identity {
        script: String,
    },
    /// A pulled commit and the committed-write position after it.
    Commit {
        stamp: WalStamp,
        position: u64,
        commit: CapturedCommit,
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

/// The cell thread's handle on its stream.
#[derive(Clone)]
pub(crate) struct CellStream {
    tx: mpsc::UnboundedSender<Input>,
    denied: HashSet<String>,
}

impl CellStream {
    /// Tables of this cell's class the operator denied.
    pub(crate) fn denied_tables(&self) -> HashSet<String> {
        self.denied.clone()
    }

    pub(crate) fn commit(&self, stamp: WalStamp, position: u64, commit: CapturedCommit) {
        let _ = self.tx.send(Input::Commit {
            stamp,
            position,
            commit,
        });
    }

    pub(crate) fn caught_up(&self) {
        let _ = self.tx.send(Input::CaughtUp);
    }
}

/// Counters for `/state`.
#[derive(Default)]
struct Counters {
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
    position: Position,
    /// The last fragment of a record: the record counts once when it lands.
    whole: bool,
    /// The last record of a commit (or a gap): the delivered position may
    /// move to it.
    closes: bool,
    watermark: bool,
}

pub struct Exporter {
    node: String,
    config: Config,
    sink: Arc<dyn ExportSink>,
    streams: Mutex<HashMap<(String, u64), mpsc::WeakUnboundedSender<Input>>>,
    ask: Box<dyn Fn(TicketAsk) + Send + Sync>,
    next_seq: AtomicU64,
    submitted: Mutex<HashMap<u64, Submitted>>,
    counters: Counters,
}

impl Exporter {
    /// Start the bucket sink and the delivery task, and install the
    /// exporter for the node. `ask` hands a ticket to the actor. Must run
    /// inside the node's runtime, before any cell activates.
    pub fn start(
        config: Config,
        bucket: Bucket,
        node: String,
        ask: impl Fn(TicketAsk) + Send + Sync + 'static,
    ) -> anyhow::Result<Arc<Exporter>> {
        anyhow::ensure!(
            !config.sinks.blob_stream,
            "CELLD_EXPORT_SINK=blob-stream is not available in this build yet; use bucket"
        );
        let (outcomes_tx, outcomes_rx) = mpsc::unbounded_channel();
        let sink = BucketSink::start(
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
        );
        let exporter = Arc::new(Exporter {
            node,
            config,
            sink: Arc::new(sink),
            streams: Mutex::new(HashMap::new()),
            ask: Box::new(ask),
            next_seq: AtomicU64::new(1),
            submitted: Mutex::new(HashMap::new()),
            counters: Counters::default(),
        });
        EXPORTER
            .set(exporter.clone())
            .map_err(|_| anyhow::anyhow!("the change exporter is already installed"))?;
        crate::asyncrt::spawn(deliver(exporter.clone(), outcomes_rx));
        Ok(exporter)
    }

    /// Capture settings for every isolate on the node.
    pub(crate) fn capture_settings(&self) -> crate::storage::export_capture::Settings {
        crate::storage::export_capture::Settings {
            max_tx_bytes: self.config.max_tx_bytes as u64,
        }
    }

    /// Whether `cell` is exported: a root cell of an exported class.
    fn exports(&self, cell: &str) -> bool {
        if cell.contains("/facets/") {
            return false;
        }
        let class = cell.split_once(':').map_or(cell, |(class, _)| class);
        self.config.exports_class(class)
    }

    /// Open the stream of one residency when its replica opens, and return
    /// the capture observer that feeds it. `None` when the cell is not
    /// exported.
    pub(crate) fn open_stream(
        self: &Arc<Self>,
        cell: &str,
        epoch: u64,
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
                    let stream = Stream::new(self.clone(), key, tx.downgrade());
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
    /// its replica opened one.
    pub(crate) fn attach(&self, scope: &str, epoch: u64, script: &str) -> Option<CellStream> {
        let tx = self
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(scope.to_string(), epoch))
            .and_then(mpsc::WeakUnboundedSender::upgrade)?;
        let _ = tx.send(Input::Identity {
            script: script.to_string(),
        });
        let class = scope.split_once(':').map_or(scope, |(class, _)| class);
        let denied = self
            .config
            .denied_tables
            .iter()
            .filter(|(denied_class, _)| denied_class == class)
            .map(|(_, table)| table.clone())
            .collect();
        Some(CellStream { tx, denied })
    }

    /// The actor's verdict on a ticket.
    pub(crate) fn proven(
        &self,
        cell: &str,
        epoch: u64,
        ticket: u64,
        result: Result<u64, RequestError>,
    ) {
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

    /// A facet delete that succeeded, for the root's stream at `epoch`.
    /// Nothing is recorded when the root is not exported or its stream is
    /// gone; the reconciler reports a consumer stream with no bucket prefix.
    pub(crate) fn facet_deleted(&self, root: &str, epoch: u64, deleted: &FacetDeleted) {
        let Some(path) = facet_path(root, &deleted.stream) else {
            tracing::warn!(root, stream = %deleted.stream, "export: a facet delete names no facet of its root");
            return;
        };
        let tx = self
            .streams
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&(root.to_string(), epoch))
            .and_then(mpsc::WeakUnboundedSender::upgrade);
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

    /// Flush the sink and wait for its results. Called at shutdown.
    pub async fn close(&self) {
        self.sink.close().await;
    }

    /// The `export` object of `/state`.
    pub fn state(&self) -> serde_json::Value {
        let c = &self.counters;
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        serde_json::json!({
            "queue_bytes": load(&c.pending_bytes) + self.sink.buffered_bytes(),
            "pending_commits": load(&c.pending_commits),
            "dropped_records": load(&c.dropped_records),
            "gaps": load(&c.gaps),
            "bulk_commits": load(&c.bulk_commits),
            "attribution_mismatches": load(&c.attribution_mismatches),
            "delivered_records": load(&c.delivered_records),
            "watermarks": load(&c.watermarks),
        })
    }

    /// Submit records in order, registering what delivery needs first.
    fn submit(&self, records: Vec<(Record, Submitted)>) {
        if records.is_empty() {
            return;
        }
        let mut batch = Vec::with_capacity(records.len());
        {
            let mut submitted = self.submitted.lock().unwrap_or_else(|e| e.into_inner());
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

    fn envelope(&self, stream: &StreamId, position: Position, committed_at: i64) -> Envelope {
        Envelope {
            stream: stream.clone(),
            cell_name: None,
            position,
            committed_at,
            node: self.node.clone(),
            origin: Origin::Live,
            fragment: 1,
            fragments: 1,
        }
    }
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

/// Encoded size of a commit's rows, for the queue budget.
fn encoded_bytes(commit: &CapturedCommit) -> u64 {
    let rows: usize = commit
        .tables
        .iter()
        .map(|table| serde_json::to_vec(table).map_or(0, |bytes| bytes.len()))
        .sum();
    (rows + 64 * commit.bulk.len() + 256) as u64
}

/// The attribution and release of one cell residency.
struct Stream {
    exporter: Arc<Exporter>,
    key: (String, u64),
    identity: StreamId,
    attribution: Attribution<CapturedCommit>,
    /// Handed out as `commit` in positions, in release order.
    next_commit: u64,
    /// The TXID of the newest released position, so a gap never sorts
    /// before a commit released ahead of it.
    last_txid: u64,
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
    sequencer: Sequencer<CapturedCommit>,
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
        this: mpsc::WeakUnboundedSender<Input>,
    ) -> Self {
        let (cell, _) = &key;
        let class = cell
            .split_once(':')
            .map_or(cell.as_str(), |(class, _)| class);
        let identity = StreamId {
            script: String::new(),
            class: class.to_string(),
            cell: cell.clone(),
            facet: None,
            incarnation: 0,
        };
        Stream {
            exporter,
            key,
            identity,
            attribution: Attribution::new(),
            next_commit: 1,
            last_txid: 0,
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
            Some(Input::Identity { script }) => self.identity.script = script,
            Some(Input::Commit {
                stamp,
                position,
                commit,
            }) => {
                if !commit.bulk.is_empty() {
                    counters.bulk_commits.fetch_add(1, Ordering::Relaxed);
                }
                let bytes = encoded_bytes(&commit);
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
                        commit,
                    ),
                    WalStamp::Unplaced => self.attribution.unplaced(bytes, commit),
                }
                self.wanted = Some(self.wanted.map_or(position, |wanted| wanted.max(position)));
            }
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
        let ticket = self.next_ticket;
        self.next_ticket += 1;
        self.outstanding = Some((ticket, position));
        self.keepalive = self.this.upgrade();
        (self.exporter.ask)(TicketAsk {
            cell: self.key.0.clone(),
            epoch: self.key.1,
            position,
            ticket,
        });
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

    /// Turn released commits, gaps, and facet deletes into records and
    /// submit them.
    fn release(&mut self, released: Vec<Out<CapturedCommit>>) {
        let epoch = self.key.1;
        let max_record = self.exporter.config.max_record_bytes;
        let mut out: Vec<(Record, Submitted)> = Vec::new();
        for entry in released {
            let number = self.next_commit;
            self.next_commit += 1;
            let mut records = Vec::new();
            match entry {
                Out::Deleted(delete) => {
                    let position = Position::new(epoch, self.last_txid, number);
                    records.push(Record {
                        envelope: self
                            .exporter
                            .envelope(&self.identity, position, delete.at_ms),
                        body: Body::Deleted(delete.body),
                    });
                }
                Out::Released(Released::Commit { label, payload }) => {
                    self.last_txid = self.last_txid.max(label);
                    let position = Position::new(epoch, label, number);
                    let envelope =
                        self.exporter
                            .envelope(&self.identity, position, payload.committed_at);
                    let mut bulk: Vec<TableGen> = payload.bulk;
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
                    self.exporter.counters.gaps.fetch_add(1, Ordering::Relaxed);
                    let position = Position::new(epoch, self.last_txid.max(after), number);
                    records.push(Record {
                        envelope: self.exporter.envelope(
                            &self.identity,
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
            for (index, record) in records.into_iter().enumerate() {
                let meta = Submitted {
                    key: self.key.clone(),
                    stream: self.identity.clone(),
                    position: record.envelope.position,
                    whole: record.envelope.fragment == record.envelope.fragments,
                    closes: index == last,
                    watermark: false,
                };
                out.push((record, meta));
            }
        }
        self.exporter.submit(out);
    }
}

/// What delivery knows about one stream.
#[derive(Default)]
struct Delivered {
    stream: Option<StreamId>,
    /// The newest position whose commit is wholly acknowledged.
    position: Option<Position>,
    /// The `through` of the last watermark submitted.
    marked: Option<Position>,
    /// Whole records acknowledged since the last watermark, per position.
    acked: BTreeMap<Position, u64>,
    frozen: bool,
}

/// Read the sink's results, advance delivered positions, and submit
/// watermarks and gaps.
async fn deliver(exporter: Arc<Exporter>, mut outcomes: mpsc::UnboundedReceiver<Outcome>) {
    let mut streams: BTreeMap<(String, u64), Delivered> = BTreeMap::new();
    while let Some(outcome) = outcomes.recv().await {
        let mut follow_up: Vec<(Record, Submitted)> = Vec::new();
        for (seq, result) in outcome.results {
            let Some(meta) = exporter
                .submitted
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .remove(&seq)
            else {
                continue;
            };
            let state = streams.entry(meta.key.clone()).or_default();
            state.stream.get_or_insert_with(|| meta.stream.clone());
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
        for (key, state) in &mut streams {
            let (Some(through), Some(stream)) = (state.position, state.stream.clone()) else {
                continue;
            };
            if state.frozen || state.marked.is_some_and(|marked| marked >= through) {
                continue;
            }
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
                    envelope: exporter.envelope(&stream, through, crate::asyncrt::wall_ms()),
                    body: Body::Watermark(body),
                },
                Submitted {
                    key: key.clone(),
                    stream,
                    position: through,
                    whole: false,
                    closes: false,
                    watermark: true,
                },
            ));
            state.marked = Some(through);
        }
        exporter.submit(follow_up);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
