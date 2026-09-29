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
//! Scope of this first wiring:
//!
//! - root cells only. A facet's stream has its own replication and no gate
//!   residency of its own, so facets are not exported yet;
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
use crate::storage::export_capture::{CapturedCommit, WalStamp};
use celld_export_format::{
    split, Body, BulkBody, Envelope, GapBody, Origin, Position, Record, RowsBody, Split, StreamId,
    TableGen, WatermarkBody,
};
use celld_logic::export::{Attribution, Capture, CapturedWal, Released, WalGeneration, WalPoint};
use celld_logic::RequestError;
use std::collections::{BTreeMap, HashMap, HashSet};
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
    CaughtUp,
    Captured(Capture),
    /// The verdict on a ticket, with the proven TXID read when it settled.
    Proven {
        ticket: u64,
        result: Result<u64, RequestError>,
    },
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
    /// Set while the delivery task handles a batch of results, which can
    /// submit watermarks and gaps of its own.
    delivering: AtomicBool,
    counters: Counters,
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
        // Delivered positions are tracked for one sink; running both at once
        // needs a position per sink.
        anyhow::ensure!(
            !(config.sinks.bucket && config.sinks.blob_stream),
            "CELLD_EXPORT_SINK=bucket,blob-stream is not supported yet; choose one sink"
        );
        let (outcomes_tx, outcomes_rx) = mpsc::unbounded_channel();
        let sink: Arc<dyn ExportSink> = if config.sinks.blob_stream {
            crate::export_blob_stream::start(&config, outcomes_tx)?
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
        let exporter = Exporter::new(config, node, sink, Box::new(ask));
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
    ) -> Arc<Exporter> {
        Arc::new(Exporter {
            node,
            config,
            sink,
            streams: Mutex::new(HashMap::new()),
            ask,
            next_seq: AtomicU64::new(1),
            submitted: Mutex::new(HashMap::new()),
            delivering: AtomicBool::new(false),
            counters: Counters::default(),
        })
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
    pub(crate) fn attach(
        self: &Arc<Self>,
        scope: &str,
        epoch: u64,
        script: &str,
    ) -> Option<CellStream> {
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
        Some(CellStream {
            exporter: self.clone(),
            tx,
            denied,
        })
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
            self.sink.flush();
            crate::asyncrt::sleep(Duration::from_millis(20)).await;
            let unresolved = !self
                .submitted
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

/// About the encoded size of a commit's rows, for the queue budget. An
/// estimate from the values, so the cell thread does not encode twice.
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
    let tables: usize = commit
        .tables
        .iter()
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
    (tables + 64 * commit.bulk.len() + 256) as u64
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
                bytes,
                commit,
            }) => {
                // The reservation moves from the FIFO to the pending list,
                // which `account` charges below.
                exporter.release_queued(bytes);
                if !commit.bulk.is_empty() {
                    counters.bulk_commits.fetch_add(1, Ordering::Relaxed);
                }
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
            Some(Input::Shed { stamp, position }) => {
                match stamp {
                    WalStamp::At {
                        salt1,
                        salt2,
                        frames,
                    } => self.attribution.dropped(WalPoint {
                        generation: WalGeneration { salt1, salt2 },
                        frames,
                    }),
                    WalStamp::Unplaced => self.attribution.unplaced(
                        0,
                        CapturedCommit {
                            seq: 0,
                            committed_at: 0,
                            tables: Vec::new(),
                            bulk: Vec::new(),
                        },
                    ),
                }
                self.wanted = Some(self.wanted.map_or(position, |wanted| wanted.max(position)));
            }
            Some(Input::CaughtUp) => self.attribution.caught_up(),
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
        self.release(released);
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

    /// Turn released commits and gaps into records and submit them.
    fn release(&mut self, released: Vec<Released<CapturedCommit>>) {
        let epoch = self.key.1;
        let max_record = self.exporter.config.max_record_bytes;
        let mut out: Vec<(Record, Submitted)> = Vec::new();
        for entry in released {
            let number = self.next_commit;
            self.next_commit += 1;
            let mut records = Vec::new();
            match entry {
                Released::Commit { label, payload } => {
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
                Released::Gap {
                    after,
                    through,
                    unmatched,
                    overflowed,
                } => {
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
        exporter.delivering.store(true, Ordering::SeqCst);
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
        exporter.delivering.store(false, Ordering::SeqCst);
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
                    envelope: exporter.envelope(&stream, position, 0),
                    body: Body::Rows(RowsBody {
                        data: commit_of(8).tables.remove(0),
                    }),
                },
                Submitted {
                    key: ("Items:a".to_string(), 1),
                    stream,
                    position,
                    whole: true,
                    closes: true,
                    watermark: false,
                },
            )]);
            exporter.close().await;
            assert_eq!(*sink.log.lock().unwrap(), ["rows", "watermark", "close"]);
            assert_eq!(exporter.state()["watermarks"], 1);
        });
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
