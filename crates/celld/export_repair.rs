// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Snapshot emission for `celld export repair` and `celld export backfill`
//! (`docs/design/change-export.md`, "Snapshots and repair").
//!
//! Repair does not diff states. It restores a stream from the bucket with
//! [`crate::export_restore`] and emits, all at the position it restored and
//! with `origin: repair`, a `schema` record for every exported table, one
//! `snapshot` record per table (split into fragments), and a `snapshot_end`
//! with scope `stream`. By the precedence rule a consumer replaces the
//! stream's state with the snapshot, deleting rows it lacks. Backfill is the
//! same code run at the head of every stream it is given.
//!
//! **The position reached, not the one asked for.** The restorable
//! positions are the cuts the bucket holds, and the bucket's head is not the
//! fleet's head: unfolded `log/` tails and unflushed rows are in no per-cell
//! object (plan C16). So a job restores at the first cut at or after its
//! target, falls back to the bucket head when the bucket holds nothing that
//! far, and its [`Report`] says what it reached and whether that covers the
//! target. Anything newer is left to the next live change or the
//! reconciler; nothing here claims a stream is whole past what it restored.
//!
//! **Positions.** The snapshot's position is `(epoch, txid, u64::MAX)`: a
//! cut at a txid holds every commit of that txid, so the snapshot must sort
//! after each of them, which a live commit's `commit` counter never reaches.
//! This is the same bound the live path gives a gap's end.
//!
//! **Identity.** A snapshot is written to the stream the image belongs to:
//! its incarnation and `cell_name` come from the image's `_cf_METADATA`,
//! where the live path keeps them. An image from before stream
//! incarnations has none, and takes [`ROOT_INCARNATION`] as the live path
//! did then. A cell that has never opened with export on has no stream yet
//! and is skipped.
//!
//! **Not yet.** Table generations are the capture's first generation until
//! piece 11 records real ones, `_cf_KV` is snapshotted raw as the live path
//! exports it until piece 12, facets are refused because they are not
//! exported yet, and tombstoned streams (piece 16) are not checked.
#![allow(clippy::disallowed_methods)] // Offline operator path, outside Actor execution.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, ensure, Context as _};
use celld_export_format::{
    Body, ColumnDef, Envelope, Origin, Position, Record, RowChange, SchemaBody, SnapshotBody,
    SnapshotEndBody, SnapshotScope, StreamId, TableGen, TableRows,
};
use futures_util::StreamExt as _;
use rusqlite::{Connection, OptionalExtension as _};
use serde::Serialize;
use tokio::sync::{mpsc, Notify};

use crate::bucket::Bucket;
use crate::export_restore::{self, Pace, Restored, Stream, Target};
use crate::export_sink::{Delivery, ExportSink, Outcome, SinkRecord};
use crate::storage::export_capture::{exported_table, table_scan, FIRST_GENERATION};

/// The `commit` of every repair record: after each commit of its txid.
pub const REPAIR_COMMIT: u64 = u64::MAX;

/// The incarnation of a root cell's stream in an image that records none:
/// what the live path stamped before stream incarnations.
pub const ROOT_INCARNATION: u64 = 0;

/// Records a job may have produced but not yet handed to the sink.
const RECORDS_IN_FLIGHT: usize = 64;

/// How snapshots are written.
#[derive(Clone, Debug)]
pub struct Settings {
    /// The `node` of every record, and the bucket sink's object prefix.
    pub node: String,
    /// `CELLD_EXPORT_MAX_RECORD_BYTES`.
    pub max_record_bytes: usize,
    /// `CELLD_EXPORT_TABLES` as `(class, table)`.
    pub denied_tables: BTreeSet<(String, String)>,
    /// Streams restored at once.
    pub concurrency: usize,
    /// Encoded bytes the sink may hold before a job waits for it to write.
    pub buffer_bytes: u64,
}

/// One stream to snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    /// The stream. Its incarnation is a default the image overrides unless
    /// `pin_incarnation` is set.
    pub stream: StreamId,
    pub target: Target,
    /// Why the stream is here: the gap kinds that named it, or `backfill`.
    pub reasons: BTreeSet<String>,
    /// The incarnation came from the consumer, as a gaps row's does: the
    /// image must hold that incarnation, or the job is skipped rather than
    /// writing a snapshot into a stream the image did not produce.
    pub pin_incarnation: bool,
}

/// What one job did.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Report {
    pub script: String,
    pub class: String,
    pub cell: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facet: Option<String>,
    pub incarnation: u64,
    pub reasons: Vec<String>,
    pub status: Status,
    /// The target position, `e<epoch>:<txid>`, or `head`.
    pub target: String,
    /// The position the snapshot carries: what the restore reached.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reached: Option<Position>,
    /// The newest cut in the bucket when the restore planned.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bucket_head: Option<Position>,
    /// Whether `reached` is at or after the target. `false` means the bucket
    /// does not hold the target yet: the snapshot replaces state through
    /// `reached`, and the rest waits for the next live change, the next
    /// flush, or the reconciler.
    pub covers_target: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_id: Option<String>,
    pub tables: u64,
    pub rows: u64,
    /// Records handed to the sink, fragments counted one by one.
    pub records: u64,
    /// Rows larger than the record limit on their own. Each rides alone in
    /// an oversized fragment, since a snapshot cannot mark a table unknown.
    pub oversized_rows: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Status {
    /// Every record is durable in the sink.
    Written,
    /// Nothing was written, for the reason in `error`.
    Skipped,
    /// The restore or the scan failed, or the sink dropped a record; the
    /// snapshot is incomplete and a consumer ignores it.
    Failed,
}

impl Report {
    fn new(job: &Job) -> Self {
        Report {
            script: job.stream.script.clone(),
            class: job.stream.class.clone(),
            cell: job.stream.cell.clone(),
            facet: job.stream.facet.clone(),
            incarnation: job.stream.incarnation,
            reasons: job.reasons.iter().cloned().collect(),
            status: Status::Failed,
            target: match job.target {
                Target::Head => "head".to_string(),
                Target::AtOrAfter(p) => p.to_string(),
            },
            reached: None,
            bucket_head: None,
            covers_target: false,
            snapshot_id: None,
            tables: 0,
            rows: 0,
            records: 0,
            oversized_rows: 0,
            error: None,
        }
    }
}

/// A root cell's export stream identity, as the live path gives it.
pub fn root_stream(script: &str, scope: &str) -> anyhow::Result<StreamId> {
    Stream::cell(scope)?;
    Ok(StreamId {
        script: script.to_string(),
        class: class_of(scope).to_string(),
        cell: scope.to_string(),
        facet: None,
        incarnation: ROOT_INCARNATION,
    })
}

/// The class of a cell scope: what precedes the first `:`, or the whole
/// scope for a bare instance.
pub fn class_of(scope: &str) -> &str {
    scope.split_once(':').map_or(scope, |(class, _)| class)
}

// ---- The gaps list --------------------------------------------------------

/// One row of the consumer's `EXPORT_GAPS` view, unloaded as JSON lines.
/// Snowflake unloads column names in upper case and may quote numbers, so
/// both are accepted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GapRow {
    pub stream: StreamId,
    /// `gap`, `link`, `recovered`, `bulk`, or `reconciler`.
    pub kind: String,
    /// The position the stream must be whole through, when the row has one.
    /// A `bulk` row has none: its table is unknown until a snapshot after
    /// the `bulk` record, which only the head can promise.
    pub bound: Option<export_restore::Position>,
}

/// Parse an `EXPORT_GAPS` unload: one JSON object per line.
pub fn parse_gaps(text: &str) -> anyhow::Result<Vec<GapRow>> {
    text.lines()
        .enumerate()
        .filter(|(_, line)| !line.trim().is_empty())
        .map(|(n, line)| parse_gap(line).with_context(|| format!("gaps list line {}", n + 1)))
        .collect()
}

fn parse_gap(line: &str) -> anyhow::Result<GapRow> {
    let value: serde_json::Value = serde_json::from_str(line)?;
    let object = value.as_object().context("not a JSON object")?;
    let fields: HashMap<String, &serde_json::Value> = object
        .iter()
        .map(|(k, v)| (k.to_ascii_lowercase(), v))
        .collect();
    let text = |name: &str| -> Option<String> {
        match fields.get(name)? {
            serde_json::Value::String(s) => Some(s.clone()),
            serde_json::Value::Null => None,
            other => Some(other.to_string()),
        }
    };
    let number = |name: &str| -> anyhow::Result<Option<u64>> {
        match fields.get(name).copied() {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::Number(n)) => n
                .as_u64()
                .map(Some)
                .with_context(|| format!("{name} is not a position number: {n}")),
            Some(serde_json::Value::String(s)) => s
                .parse()
                .map(Some)
                .with_context(|| format!("{name} is not a position number: {s:?}")),
            Some(other) => bail!("{name} is not a number: {other}"),
        }
    };
    let required = |name: &str| text(name).with_context(|| format!("missing {name}"));
    let cell = required("cell")?;
    let class = required("class")?;
    ensure!(
        class_of(&cell) == class,
        "cell {cell:?} is not of class {class:?}"
    );
    let stream = StreamId {
        script: required("script")?,
        class,
        cell,
        // The consumer stores a root's facet as '' so streams join by
        // equality.
        facet: text("facet").filter(|f| !f.is_empty()),
        incarnation: number("incarnation")?.context("missing incarnation")?,
    };
    let bound = match (number("bound_epoch")?, number("bound_txid")?) {
        (Some(epoch), Some(txid)) => Some(export_restore::Position { epoch, txid }),
        _ => None,
    };
    Ok(GapRow {
        stream,
        kind: required("gap_kind")?.to_ascii_lowercase(),
        bound,
    })
}

/// One job per stream the gaps list names. With `at_head` every job
/// restores the head (backfill); otherwise a stream restores at or after the
/// highest bound its rows carry, or at the head when any row has none.
pub fn jobs_from_gaps(rows: &[GapRow], at_head: bool) -> Vec<Job> {
    let mut by_stream: BTreeMap<
        &StreamId,
        (BTreeSet<String>, Option<export_restore::Position>, bool),
    > = BTreeMap::new();
    for row in rows {
        let (reasons, bound, unbounded) = by_stream.entry(&row.stream).or_default();
        reasons.insert(row.kind.clone());
        match row.bound {
            Some(b) => *bound = (*bound).max(Some(b)),
            None => *unbounded = true,
        }
    }
    by_stream
        .into_iter()
        .map(|(stream, (reasons, bound, unbounded))| Job {
            stream: stream.clone(),
            target: match bound {
                Some(bound) if !at_head && !unbounded => Target::AtOrAfter(bound),
                _ => Target::Head,
            },
            reasons,
            pin_incarnation: true,
        })
        .collect()
}

// ---- Snapshot emission -----------------------------------------------------

/// What an image says about its own stream, from `_cf_METADATA`.
#[derive(Debug, Default, PartialEq, Eq)]
struct ImageIdentity {
    /// `None` when the image predates stream incarnations; `Some(None)` when
    /// the cell has not opened with export on since they arrived.
    incarnation: Option<Option<u64>>,
    cell_name: Option<String>,
}

fn image_identity(db: &Connection, scope: &str) -> anyhow::Result<ImageIdentity> {
    let columns: Vec<String> = db
        .prepare("SELECT name FROM pragma_table_info('_cf_METADATA', 'main')")?
        .query_map([], |row| row.get(0))?
        .collect::<Result<_, _>>()?;
    let has = |name: &str| columns.iter().any(|c| c == name);
    if !has("scope") {
        return Ok(ImageIdentity::default());
    }
    let name = if has("actor_name") {
        "actor_name"
    } else {
        "NULL"
    };
    let incarnation = if has("incarnation") {
        "incarnation"
    } else {
        "NULL"
    };
    let row = db
        .query_row(
            &format!("SELECT {name}, {incarnation} FROM main._cf_METADATA WHERE scope = ?1"),
            [scope],
            |row| {
                Ok((
                    row.get::<_, Option<String>>(0)?,
                    row.get::<_, Option<i64>>(1)?,
                ))
            },
        )
        .optional()?;
    let (cell_name, stored) = row.unwrap_or((None, None));
    let stored = stored.map(u64::try_from).transpose()?;
    Ok(ImageIdentity {
        incarnation: has("incarnation").then_some(stored),
        cell_name,
    })
}

/// The stream a job's snapshot is written to, or why it is skipped.
fn resolve_identity(job: &Job, image: &ImageIdentity) -> Result<StreamId, String> {
    let mut stream = job.stream.clone();
    // An image from before stream incarnations belongs to the stream the
    // live path stamped then.
    match image.incarnation.unwrap_or(Some(ROOT_INCARNATION)) {
        None => {
            return Err(
                "the cell has not opened with export on, so it has no stream yet; \
                 backfill it once it has"
                    .to_string(),
            )
        }
        Some(held) if job.pin_incarnation && held != stream.incarnation => {
            return Err(format!(
                "the bucket holds incarnation {held} of this cell, not {}",
                stream.incarnation
            ))
        }
        Some(held) => stream.incarnation = held,
    }
    Ok(stream)
}

/// What the blocking scan did.
enum Scanned {
    Done(Counts, StreamId),
    Skipped(String),
}

/// What one stream's snapshot is: its identity, position and id.
struct Snapshot<'a> {
    stream: &'a StreamId,
    position: Position,
    snapshot_id: String,
    cell_name: Option<String>,
    committed_at: i64,
    settings: &'a Settings,
}

#[derive(Default)]
struct Counts {
    tables: u64,
    rows: u64,
    records: u64,
    oversized_rows: u64,
}

impl Snapshot<'_> {
    fn record(&self, body: Body, fragment: u32, fragments: u32) -> Record {
        Record {
            envelope: Envelope {
                stream: self.stream.clone(),
                cell_name: self.cell_name.clone(),
                position: self.position,
                committed_at: self.committed_at,
                node: self.settings.node.clone(),
                origin: Origin::Repair,
                fragment,
                fragments,
            },
            body,
        }
    }

    /// Emit the whole snapshot of `db` through `emit`, in order: schemas,
    /// then one snapshot record per table, then the end.
    fn emit(
        &self,
        db: &Connection,
        emit: &mut dyn FnMut(Record) -> anyhow::Result<()>,
    ) -> anyhow::Result<Counts> {
        let mut counts = Counts::default();
        let tables = self.tables(db)?;
        for table in &tables {
            let schema = schema_of(db, table)?;
            emit(self.record(Body::Schema(schema), 1, 1))?;
            counts.records += 1;
        }
        for table in &tables {
            self.emit_table(db, table, emit, &mut counts)?;
            counts.tables += 1;
        }
        let end = SnapshotEndBody {
            snapshot_id: self.snapshot_id.clone(),
            scope: SnapshotScope::Stream,
            tables: tables
                .iter()
                .map(|table| TableGen {
                    table: table.clone(),
                    generation: FIRST_GENERATION,
                })
                .collect(),
            records: tables.len() as u64,
        };
        emit(self.record(Body::SnapshotEnd(end), 1, 1))?;
        counts.records += 1;
        Ok(counts)
    }

    /// The exported tables of the image, as capture chooses them: ordinary
    /// tables of `main`, not virtual or shadow, not internal, not denied.
    fn tables(&self, db: &Connection) -> anyhow::Result<Vec<String>> {
        let mut statement = db.prepare("PRAGMA table_list")?;
        let mut rows = statement.query([])?;
        let mut tables = Vec::new();
        while let Some(row) = rows.next()? {
            let (schema, name, kind): (String, String, String) =
                (row.get(0)?, row.get(1)?, row.get(2)?);
            if schema == "main"
                && kind == "table"
                && exported_table(&name)
                && !self
                    .settings
                    .denied_tables
                    .contains(&(self.stream.class.clone(), name.clone()))
            {
                tables.push(name);
            }
        }
        tables.sort();
        Ok(tables)
    }

    /// One table as one `snapshot` record, split by rows under the record
    /// limit. The fragment count must be known before the first fragment,
    /// so a first scan measures and a second emits; the image never changes
    /// between them, and only one fragment is held at a time.
    fn emit_table(
        &self,
        db: &Connection,
        table: &str,
        emit: &mut dyn FnMut(Record) -> anyhow::Result<()>,
        counts: &mut Counts,
    ) -> anyhow::Result<()> {
        let scan = table_scan(db, table)?;
        let data = |rows: Vec<RowChange>| TableRows {
            table: table.to_string(),
            generation: FIRST_GENERATION,
            columns: scan.columns.clone(),
            key_columns: scan.key_columns.clone(),
            rows,
        };
        let body = |rows: Vec<RowChange>| {
            Body::Snapshot(SnapshotBody {
                snapshot_id: self.snapshot_id.clone(),
                data: data(rows),
            })
        };
        let base = self
            .record(body(Vec::new()), u32::MAX, u32::MAX)
            .to_json()
            .len();
        let max = self.settings.max_record_bytes;

        let mut chunks: Vec<u64> = Vec::new();
        let (mut current, mut size) = (0u64, base);
        let total = scan.for_each(db, |row| {
            let len = serde_json::to_vec(&row)?.len();
            if base + len > max {
                counts.oversized_rows += 1;
            }
            if current > 0 && size + 1 + len > max {
                chunks.push(current);
                (current, size) = (0, base);
            }
            size += len + usize::from(current > 0);
            current += 1;
            Ok(())
        })?;
        // An empty table is still one record, so the snapshot names it.
        if current > 0 || chunks.is_empty() {
            chunks.push(current);
        }
        let fragments = u32::try_from(chunks.len()).context("fragment count overflows")?;

        let mut rows = Vec::new();
        let mut chunk = 0usize;
        let mut emitted = 0u64;
        let mut emit_chunk = |rows: Vec<RowChange>, chunk: usize| {
            emit(self.record(body(rows), chunk as u32 + 1, fragments))
        };
        let seen = scan.for_each(db, |row| {
            rows.push(row);
            if rows.len() as u64 == chunks[chunk] {
                emit_chunk(std::mem::take(&mut rows), chunk)?;
                chunk += 1;
                emitted += 1;
            }
            Ok(())
        })?;
        ensure!(
            seen == total,
            "{table} changed between scans of an immutable image"
        );
        if emitted < u64::from(fragments) {
            // Only the empty table's single empty fragment is left.
            emit_chunk(std::mem::take(&mut rows), chunk)?;
        }
        counts.rows += total;
        counts.records += u64::from(fragments);
        Ok(())
    }
}

/// The `schema` record of a table at the capture's generation.
fn schema_of(db: &Connection, table: &str) -> anyhow::Result<SchemaBody> {
    let sql: String = db.query_row(
        "SELECT sql FROM main.sqlite_schema WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    let mut statement = db.prepare(
        "SELECT name, type, pk, \"notnull\", hidden FROM pragma_table_xinfo(?1, 'main')",
    )?;
    let columns = statement
        .query_map([table], |row| {
            let hidden: i64 = row.get(4)?;
            Ok((
                hidden,
                ColumnDef {
                    name: row.get(0)?,
                    decl_type: row.get(1)?,
                    pk: u32::try_from(row.get::<_, i64>(2)?).unwrap_or(0),
                    not_null: row.get::<_, i64>(3)? != 0,
                    generated: matches!(hidden, 2 | 3),
                },
            ))
        })?
        .filter_map(|column| match column {
            // Hidden columns of a virtual table are not the table's.
            Ok((1, _)) => None,
            Ok((_, column)) => Some(Ok(column)),
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SchemaBody {
        table: table.to_string(),
        generation: FIRST_GENERATION,
        sql,
        columns,
        dropped: false,
        renamed_from: None,
        unsupported: false,
    })
}

// ---- Running jobs against a sink -------------------------------------------

/// Which job each submitted record belongs to, and what the sink said.
#[derive(Default)]
struct Tracker {
    state: Mutex<TrackerState>,
    progress: Notify,
}

#[derive(Default)]
struct TrackerState {
    next_seq: u64,
    owner: HashMap<u64, usize>,
    dropped: HashMap<usize, String>,
}

impl Tracker {
    fn register(&self, job: usize, records: Vec<Record>) -> Vec<SinkRecord> {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        records
            .into_iter()
            .map(|record| {
                state.next_seq += 1;
                let seq = state.next_seq;
                state.owner.insert(seq, job);
                SinkRecord { seq, record }
            })
            .collect()
    }

    fn resolve(&self, outcome: Outcome) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for (seq, delivery) in outcome.results {
            let Some(job) = state.owner.remove(&seq) else {
                continue;
            };
            if let Delivery::Dropped { reason } = delivery {
                state
                    .dropped
                    .entry(job)
                    .or_insert_with(|| reason.to_string());
            }
        }
        drop(state);
        self.progress.notify_waiters();
    }

    fn forget(&self, seqs: &[u64]) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for seq in seqs {
            state.owner.remove(seq);
        }
    }

    /// The sink's verdict on a job once every record has a result: `None`
    /// when all were acknowledged.
    fn verdict(&self, job: usize) -> Option<String> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(reason) = state.dropped.get(&job) {
            return Some(format!("the sink dropped a record: {reason}"));
        }
        state
            .owner
            .values()
            .any(|owner| *owner == job)
            .then(|| "the sink closed before every record had a result".to_string())
    }
}

/// Snapshot every job's stream from `source` into `sink`, `concurrency` at a
/// time, and report on each once the sink has written everything. `sink`'s
/// results must arrive on `outcomes`. `pace` limits the restore's bucket
/// requests across all jobs. Reports come back in job order.
pub async fn run(
    source: &Bucket,
    sink: Arc<dyn ExportSink>,
    mut outcomes: mpsc::UnboundedReceiver<Outcome>,
    jobs: Vec<Job>,
    settings: &Settings,
    pace: Option<Arc<Pace>>,
    mut progress: impl FnMut(&Report),
) -> Vec<Report> {
    let tracker = Arc::new(Tracker::default());
    let collector = {
        let tracker = tracker.clone();
        tokio::spawn(async move {
            while let Some(outcome) = outcomes.recv().await {
                tracker.resolve(outcome);
            }
        })
    };

    let mut reports: Vec<(usize, Report)> = futures_util::stream::iter(jobs.iter().enumerate())
        .map(|(index, job)| {
            let (sink, tracker, pace) = (sink.clone(), tracker.clone(), pace.clone());
            async move {
                let report = run_job(source, &sink, &tracker, index, job, settings, pace).await;
                (index, report)
            }
        })
        .buffer_unordered(settings.concurrency.max(1))
        .collect()
        .await;

    sink.close().await;
    // Every result has been sent. The channel ends once the sink, or its
    // task, drops the sender.
    drop(sink);
    let _ = collector.await;
    reports.sort_by_key(|(index, _)| *index);
    reports
        .into_iter()
        .map(|(index, mut report)| {
            if report.status == Status::Written {
                if let Some(error) = tracker.verdict(index) {
                    report.status = Status::Failed;
                    report.error = Some(error);
                }
            }
            progress(&report);
            report
        })
        .collect()
}

async fn run_job(
    source: &Bucket,
    sink: &Arc<dyn ExportSink>,
    tracker: &Arc<Tracker>,
    index: usize,
    job: &Job,
    settings: &Settings,
    pace: Option<Arc<Pace>>,
) -> Report {
    let mut report = Report::new(job);
    if crate::export::is_never_exported(&job.stream.class) {
        report.status = Status::Skipped;
        report.error = Some(format!("class {} is never exported", job.stream.class));
        return report;
    }
    if job.stream.facet.is_some() {
        report.status = Status::Skipped;
        report.error = Some("facet streams are not exported yet".to_string());
        return report;
    }
    match snapshot_job(
        source,
        sink,
        tracker,
        index,
        job,
        settings,
        pace,
        &mut report,
    )
    .await
    {
        Ok(None) => report.status = Status::Written,
        Ok(Some(reason)) => {
            report.status = Status::Skipped;
            report.error = Some(reason);
        }
        Err(error) => {
            report.status = Status::Failed;
            report.error = Some(format!("{error:#}"));
        }
    }
    report
}

#[allow(clippy::too_many_arguments)]
async fn snapshot_job(
    source: &Bucket,
    sink: &Arc<dyn ExportSink>,
    tracker: &Arc<Tracker>,
    index: usize,
    job: &Job,
    settings: &Settings,
    pace: Option<Arc<Pace>>,
    report: &mut Report,
) -> anyhow::Result<Option<String>> {
    let stream = Stream::cell(&job.stream.cell)?;
    let restored = restore(source, &stream, job.target, pace).await?;
    let reached = Position::new(
        restored.position.epoch,
        restored.position.txid,
        REPAIR_COMMIT,
    );
    report.reached = Some(reached);
    report.bucket_head = Some(Position::new(
        restored.bucket_head.epoch,
        restored.bucket_head.txid,
        REPAIR_COMMIT,
    ));
    report.covers_target = match job.target {
        Target::Head => true,
        Target::AtOrAfter(at) => restored.position >= at,
    };
    let snapshot_id = snapshot_id(reached);
    report.snapshot_id = Some(snapshot_id.clone());

    // The scan is synchronous SQLite work: it runs on a blocking thread
    // and hands records back through a bounded channel.
    let (tx, mut rx) = mpsc::channel::<Record>(RECORDS_IN_FLIGHT);
    let scan = {
        let (job, settings) = (job.clone(), settings.clone());
        tokio::task::spawn_blocking(move || -> anyhow::Result<Scanned> {
            let db = restored.open()?;
            let image = image_identity(&db, &job.stream.cell)?;
            let stream = match resolve_identity(&job, &image) {
                Ok(stream) => stream,
                Err(reason) => return Ok(Scanned::Skipped(reason)),
            };
            let snapshot = Snapshot {
                stream: &stream,
                position: reached,
                snapshot_id,
                cell_name: image.cell_name,
                committed_at: now_ms(),
                settings: &settings,
            };
            let counts = snapshot.emit(&db, &mut |record| {
                tx.blocking_send(record)
                    .map_err(|_| anyhow!("the snapshot writer stopped"))
            })?;
            Ok(Scanned::Done(counts, stream))
        })
    };
    let mut submit_error = None;
    while let Some(record) = rx.recv().await {
        if submit_error.is_some() {
            continue;
        }
        if let Err(error) = submit(sink, tracker, index, record, settings.buffer_bytes).await {
            submit_error = Some(error);
            // Dropping the receiver stops the scan at its next record.
            rx.close();
        }
    }
    let scanned = scan.await.context("the snapshot scan panicked")?;
    if let Some(error) = submit_error {
        return Err(error);
    }
    let (counts, stream) = match scanned? {
        Scanned::Done(counts, stream) => (counts, stream),
        Scanned::Skipped(reason) => return Ok(Some(reason)),
    };
    report.incarnation = stream.incarnation;
    report.tables = counts.tables;
    report.rows = counts.rows;
    report.records = counts.records;
    report.oversized_rows = counts.oversized_rows;
    Ok(None)
}

/// Restore at the target, or, when the bucket does not reach it yet, at
/// the bucket head: a snapshot through the head still replaces everything
/// up to it, and the report says the target is not covered.
async fn restore(
    source: &Bucket,
    stream: &Stream,
    target: Target,
    pace: Option<Arc<Pace>>,
) -> anyhow::Result<Restored> {
    match export_restore::restore_paced(source, stream, target, pace.clone()).await {
        Ok(restored) => Ok(restored),
        Err(error) if matches!(target, Target::AtOrAfter(_)) => {
            match export_restore::restore_paced(source, stream, Target::Head, pace).await {
                Ok(head) if matches!(target, Target::AtOrAfter(at) if head.position < at) => {
                    Ok(head)
                }
                _ => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// Hand one record to the sink, first waiting while the sink holds more
/// than `limit` unwritten bytes.
async fn submit(
    sink: &Arc<dyn ExportSink>,
    tracker: &Tracker,
    job: usize,
    record: Record,
    limit: u64,
) -> anyhow::Result<()> {
    loop {
        let progressed = tracker.progress.notified();
        if sink.buffered_bytes() <= limit {
            break;
        }
        sink.flush();
        progressed.await;
    }
    let batch = tracker.register(job, vec![record]);
    let seqs: Vec<u64> = batch.iter().map(|r| r.seq).collect();
    if sink.submit(batch).is_err() {
        tracker.forget(&seqs);
        bail!("the export sink closed");
    }
    Ok(())
}

/// Unique per run, so two repairs at one position are two snapshots and
/// never merge.
fn snapshot_id(position: Position) -> String {
    format!(
        "repair-e{}-{}-{:016x}",
        position.epoch,
        position.txid,
        rand::random::<u64>()
    )
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| i64::try_from(d.as_millis()).unwrap_or(i64::MAX))
}

#[cfg(test)]
mod tests;
