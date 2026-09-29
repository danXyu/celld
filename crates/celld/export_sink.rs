// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Export sinks are shell tasks outside the execution boundary, like telemetry.
#![allow(clippy::disallowed_methods)]

//! Change-export sinks (`docs/design/change-export.md#sinks`).
//!
//! [`ExportSink`] is what the exporter hands released records to. It takes
//! records for any set of streams and reports one terminal result per
//! record: acknowledged, or dropped with a reason. The exporter derives each
//! sink's delivered position from those results; a sink never tracks
//! positions itself.
//!
//! The contract every implementation keeps:
//!
//! - **Order.** Results arrive on the outcome channel in submission order,
//!   so a caller can advance a delivered position with a single cursor.
//! - **Acknowledged means durable.** A record is acknowledged only once the
//!   object or segment holding it has been written.
//! - **At least once.** A retried write can land twice; consumers drop
//!   duplicates by the dedup key.
//! - **Budgeted.** [`ExportSink::buffered_bytes`] counts every submitted
//!   record without a result yet, so the exporter can hold the sink buffer
//!   under the shared queue budget.
//!
//! [`BucketSink`] is the first implementation: Parquet objects in the bucket
//! through [`parquet_batch`], one row per record.

use crate::bucket::Bucket;
use crate::parquet_batch;
use crate::parquet_batch::text;
use anyhow::bail;
use anyhow::Context as _;
use celld_export_format::Body;
use celld_export_format::Envelope;
use celld_export_format::Kind;
use celld_export_format::Origin;
use celld_export_format::Position;
use celld_export_format::Record;
use celld_export_format::StreamId;
use futures_util::future::BoxFuture;
use futures_util::FutureExt as _;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;
use tokio::sync::mpsc;
use tokio::sync::oneshot;

/// Where the bucket sink writes, under `<node>/<yyyy>/<mm>/<dd>/<hh>/`.
pub const CHANGES_PREFIX: &str = "export/changes";

/// Stamped on every object as `celld-schema`. Bumped when the Parquet
/// layout below changes incompatibly.
const SCHEMA_VERSION: &str = "v0-unstable";

/// How often the retention sweep runs when retention is set. Telemetry's
/// cadence; retention is in whole days, so finer is wasted listing.
const SWEEP_INTERVAL: Duration = Duration::from_secs(6 * 3600);

/// One record handed to a sink, tagged with a sequence number the caller
/// chooses. The sink reports the record's result under the same number.
#[derive(Clone, Debug)]
pub struct SinkRecord {
    pub seq: u64,
    pub record: Record,
}

/// The terminal result of one record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Delivery {
    /// Durable at the sink. For the bucket sink, `object` is the key of the
    /// Parquet object holding it.
    Acknowledged { object: Arc<str> },
    /// Will never be delivered by this sink. The caller freezes the
    /// delivered position and emits a `gap`.
    Dropped { reason: Arc<str> },
}

impl Delivery {
    pub fn is_acknowledged(&self) -> bool {
        matches!(self, Delivery::Acknowledged { .. })
    }
}

/// Results for a run of consecutive submitted records, in submission order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Outcome {
    /// [`ExportSink::name`] of the sink that produced it, so several sinks
    /// can share one outcome channel.
    pub sink: &'static str,
    pub results: Vec<(u64, Delivery)>,
}

/// The sink has shut down and takes no more records.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Closed;

impl std::fmt::Display for Closed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("export sink closed")
    }
}

impl std::error::Error for Closed {}

/// A destination for released export records. Object safe, so the exporter
/// can drive the bucket sink and the blob-stream sink through one list.
pub trait ExportSink: Send + Sync {
    /// Stable name, used in [`Outcome::sink`], logs and metrics.
    fn name(&self) -> &'static str;

    /// Queue records for delivery. Never blocks and never waits on I/O. On
    /// `Ok`, every record gets exactly one result on the outcome channel;
    /// on `Err`, none of them does.
    fn submit(&self, records: Vec<SinkRecord>) -> Result<(), Closed>;

    /// Ask for everything submitted so far to be written now instead of at
    /// the next flush interval. Returns at once; results follow as usual.
    fn flush(&self);

    /// Encoded bytes of the records submitted and not yet resolved.
    fn buffered_bytes(&self) -> u64;

    /// Write what is buffered, report its results, and stop. Records
    /// submitted after this is called are refused.
    fn close(&self) -> BoxFuture<'static, ()>;
}

/// How long to keep what the bucket sink writes.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Retention {
    /// No sweep. Lifecycle is the consumer's, which is the default because
    /// the bucket may be the only copy a consumer has not loaded yet.
    #[default]
    None,
    /// Delete day partitions older than this many whole days.
    Days(u32),
}

impl Retention {
    fn label(self) -> String {
        match self {
            Retention::None => "none".to_string(),
            Retention::Days(days) => format!("{days}d"),
        }
    }
}

/// Bucket sink settings. Defaults follow the design's configuration table;
/// `CELLD_EXPORT_FLUSH_MS`, `CELLD_EXPORT_FLUSH_BYTES` and
/// `CELLD_EXPORT_RETENTION` map onto the first three fields. Which bucket
/// it writes to (`CELLD_EXPORT_BUCKET`) is the [`Bucket`] it is started
/// with.
#[derive(Clone, Debug)]
pub struct BucketSinkConfig {
    /// Flush interval. Ten seconds by default: a one-second flush on a
    /// hundred nodes is over eight million objects a day.
    pub flush: Duration,
    /// Flush early once this many encoded bytes are buffered.
    pub flush_bytes: u64,
    pub retention: Retention,
    /// Put attempts per object before its records are dropped.
    pub put_attempts: u32,
    /// Wait before the first retry, doubled for each one after.
    pub retry_backoff: Duration,
}

impl Default for BucketSinkConfig {
    fn default() -> Self {
        Self {
            flush: Duration::from_millis(10_000),
            flush_bytes: 8_388_608,
            retention: Retention::None,
            put_attempts: 3,
            retry_backoff: Duration::from_secs(1),
        }
    }
}

enum Command {
    Records(Vec<Pending>),
    Flush,
    Close(oneshot::Sender<()>),
}

/// A submitted record, encoded at submit so its size is known when it is
/// counted against the budget.
struct Pending {
    seq: u64,
    row: Result<Row, Arc<str>>,
    bytes: u64,
}

/// The bucket sink: Parquet under
/// `export/changes/<node>/<yyyy>/<mm>/<dd>/<hh>/<unix_us>-<rand>.parquet`,
/// one row per record, one column per envelope field and the kind-specific
/// body as a JSON string.
///
/// One task owns the buffer and writes one object per flush, so results
/// come out in submission order. A put that fails is retried with backoff
/// up to [`BucketSinkConfig::put_attempts`]; after that the object's
/// records are dropped. Records submitted while a put is retrying wait
/// behind it and count toward [`ExportSink::buffered_bytes`].
pub struct BucketSink {
    tx: mpsc::UnboundedSender<Command>,
    buffered: Arc<AtomicU64>,
}

impl BucketSink {
    pub const NAME: &'static str = "bucket";

    /// Start the sink's task, and the retention sweep when retention is
    /// set. Must be called inside a Tokio runtime.
    pub fn start(
        bucket: Bucket,
        node: String,
        config: BucketSinkConfig,
        outcomes: mpsc::UnboundedSender<Outcome>,
    ) -> BucketSink {
        tracing::info!(
            bucket = %bucket.name,
            flush_ms = config.flush.as_millis() as u64,
            flush_bytes = config.flush_bytes,
            retention = %config.retention.label(),
            "export bucket sink on"
        );
        if let Retention::Days(days) = config.retention {
            tokio::spawn(sweep_loop(bucket.clone(), days));
        }
        let (tx, rx) = mpsc::unbounded_channel();
        let buffered = Arc::new(AtomicU64::new(0));
        tokio::spawn(run(rx, bucket, node, config, outcomes, buffered.clone()));
        BucketSink { tx, buffered }
    }
}

impl ExportSink for BucketSink {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    fn submit(&self, records: Vec<SinkRecord>) -> Result<(), Closed> {
        if records.is_empty() {
            return Ok(());
        }
        let pending: Vec<Pending> = records
            .into_iter()
            .map(|SinkRecord { seq, record }| {
                let row = Row::new(record).map_err(|error| Arc::from(format!("{error:#}")));
                let bytes = row.as_ref().map_or(0, Row::approx_bytes);
                Pending { seq, row, bytes }
            })
            .collect();
        let bytes: u64 = pending.iter().map(|p| p.bytes).sum();
        self.buffered.fetch_add(bytes, Ordering::Relaxed);
        if self.tx.send(Command::Records(pending)).is_err() {
            self.buffered.fetch_sub(bytes, Ordering::Relaxed);
            return Err(Closed);
        }
        Ok(())
    }

    fn flush(&self) {
        let _ = self.tx.send(Command::Flush);
    }

    fn buffered_bytes(&self) -> u64 {
        self.buffered.load(Ordering::Relaxed)
    }

    fn close(&self) -> BoxFuture<'static, ()> {
        let (done, wait) = oneshot::channel();
        let sent = self.tx.send(Command::Close(done)).is_ok();
        async move {
            if sent {
                let _ = wait.await;
            }
        }
        .boxed()
    }
}

/// The sink's task: buffer until the interval, the byte threshold, an
/// explicit flush or close, then write one object and report.
async fn run(
    mut rx: mpsc::UnboundedReceiver<Command>,
    bucket: Bucket,
    node: String,
    config: BucketSinkConfig,
    outcomes: mpsc::UnboundedSender<Outcome>,
    buffered: Arc<AtomicU64>,
) {
    let writer = Writer {
        bucket,
        node,
        retention: config.retention.label(),
        put_attempts: config.put_attempts.max(1),
        retry_backoff: config.retry_backoff,
    };
    let mut batch: Vec<Pending> = Vec::new();
    let mut closing = None;
    loop {
        let deadline = tokio::time::Instant::now() + config.flush;
        let mut bytes = 0u64;
        let mut stop = false;
        loop {
            match tokio::time::timeout_at(deadline, rx.recv()).await {
                Ok(Some(Command::Records(records))) => {
                    bytes += records.iter().map(|p| p.bytes).sum::<u64>();
                    batch.extend(records);
                    if bytes >= config.flush_bytes {
                        break;
                    }
                }
                Ok(Some(Command::Flush)) => break,
                Ok(Some(Command::Close(done))) => {
                    closing = Some(done);
                    stop = true;
                    break;
                }
                // Every handle is gone: write what is left and stop.
                Ok(None) => {
                    stop = true;
                    break;
                }
                Err(_) => break,
            }
        }
        if stop {
            // Refuse later submits, but write what they queued before
            // the close so none of them is left without a result.
            rx.close();
            while let Ok(command) = rx.try_recv() {
                if let Command::Records(records) = command {
                    batch.extend(records);
                }
            }
        }
        if !batch.is_empty() {
            let pending = std::mem::take(&mut batch);
            let bytes: u64 = pending.iter().map(|p| p.bytes).sum();
            let results = writer.write(pending).await;
            buffered.fetch_sub(bytes, Ordering::Relaxed);
            let _ = outcomes.send(Outcome {
                sink: BucketSink::NAME,
                results,
            });
        }
        if stop {
            if let Some(done) = closing {
                let _ = done.send(());
            }
            return;
        }
    }
}

struct Writer {
    bucket: Bucket,
    node: String,
    retention: String,
    put_attempts: u32,
    retry_backoff: Duration,
}

impl Writer {
    /// Write one object holding every encodable record of `pending`, and
    /// return a result for each record in order.
    async fn write(&self, pending: Vec<Pending>) -> Vec<(u64, Delivery)> {
        // Each slot is a record's seq and, when it could not be encoded,
        // why; the rest share the one object's result.
        let mut slots = Vec::with_capacity(pending.len());
        let mut rows = Vec::with_capacity(pending.len());
        for Pending { seq, row, .. } in pending {
            match row {
                Ok(row) => {
                    rows.push(row);
                    slots.push((seq, None));
                }
                Err(reason) => slots.push((seq, Some(reason))),
            }
        }
        let delivery = if rows.is_empty() {
            None
        } else {
            Some(self.put(rows).await)
        };
        slots
            .into_iter()
            .map(|(seq, refused)| match (refused, &delivery) {
                (Some(reason), _) => (seq, Delivery::Dropped { reason }),
                (None, Some(delivery)) => (seq, delivery.clone()),
                (None, None) => unreachable!("an encodable record makes a put"),
            })
            .collect()
    }

    async fn put(&self, rows: Vec<Row>) -> Delivery {
        let count = rows.len();
        let encoded = tokio::task::spawn_blocking(move || encode_rows(&rows)).await;
        let bytes = match encoded {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(error)) => {
                tracing::warn!(%error, records = count, "export batch dropped: encode failed");
                return Delivery::Dropped {
                    reason: format!("encode failed: {error:#}").into(),
                };
            }
            Err(error) => {
                tracing::warn!(%error, records = count, "export batch dropped: encode failed");
                return Delivery::Dropped {
                    reason: format!("encode failed: {error}").into(),
                };
            }
        };
        let meta = [
            ("celld-retention", self.retention.as_str()),
            ("celld-schema", SCHEMA_VERSION),
        ];
        let mut backoff = self.retry_backoff;
        let mut attempt = 1;
        loop {
            let put = parquet_batch::put(
                &self.bucket,
                CHANGES_PREFIX,
                &self.node,
                now_unix_us(),
                bytes.clone(),
                &meta,
            );
            match put.await {
                Ok(key) => return Delivery::Acknowledged { object: key.into() },
                Err(parquet_batch::PutError { key, error }) if attempt < self.put_attempts => {
                    tracing::warn!(%error, key, attempt, "export batch put failed; retrying");
                    tokio::time::sleep(backoff).await;
                    backoff = backoff.saturating_mul(2);
                    attempt += 1;
                }
                Err(parquet_batch::PutError { key, error }) => {
                    tracing::warn!(
                        %error,
                        key,
                        attempts = attempt,
                        records = count,
                        "export batch dropped: put failed"
                    );
                    return Delivery::Dropped {
                        reason: format!("put failed after {attempt} attempts: {error:#}").into(),
                    };
                }
            }
        }
    }
}

fn now_unix_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros() as i64
}

/// Retention for the export prefix. Every node sweeps the whole prefix, so
/// a dead node's objects expire too; the deletes are idempotent.
async fn sweep_loop(bucket: Bucket, retention_days: u32) {
    loop {
        let deleted = sweep_once(&bucket, now_unix_us(), retention_days).await;
        if deleted > 0 {
            tracing::info!(deleted, retention_days, "export changes swept");
        }
        tokio::time::sleep(SWEEP_INTERVAL).await;
    }
}

/// One retention pass over [`CHANGES_PREFIX`].
#[doc(hidden)]
pub async fn sweep_once(bucket: &Bucket, now_unix_us: i64, retention_days: u32) -> u64 {
    let cutoff = parquet_batch::cutoff_date(now_unix_us, retention_days);
    parquet_batch::sweep_once(bucket, &[CHANGES_PREFIX], cutoff).await
}

/// The Parquet schema. These column names are what the loader's `COPY
/// INTO` and DuckDB queries read, so renaming one is a format change:
/// bump [`SCHEMA_VERSION`].
///
/// Unsigned envelope fields are stored as 64- and 32-bit integers
/// annotated unsigned. `body` is the record's kind-specific fields as one
/// JSON object, including `kind`; the envelope plus `body` is the whole
/// record.
const MESSAGE_TYPE: &str = "
message celld_export_change {
  required binary kind (STRING);
  required binary script (STRING);
  required binary class (STRING);
  required binary cell (STRING);
  optional binary cell_name (STRING);
  optional binary facet (STRING);
  required int64 incarnation (INTEGER(64,false));
  required int64 epoch (INTEGER(64,false));
  required int64 txid (INTEGER(64,false));
  required int64 commit (INTEGER(64,false));
  required int64 committed_at (TIMESTAMP(MILLIS,true));
  required binary node (STRING);
  required binary origin (STRING);
  required int32 fragment (INTEGER(32,false));
  required int32 fragments (INTEGER(32,false));
  required binary body (STRING);
}";

/// One record, ready for its Parquet row: the envelope and the encoded body.
struct Row {
    kind: Kind,
    envelope: Envelope,
    body: String,
}

impl Row {
    fn new(record: Record) -> anyhow::Result<Row> {
        let kind = record.body.kind();
        let body = serde_json::to_string(&record.body).context("encode record body")?;
        Ok(Row {
            kind,
            envelope: record.envelope,
            body,
        })
    }

    /// The string fields plus a fixed allowance for the numeric ones.
    fn approx_bytes(&self) -> u64 {
        let e = &self.envelope;
        let len = |s: &Option<String>| s.as_deref().map_or(0, str::len);
        (self.body.len()
            + e.stream.script.len()
            + e.stream.class.len()
            + e.stream.cell.len()
            + len(&e.cell_name)
            + len(&e.stream.facet)
            + e.node.len()
            + 64) as u64
    }
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Rows => "rows",
        Kind::Snapshot => "snapshot",
        Kind::SnapshotEnd => "snapshot_end",
        Kind::Schema => "schema",
        Kind::Link => "link",
        Kind::Recovered => "recovered",
        Kind::Deleted => "deleted",
        Kind::Watermark => "watermark",
        Kind::Bulk => "bulk",
        Kind::Gap => "gap",
    }
}

fn origin_name(origin: Origin) -> &'static str {
    match origin {
        Origin::Live => "live",
        Origin::Snapshot => "snapshot",
        Origin::Repair => "repair",
    }
}

fn parse_origin(name: &str) -> anyhow::Result<Origin> {
    Ok(match name {
        "live" => Origin::Live,
        "snapshot" => Origin::Snapshot,
        "repair" => Origin::Repair,
        other => bail!("unknown origin {other:?}"),
    })
}

/// Encode records as one bucket-sink Parquet object. Exposed for tools and
/// tests; the sink itself encodes through [`BucketSink`].
#[doc(hidden)]
pub fn encode_records(records: Vec<Record>) -> anyhow::Result<Vec<u8>> {
    let rows = records
        .into_iter()
        .map(Row::new)
        .collect::<anyhow::Result<Vec<_>>>()?;
    encode_rows(&rows)
}

fn encode_rows(rows: &[Row]) -> anyhow::Result<Vec<u8>> {
    use parquet::data_type::ByteArrayType;
    use parquet::data_type::Int32Type;
    use parquet::data_type::Int64Type;

    // A consumer looking for one cell's history scans every object in a
    // time range; the bloom filter lets it skip most of them.
    parquet_batch::encode(MESSAGE_TYPE, &["cell"], |columns| {
        let each = rows.iter();
        let envelopes = || each.clone().map(|r| &r.envelope);
        columns.required::<ByteArrayType>(
            &each
                .clone()
                .map(|r| text(kind_name(r.kind)))
                .collect::<Vec<_>>(),
        )?;
        columns.required::<ByteArrayType>(
            &envelopes()
                .map(|e| text(&e.stream.script))
                .collect::<Vec<_>>(),
        )?;
        columns.required::<ByteArrayType>(
            &envelopes()
                .map(|e| text(&e.stream.class))
                .collect::<Vec<_>>(),
        )?;
        columns.required::<ByteArrayType>(
            &envelopes()
                .map(|e| text(&e.stream.cell))
                .collect::<Vec<_>>(),
        )?;
        columns.optional::<ByteArrayType>(envelopes().map(|e| e.cell_name.as_deref().map(text)))?;
        columns
            .optional::<ByteArrayType>(envelopes().map(|e| e.stream.facet.as_deref().map(text)))?;
        columns.required::<Int64Type>(
            &envelopes()
                .map(|e| e.stream.incarnation as i64)
                .collect::<Vec<_>>(),
        )?;
        columns.required::<Int64Type>(
            &envelopes()
                .map(|e| e.position.epoch as i64)
                .collect::<Vec<_>>(),
        )?;
        columns.required::<Int64Type>(
            &envelopes()
                .map(|e| e.position.txid as i64)
                .collect::<Vec<_>>(),
        )?;
        columns.required::<Int64Type>(
            &envelopes()
                .map(|e| e.position.commit as i64)
                .collect::<Vec<_>>(),
        )?;
        columns.required::<Int64Type>(&envelopes().map(|e| e.committed_at).collect::<Vec<_>>())?;
        columns
            .required::<ByteArrayType>(&envelopes().map(|e| text(&e.node)).collect::<Vec<_>>())?;
        columns.required::<ByteArrayType>(
            &envelopes()
                .map(|e| text(origin_name(e.origin)))
                .collect::<Vec<_>>(),
        )?;
        columns
            .required::<Int32Type>(&envelopes().map(|e| e.fragment as i32).collect::<Vec<_>>())?;
        columns
            .required::<Int32Type>(&envelopes().map(|e| e.fragments as i32).collect::<Vec<_>>())?;
        columns
            .required::<ByteArrayType>(&each.clone().map(|r| text(&r.body)).collect::<Vec<_>>())?;
        Ok(())
    })
}

/// Read a bucket-sink Parquet object back into records, in row order.
/// The inverse of what [`BucketSink`] writes; for inspection, repair tools
/// and tests.
pub fn decode_records(bytes: Vec<u8>) -> anyhow::Result<Vec<Record>> {
    use parquet::file::reader::FileReader as _;
    use parquet::file::reader::SerializedFileReader;
    use parquet::record::Field;

    let reader = SerializedFileReader::new(bytes::Bytes::from(bytes))?;
    let mut records = Vec::new();
    for row in reader.get_row_iter(None)? {
        let row = row?;
        let mut kind = None;
        let mut script = None;
        let mut class = None;
        let mut cell = None;
        let mut cell_name = None;
        let mut facet = None;
        let mut incarnation = None;
        let mut epoch = None;
        let mut txid = None;
        let mut commit = None;
        let mut committed_at = None;
        let mut node = None;
        let mut origin = None;
        let mut fragment = None;
        let mut fragments = None;
        let mut body = None;
        for (name, field) in row.get_column_iter() {
            let string = || match field {
                Field::Str(value) => Ok(value.clone()),
                other => bail!("column {name}: expected a string, found {other:?}"),
            };
            let optional_string = || match field {
                Field::Null => Ok(None),
                Field::Str(value) => Ok(Some(value.clone())),
                other => bail!("column {name}: expected a string, found {other:?}"),
            };
            let unsigned = || match field {
                Field::ULong(value) => Ok(*value),
                Field::UInt(value) => Ok(*value as u64),
                other => bail!("column {name}: expected an unsigned integer, found {other:?}"),
            };
            match name.as_str() {
                "kind" => kind = Some(string()?),
                "script" => script = Some(string()?),
                "class" => class = Some(string()?),
                "cell" => cell = Some(string()?),
                "cell_name" => cell_name = optional_string()?,
                "facet" => facet = optional_string()?,
                "incarnation" => incarnation = Some(unsigned()?),
                "epoch" => epoch = Some(unsigned()?),
                "txid" => txid = Some(unsigned()?),
                "commit" => commit = Some(unsigned()?),
                "committed_at" => {
                    committed_at = Some(match field {
                        Field::TimestampMillis(value) => *value,
                        other => {
                            bail!("column committed_at: expected a timestamp, found {other:?}")
                        }
                    })
                }
                "node" => node = Some(string()?),
                "origin" => origin = Some(parse_origin(&string()?)?),
                "fragment" => fragment = Some(u32::try_from(unsigned()?)?),
                "fragments" => fragments = Some(u32::try_from(unsigned()?)?),
                "body" => body = Some(string()?),
                other => bail!("unknown column {other}"),
            }
        }
        let missing = |name: &str| anyhow::anyhow!("row is missing column {name}");
        let body: Body =
            serde_json::from_str(&body.ok_or_else(|| missing("body"))?).context("decode body")?;
        let kind = kind.ok_or_else(|| missing("kind"))?;
        if kind != kind_name(body.kind()) {
            bail!(
                "kind column {kind:?} disagrees with body kind {:?}",
                kind_name(body.kind())
            );
        }
        records.push(Record {
            envelope: Envelope {
                stream: StreamId {
                    script: script.ok_or_else(|| missing("script"))?,
                    class: class.ok_or_else(|| missing("class"))?,
                    cell: cell.ok_or_else(|| missing("cell"))?,
                    facet,
                    incarnation: incarnation.ok_or_else(|| missing("incarnation"))?,
                },
                cell_name,
                position: Position {
                    epoch: epoch.ok_or_else(|| missing("epoch"))?,
                    txid: txid.ok_or_else(|| missing("txid"))?,
                    commit: commit.ok_or_else(|| missing("commit"))?,
                },
                committed_at: committed_at.ok_or_else(|| missing("committed_at"))?,
                node: node.ok_or_else(|| missing("node"))?,
                origin: origin.ok_or_else(|| missing("origin"))?,
                fragment: fragment.ok_or_else(|| missing("fragment"))?,
                fragments: fragments.ok_or_else(|| missing("fragments"))?,
            },
            body,
        });
    }
    Ok(records)
}

#[cfg(test)]
mod tests;
