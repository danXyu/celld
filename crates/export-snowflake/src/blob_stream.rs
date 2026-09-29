// The loader is a host tool outside celld's execution boundary: its clock,
// timers, tasks and config file are the host's.
#![allow(clippy::disallowed_methods)]

//! Feeding the loader from the export topic (`blob-stream` feature).
//!
//! The loader is a member of a blob-stream consumer group, `snowflake` by
//! default, over the topic the nodes' blob-stream sink writes. It reads
//! records into a [`Batch`] and lands the batch in `EXPORT_LANDING` (through
//! Snowpipe Streaming, [`crate::streaming`]) once it is full or has waited
//! `linger`; only once every row is acknowledged does it store and commit
//! the offsets the batch covered. A crash or a lost lease therefore replays at
//! most the batches that had not landed, and a replayed record is a
//! duplicate every reader drops. The route task moves landed records into
//! the tables on its schedule, and the same loop keeps the Dynamic Tables in
//! step with the schemas it has seen.
//!
//! A batch that fails to land is retried, the same batch, with backoff, and
//! nothing is read meanwhile. When the group revokes partitions, the loader
//! lands what it holds before letting them go, so the next owner starts
//! where it stopped.
//!
//! The consumer's own settings (topic, S3 and DynamoDB, broker discovery) are
//! blob-stream's `ConsumerIteratorBootstrapConfig`, read by
//! [`bootstrap_config`] from a YAML or JSON file in the same form as the
//! brokers' config.

use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Context as _};
use blob_stream_consumer::iterator::{ConsumerIterator, NextResult};
use blob_stream_consumer::ConsumerConfigFactory;
use blob_stream_proto::protos::blobstream::v1::config::ConsumerIteratorBootstrapConfig;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::consume::{Batch, Land, Limits, Undecodable};
use crate::loader::{LoadError, Loader, SyncReport, Warehouse};

/// The consumer group the loader joins unless told otherwise.
pub const DEFAULT_GROUP: &str = "snowflake";

/// How the loop batches and how often it syncs the Dynamic Tables.
#[derive(Clone, Debug)]
pub struct Settings {
    pub limits: Limits,
    /// The longest a record waits in a batch before the batch lands.
    pub linger: Duration,
    /// How often the Dynamic Tables are synced with the schema union.
    pub sync_every: Duration,
    /// The first wait before landing a failed batch again, doubled for each
    /// failure after, up to `retry_max`.
    pub retry: Duration,
    pub retry_max: Duration,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            limits: Limits::default(),
            linger: Duration::from_secs(5),
            sync_every: Duration::from_secs(60),
            retry: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
        }
    }
}

/// What the loop did, for the caller to log.
#[derive(Debug)]
pub enum Event<'a> {
    /// A batch landed and its offsets were committed.
    Landed {
        records: usize,
        offsets: &'a BTreeMap<u32, u64>,
    },
    /// A message that is not a record; its offset is committed with the
    /// batch's.
    Skipped(&'a Undecodable),
    /// A batch failed to land; the same batch is tried again after `retry`.
    LandFailed {
        error: &'a LoadError,
        retry: Duration,
    },
    /// Storing or committing offsets failed. The records have landed, so at
    /// worst another member reads them again.
    CommitFailed(&'a anyhow::Error),
    /// Partitions the group committed without this member: another member
    /// owns them now and may read their last batch again.
    Fenced(&'a [u32]),
    /// The group took partitions away; what the loader held of them landed
    /// first.
    Revoked(&'a [u32]),
    Synced(&'a SyncReport),
    SyncFailed(&'a LoadError),
}

/// Read the consumer's bootstrap config from `path` (`.yaml`, `.yml` or
/// `.json`), in blob-stream's protobuf JSON form. `group` and `member`
/// replace the file's group and member ids when given; a group left unset in
/// both is [`DEFAULT_GROUP`], and the group and read topics default to the
/// topic's name.
pub fn bootstrap_config(
    path: &Path,
    group: Option<&str>,
    member: Option<&str>,
) -> anyhow::Result<ConsumerIteratorBootstrapConfig> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("read the blob-stream consumer config {}", path.display()))?;
    let json = match path.extension().and_then(|e| e.to_str()) {
        Some("json") => text,
        Some("yaml" | "yml") => {
            let value: serde_json::Value =
                serde_yaml::from_str(&text).context("parse the consumer config as YAML")?;
            value.to_string()
        }
        _ => bail!(
            "the blob-stream consumer config must be .yaml, .yml or .json, not {}",
            path.display()
        ),
    };
    let mut config =
        protobuf_json_mapping::parse_from_str::<ConsumerIteratorBootstrapConfig>(&json)
            .context("decode the blob-stream consumer config")?;
    let topic = config
        .topic
        .as_ref()
        .map(|t| t.name.to_string())
        .unwrap_or_default();
    let runtime = config.runtime.mut_or_insert_default();
    let read = runtime.read.mut_or_insert_default();
    if read.topic.is_empty() {
        read.topic = topic.clone().into();
    }
    let g = runtime.group.mut_or_insert_default();
    if g.topic.is_empty() {
        g.topic = topic.into();
    }
    if let Some(group) = group {
        g.group_id = group.to_string().into();
    } else if g.group_id.is_empty() {
        g.group_id = DEFAULT_GROUP.to_string().into();
    }
    if let Some(member) = member {
        g.member_id = member.to_string().into();
    }
    if g.member_id.is_empty() {
        bail!("the blob-stream consumer needs a member id, stable for this loader: set EXPORT_MEMBER_ID");
    }
    Ok(config)
}

/// A consumer iterator for `config`, not yet started.
pub async fn connect(
    config: ConsumerIteratorBootstrapConfig,
) -> anyhow::Result<Box<dyn ConsumerIterator>> {
    let scope = bd_server_stats::stats::Collector::default().scope("celld_export_loader");
    let iterator = ConsumerConfigFactory::build_iterator_from_proto_config(config, scope, None)
        .await
        .context("start the blob-stream consumer")?;
    Ok(Box::new(iterator))
}

/// Consume until `stop` is cancelled or the consumer fails: land batches
/// with `lander`, commit their offsets, and sync the Dynamic Tables through
/// `loader` every `sync_every`.
/// On stop, what has been read lands (unless it is failing to), and the
/// iterator shuts down, committing and giving up its partitions.
///
/// Must run on a multi-threaded Tokio runtime: Snowflake requests block,
/// and run in place on this task's thread.
pub async fn run<W: Warehouse, L: Land>(
    mut iterator: Box<dyn ConsumerIterator>,
    loader: &mut Loader<W>,
    lander: &mut L,
    settings: &Settings,
    stop: CancellationToken,
    mut report: impl FnMut(Event<'_>),
) -> anyhow::Result<()> {
    iterator.start()?;
    let mut batch = Batch::default();
    // When the batch's oldest record must land.
    let mut due: Option<Instant> = None;
    let mut next_sync = Instant::now() + settings.sync_every;
    let result = loop {
        let wake = due.map_or(next_sync, |d| d.min(next_sync));
        tokio::select! {
            biased;
            () = stop.cancelled() => break Ok(()),
            () = tokio::time::sleep_until(wake) => {
                let now = Instant::now();
                if due.is_some_and(|d| d <= now) {
                    if !flush(&mut *iterator, &mut batch, lander, settings, &stop, &mut report).await {
                        break Ok(());
                    }
                    due = None;
                }
                if next_sync <= now {
                    match tokio::task::block_in_place(|| loader.sync_dynamic_tables()) {
                        Ok(r) => report(Event::Synced(&r)),
                        Err(e) => report(Event::SyncFailed(&e)),
                    }
                    next_sync = Instant::now() + settings.sync_every;
                }
            }
            next = iterator.next() => match next {
                Err(e) => break Err(e.context("read the export topic")),
                Ok(NextResult::Record(r)) => {
                    if due.is_none() {
                        due = Some(Instant::now() + settings.linger);
                    }
                    if let Err(u) = batch.push_message(r.virtual_partition_id, r.offset, &r.record.payload) {
                        report(Event::Skipped(&u));
                    }
                    if batch.is_full(&settings.limits) {
                        if !flush(&mut *iterator, &mut batch, lander, settings, &stop, &mut report).await {
                            break Ok(());
                        }
                        due = None;
                    }
                }
                Ok(NextResult::Revoked(revoked)) => {
                    // Land before letting go, or the next owner reads the
                    // batch again. A stop mid-retry lets go without landing;
                    // the batch is then read again, which is harmless.
                    flush(&mut *iterator, &mut batch, lander, settings, &stop, &mut report).await;
                    due = None;
                    report(Event::Revoked(&revoked.partitions()));
                    revoked.complete().await;
                }
            },
        }
    };
    if result.is_ok() && !batch.is_empty() {
        // Stopping: one attempt, no retries, so a stop is never held up by
        // a warehouse that is down. What does not land is read again.
        let records = batch.len();
        match tokio::task::block_in_place(|| batch.land(lander)) {
            Ok(offsets) => commit(&mut *iterator, &offsets, records, &mut report).await,
            Err(e) => report(Event::LandFailed {
                error: &e,
                retry: Duration::ZERO,
            }),
        }
    }
    let shutdown = iterator.shutdown().await;
    result.and(shutdown.context("shut the blob-stream consumer down"))
}

/// Land `batch`, retrying until it lands, and commit its offsets. Returns
/// false if `stop` was cancelled first; the batch is then kept.
async fn flush<L: Land>(
    iterator: &mut dyn ConsumerIterator,
    batch: &mut Batch,
    lander: &mut L,
    settings: &Settings,
    stop: &CancellationToken,
    report: &mut impl FnMut(Event<'_>),
) -> bool {
    if batch.is_empty() {
        return true;
    }
    let records = batch.len();
    let mut wait = settings.retry;
    loop {
        match tokio::task::block_in_place(|| batch.land(lander)) {
            Ok(offsets) => {
                commit(iterator, &offsets, records, report).await;
                return true;
            }
            Err(error) => {
                report(Event::LandFailed {
                    error: &error,
                    retry: wait,
                });
                tokio::select! {
                    () = stop.cancelled() => return false,
                    () = tokio::time::sleep(wait) => {}
                }
                wait = (wait * 2).min(settings.retry_max);
            }
        }
    }
}

async fn commit(
    iterator: &mut dyn ConsumerIterator,
    offsets: &BTreeMap<u32, u64>,
    records: usize,
    report: &mut impl FnMut(Event<'_>),
) {
    for (&partition, &offset) in offsets {
        if let Err(e) = iterator.store_offset(partition, offset) {
            report(Event::CommitFailed(&e.context(format!(
                "store offset {offset} of partition {partition}"
            ))));
        }
    }
    match iterator.commit().await {
        Ok(r) => {
            if !r.fenced_partitions.is_empty() {
                report(Event::Fenced(&r.fenced_partitions));
            }
        }
        Err(e) => report(Event::CommitFailed(&e.context("commit offsets"))),
    }
    report(Event::Landed { records, offsets });
}

#[cfg(test)]
mod tests;
