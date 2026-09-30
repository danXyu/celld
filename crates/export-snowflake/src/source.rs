// The loader is a host tool outside celld's execution boundary: its clock,
// timers and tasks are the host's.
#![allow(clippy::disallowed_methods)]

//! Feeding the loader from the export topic, whichever transport carries it
//! (`blob-stream` or `kafka` feature).
//!
//! The loader is a member of a consumer group, `snowflake` by default, over
//! the topic the nodes' sink writes: blob-stream ([`crate::blob_stream`]) or
//! Kafka ([`crate::kafka`]). Each is a [`Source`]; [`run`] is the same loop
//! over either. It reads records into a [`Batch`] and lands the batch in
//! `EXPORT_LANDING` (through Snowpipe Streaming, [`crate::streaming`]) once it
//! is full or has waited `linger`; only once every row is acknowledged does
//! it store and commit the offsets the batch covered. A crash or a lost
//! lease therefore replays at most the batches that had not landed, and a
//! replayed record is a duplicate every reader drops. The route task moves
//! landed records into the tables on its schedule, and the same loop keeps
//! the Dynamic Tables in step with the schemas it has seen.
//!
//! A batch that fails to land is retried, the same batch, with backoff, and
//! nothing is read meanwhile. A message that is not a record stops the loop
//! with an error, once what came before it has landed and been committed,
//! so it is read again when the loader restarts: a newer loader may decode
//! it, and an operator can list it in `skip` to drop it. When the group
//! revokes partitions, the loader lands what it holds before letting them
//! go, so the next owner starts where it stopped.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::time::Duration;

use anyhow::anyhow;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::consume::{message_source, Batch, Land, Limits, Undecodable};
use crate::loader::{LoadError, Loader, SyncReport, Warehouse};

/// The consumer group the loader joins unless told otherwise.
pub const DEFAULT_GROUP: &str = "snowflake";

/// A consumer-group member reading the export topic. Partitions and
/// offsets are the transport's own; a message's source is
/// `<name>/<partition>/<offset>`.
pub trait Source {
    /// What the partitions were revoked with.
    type Revoked: Revoked;

    /// The transport's name, as sources and `EXPORT_SKIP` spell it:
    /// `blob-stream` or `kafka`.
    fn name(&self) -> &'static str;

    /// Join the group.
    fn start(&mut self) -> anyhow::Result<()>;

    /// The next message, or partitions the group is taking away. An error
    /// stops the loop; [`Next::Failed`] does not.
    fn next(&mut self) -> impl Future<Output = anyhow::Result<Next<Self::Revoked>>>;

    /// Record that every message up to and including `offset` in
    /// `partition` is done with, for the next [`Source::commit`].
    fn store_offset(&mut self, partition: u32, offset: u64) -> anyhow::Result<()>;

    /// Commit what was stored. Returns the partitions the group committed
    /// without this member: another member owns them now.
    fn commit(&mut self) -> impl Future<Output = anyhow::Result<Vec<u32>>>;

    /// Leave the group, committing what was stored and giving up its
    /// partitions.
    fn shutdown(self) -> impl Future<Output = anyhow::Result<()>>;
}

/// Partitions the group is taking away. They are let go once
/// [`Revoked::complete`] returns.
pub trait Revoked {
    fn partitions(&self) -> Vec<u32>;
    fn complete(self) -> impl Future<Output = ()>;
}

/// What [`Source::next`] read.
pub enum Next<R> {
    Record {
        partition: u32,
        offset: u64,
        payload: Vec<u8>,
    },
    Revoked(R),
    /// A read error the source recovers from on its own; the loop reports
    /// it and reads on.
    Failed(anyhow::Error),
}

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
    /// Messages to drop, by source (`<transport>/<partition>/<offset>`):
    /// ones an operator has looked at and decided are not records.
    pub skip: BTreeSet<String>,
}

impl Default for Settings {
    fn default() -> Self {
        Settings {
            limits: Limits::default(),
            linger: Duration::from_secs(5),
            sync_every: Duration::from_secs(60),
            retry: Duration::from_secs(1),
            retry_max: Duration::from_secs(60),
            skip: BTreeSet::new(),
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
    /// A message listed in `skip`; its offset is committed with the
    /// batch's.
    Skipped(&'a str),
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
    /// Reading failed in a way the source recovers from on its own, such as
    /// a broker that is briefly unreachable.
    ReadFailed(&'a anyhow::Error),
    Synced(&'a SyncReport),
    SyncFailed(&'a LoadError),
}

/// Consume until `stop` is cancelled or the consumer fails: land batches
/// with `lander`, commit their offsets, and sync the Dynamic Tables through
/// `loader` every `sync_every`.
/// On stop, what has been read lands (unless it is failing to), and the
/// source shuts down, committing and giving up its partitions.
///
/// Must run on a multi-threaded Tokio runtime: Snowflake requests block,
/// and run in place on this task's thread.
pub async fn run<S: Source, W: Warehouse, L: Land>(
    mut source: S,
    loader: &mut Loader<W>,
    lander: &mut L,
    settings: &Settings,
    stop: CancellationToken,
    mut report: impl FnMut(Event<'_>),
) -> anyhow::Result<()> {
    source.start()?;
    let name = source.name();
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
                    if !flush(&mut source, &mut batch, lander, settings, &stop, &mut report).await {
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
            next = source.next() => match next {
                Err(e) => break Err(e.context("read the export topic")),
                Ok(Next::Record { partition, offset, payload }) => {
                    if due.is_none() {
                        due = Some(Instant::now() + settings.linger);
                    }
                    let from = message_source(name, partition, offset);
                    if settings.skip.contains(&from) {
                        batch.skip(partition, offset);
                        report(Event::Skipped(&from));
                    } else if let Err(u) = batch.push_message(name, partition, offset, &payload) {
                        // Commit up to it, never past it.
                        if !flush(&mut source, &mut batch, lander, settings, &stop, &mut report).await {
                            break Ok(());
                        }
                        break Err(not_a_record(&u));
                    }
                    if batch.is_full(&settings.limits) {
                        if !flush(&mut source, &mut batch, lander, settings, &stop, &mut report).await {
                            break Ok(());
                        }
                        due = None;
                    }
                }
                Ok(Next::Revoked(revoked)) => {
                    // Land before letting go, or the next owner reads the
                    // batch again. A stop mid-retry lets go without landing;
                    // the batch is then read again, which is harmless.
                    flush(&mut source, &mut batch, lander, settings, &stop, &mut report).await;
                    due = None;
                    report(Event::Revoked(&revoked.partitions()));
                    revoked.complete().await;
                }
                Ok(Next::Failed(e)) => report(Event::ReadFailed(&e)),
            },
        }
    };
    if result.is_ok() && !batch.is_empty() {
        // Stopping: one attempt, no retries, so a stop is never held up by
        // a warehouse that is down. What does not land is read again.
        let records = batch.len();
        match tokio::task::block_in_place(|| batch.land(lander)) {
            Ok(offsets) => commit(&mut source, &offsets, records, &mut report).await,
            Err(e) => report(Event::LandFailed {
                error: &e,
                retry: Duration::ZERO,
            }),
        }
    }
    let shutdown = source.shutdown().await;
    result.and(shutdown.map_err(|e| e.context(format!("shut the {name} consumer down"))))
}

fn not_a_record(u: &Undecodable) -> anyhow::Error {
    anyhow!(
        "{} is not a record ({}). Nothing past it in its partition is committed. \
         If it comes from a newer celld, upgrade the loader; to drop it, add {} to EXPORT_SKIP",
        u.source,
        u.error,
        u.source
    )
}

/// Land `batch`, retrying until it lands, and commit its offsets. Returns
/// false if `stop` was cancelled first; the batch is then kept.
async fn flush<S: Source, L: Land>(
    source: &mut S,
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
                commit(source, &offsets, records, report).await;
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

async fn commit<S: Source>(
    source: &mut S,
    offsets: &BTreeMap<u32, u64>,
    records: usize,
    report: &mut impl FnMut(Event<'_>),
) {
    for (&partition, &offset) in offsets {
        if let Err(e) = source.store_offset(partition, offset) {
            report(Event::CommitFailed(&e.context(format!(
                "store offset {offset} of partition {partition}"
            ))));
        }
    }
    match source.commit().await {
        Ok(fenced) => {
            if !fenced.is_empty() {
                report(Event::Fenced(&fenced));
            }
        }
        Err(e) => report(Event::CommitFailed(&e.context("commit offsets"))),
    }
    report(Event::Landed { records, offsets });
}
