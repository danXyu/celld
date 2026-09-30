// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Export sinks are shell tasks outside the execution boundary, like telemetry.
#![allow(clippy::disallowed_methods)]

//! A change-export sink over a message topic
//! (`docs/design/change-export.md#sinks`): the part the blob-stream and
//! Kafka sinks share.
//!
//! Each record goes to the topic as one message: the record's JSON as the
//! payload, the stream identity as the key (so the producer keeps a stream
//! in one partition), and `committed_at` as the event time. The transport
//! is a [`Produce`], which answers one terminal result per message; a
//! record it cannot deliver within `CELLD_EXPORT_RETRY_MS` is dropped, and
//! the exporter freezes the stream's delivered position with a `gap`.
//! Only [`Produce`] differs between transports, so the ordering and budget
//! logic here is tested in every build, whatever client is compiled in.
//!
//! Ordering: every submit becomes one produce call, started at once so calls
//! overlap, and a reporter task waits for them in submission order. Results
//! therefore reach the outcome channel in order even when a later call
//! finishes first.
//!
//! Connecting: a producer may need its brokers before its first produce.
//! The sink starts without one and keeps retrying in the background, so a
//! broker outage at boot delays export instead of failing the node.
//! Records submitted meanwhile wait, count toward
//! [`crate::export_sink::ExportSink::buffered_bytes`], and are dropped once they have waited the
//! retry deadline.

use futures_util::future::BoxFuture;
use std::sync::Arc;

#[cfg(any(test, feature = "export-blob-stream", feature = "export-kafka"))]
pub use sink::*;

/// One message for the topic.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    /// Partitioning key: the record's stream identity.
    pub key: Vec<u8>,
    /// The record as one JSON object.
    pub payload: bytes::Bytes,
    /// Event time in Unix milliseconds: the commit's `committed_at`.
    pub event_ts_ms: i64,
}

/// Produces messages and reports one terminal result per message, in input
/// order: `Ok` with where it landed, or `Err` with why it never will.
pub trait Produce: Send + Sync + 'static {
    fn produce(
        &self,
        messages: Vec<Message>,
    ) -> BoxFuture<'static, Vec<Result<Arc<str>, Arc<str>>>>;
}

/// Builds a connected producer. Called again after each failure.
pub type Connect =
    Box<dyn Fn() -> BoxFuture<'static, anyhow::Result<Arc<dyn Produce>>> + Send + Sync>;

#[cfg(any(test, feature = "export-blob-stream", feature = "export-kafka"))]
mod sink {
    use super::*;
    use crate::export_sink::Closed;
    use crate::export_sink::Delivery;
    use crate::export_sink::ExportSink;
    use crate::export_sink::Outcome;
    use crate::export_sink::SinkRecord;
    use celld_export_format::Record;
    use celld_export_format::StreamId;
    use futures_util::FutureExt as _;
    use std::collections::VecDeque;
    use std::sync::atomic::AtomicU64;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;
    use std::time::Duration;
    use tokio::sync::mpsc;
    use tokio::sync::watch;
    use tokio::time::Instant;

    /// Sink settings that are not the producer's.
    #[derive(Clone, Debug)]
    pub struct TopicSinkConfig {
        /// How long a record may wait for a connected producer before it is
        /// dropped. The producer applies the same deadline to its own
        /// retries once connected (`CELLD_EXPORT_RETRY_MS`).
        pub retry: Duration,
        /// Wait before reconnecting after the first failed connect, doubled
        /// for each one after up to `reconnect_max`.
        pub reconnect: Duration,
        pub reconnect_max: Duration,
    }

    impl Default for TopicSinkConfig {
        fn default() -> Self {
            Self {
                retry: crate::export::DEFAULT_RETRY,
                reconnect: Duration::from_secs(1),
                reconnect_max: Duration::from_secs(30),
            }
        }
    }

    enum Command {
        Records(Vec<Pending>),
        Connected(Arc<dyn Produce>),
        Close,
    }

    /// A submitted record, encoded at submit so its size is known when it
    /// is counted against the budget.
    struct Pending {
        seq: u64,
        message: Result<Message, Arc<str>>,
        bytes: u64,
    }

    /// Records waiting for a connected producer.
    struct Waiting {
        deadline: Instant,
        records: Vec<Pending>,
    }

    /// One submit's results, as the reporter receives them: already known,
    /// or a produce call in flight.
    struct Batch {
        seqs: Vec<u64>,
        bytes: u64,
        results: BoxFuture<'static, Vec<(u64, Delivery)>>,
    }

    /// A sink over a topic. See the module docs.
    pub struct TopicSink {
        /// The sink's name, as outcomes and logs carry it.
        name: &'static str,
        /// The sender, until [`ExportSink::close`] takes it. Submitting
        /// holds the lock across the send, so no record can queue behind
        /// the close.
        tx: Mutex<Option<mpsc::UnboundedSender<Command>>>,
        buffered: Arc<AtomicU64>,
        /// Set by the reporter once the last result is sent.
        stopped: watch::Receiver<bool>,
    }

    impl TopicSink {
        /// Start the sink named `name`: its tasks and the first connect.
        /// Must be called inside a Tokio runtime.
        pub fn start(
            name: &'static str,
            connect: Connect,
            config: TopicSinkConfig,
            outcomes: mpsc::UnboundedSender<Outcome>,
        ) -> TopicSink {
            let (tx, rx) = mpsc::unbounded_channel();
            let (report_tx, report_rx) = mpsc::unbounded_channel();
            let buffered = Arc::new(AtomicU64::new(0));
            let (stop, stopped) = watch::channel(false);
            let last_error: Arc<Mutex<Option<String>>> = Arc::default();
            tokio::spawn(connect_loop(
                name,
                connect,
                tx.downgrade(),
                config.clone(),
                last_error.clone(),
            ));
            tokio::spawn(run(name, rx, config, report_tx, last_error));
            tokio::spawn(report(name, report_rx, outcomes, buffered.clone(), stop));
            TopicSink {
                name,
                tx: Mutex::new(Some(tx)),
                buffered,
                stopped,
            }
        }
    }

    impl ExportSink for TopicSink {
        fn name(&self) -> &'static str {
            self.name
        }

        fn submit(&self, records: Vec<SinkRecord>) -> Result<(), Closed> {
            if records.is_empty() {
                return Ok(());
            }
            let pending: Vec<Pending> = records
                .into_iter()
                .map(|SinkRecord { seq, record }| {
                    let message = message(&record).map_err(|error| Arc::from(format!("{error:#}")));
                    let bytes = message
                        .as_ref()
                        .map_or(0, |m| (m.key.len() + m.payload.len()) as u64);
                    Pending {
                        seq,
                        message,
                        bytes,
                    }
                })
                .collect();
            let bytes: u64 = pending.iter().map(|p| p.bytes).sum();
            let tx = self.tx.lock().unwrap_or_else(|e| e.into_inner());
            let Some(tx) = tx.as_ref() else {
                return Err(Closed);
            };
            self.buffered.fetch_add(bytes, Ordering::Relaxed);
            if tx.send(Command::Records(pending)).is_err() {
                self.buffered.fetch_sub(bytes, Ordering::Relaxed);
                return Err(Closed);
            }
            Ok(())
        }

        /// The producer flushes on its own short delay; there is nothing to
        /// hurry.
        fn flush(&self) {}

        fn buffered_bytes(&self) -> u64 {
            self.buffered.load(Ordering::Relaxed)
        }

        fn close(&self) -> BoxFuture<'static, ()> {
            // Refuse submits from here on. The first close queues the
            // command behind every accepted record; later ones only wait.
            if let Some(tx) = self.tx.lock().unwrap_or_else(|e| e.into_inner()).take() {
                let _ = tx.send(Command::Close);
            }
            let mut stopped = self.stopped.clone();
            async move {
                // An error means the reporter is gone, which it only is
                // once stopped or when the runtime is shutting down.
                let _ = stopped.wait_for(|stopped| *stopped).await;
            }
            .boxed()
        }
    }

    /// The topic message for one record.
    pub fn message(record: &Record) -> anyhow::Result<Message> {
        let payload = serde_json::to_vec(record)?;
        Ok(Message {
            key: stream_key(&record.envelope.stream),
            payload: payload.into(),
            event_ts_ms: record.envelope.committed_at,
        })
    }

    /// The stream identity as a partitioning key. Only its stability
    /// matters: the same stream always hashes to the same partition.
    fn stream_key(stream: &StreamId) -> Vec<u8> {
        let mut key =
            Vec::with_capacity(stream.script.len() + stream.class.len() + stream.cell.len() + 32);
        for part in [&stream.script, &stream.class, &stream.cell] {
            key.extend_from_slice(part.as_bytes());
            key.push(0);
        }
        if let Some(facet) = &stream.facet {
            key.extend_from_slice(facet.as_bytes());
        }
        key.push(0);
        key.extend_from_slice(&stream.incarnation.to_be_bytes());
        key
    }

    /// Build the producer, retrying with backoff until it connects or the
    /// sink goes away.
    async fn connect_loop(
        name: &'static str,
        connect: Connect,
        tx: mpsc::WeakUnboundedSender<Command>,
        config: TopicSinkConfig,
        last_error: Arc<Mutex<Option<String>>>,
    ) {
        let mut backoff = config.reconnect;
        loop {
            let result = connect().await;
            let Some(tx) = tx.upgrade() else {
                return;
            };
            match result {
                Ok(producer) => {
                    tracing::info!(sink = name, "export producer connected");
                    let _ = tx.send(Command::Connected(producer));
                    return;
                }
                Err(error) => {
                    tracing::warn!(
                        error = format!("{error:#}"),
                        sink = name,
                        retry_ms = backoff.as_millis() as u64,
                        "export producer could not connect; retrying"
                    );
                    *last_error.lock().unwrap_or_else(|e| e.into_inner()) =
                        Some(format!("{error:#}"));
                }
            }
            drop(tx);
            tokio::time::sleep(backoff).await;
            backoff = backoff.saturating_mul(2).min(config.reconnect_max);
        }
    }

    /// Hand each submit to the producer once there is one, in order.
    async fn run(
        name: &'static str,
        mut rx: mpsc::UnboundedReceiver<Command>,
        config: TopicSinkConfig,
        report: mpsc::UnboundedSender<Batch>,
        last_error: Arc<Mutex<Option<String>>>,
    ) {
        let mut producer: Option<Arc<dyn Produce>> = None;
        let mut waiting: VecDeque<Waiting> = VecDeque::new();
        let not_connected = |last_error: &Mutex<Option<String>>| -> Arc<str> {
            match &*last_error.lock().unwrap_or_else(|e| e.into_inner()) {
                Some(error) => format!("no {name} producer: {error}").into(),
                None => format!("no {name} producer: still connecting to the brokers").into(),
            }
        };
        loop {
            let command = match waiting.front() {
                Some(front) => match tokio::time::timeout_at(front.deadline, rx.recv()).await {
                    Ok(command) => command,
                    Err(_) => {
                        // Drop every batch that has waited the whole
                        // deadline, oldest first.
                        let reason = not_connected(&last_error);
                        let now = Instant::now();
                        while waiting.front().is_some_and(|w| w.deadline <= now) {
                            let batch = waiting.pop_front().expect("front checked");
                            let _ = report.send(dropped(batch.records, reason.clone()));
                        }
                        continue;
                    }
                },
                None => rx.recv().await,
            };
            match command {
                Some(Command::Records(records)) => match &producer {
                    Some(producer) => {
                        let _ = report.send(dispatch(name, producer, records));
                    }
                    None => waiting.push_back(Waiting {
                        deadline: Instant::now() + config.retry,
                        records,
                    }),
                },
                Some(Command::Connected(connected)) => {
                    for batch in waiting.drain(..) {
                        let _ = report.send(dispatch(name, &connected, batch.records));
                    }
                    producer = Some(connected);
                }
                // Close is queued behind every accepted record, and nothing
                // is accepted after it. Records still waiting for a producer
                // are dropped now rather than held for the deadline.
                Some(Command::Close) | None => {
                    let reason = not_connected(&last_error);
                    for batch in waiting.drain(..) {
                        let _ = report.send(dropped(batch.records, reason.clone()));
                    }
                    // Dropping `report` ends the reporter after the last
                    // produce call in flight.
                    return;
                }
            }
        }
    }

    /// Results for records that never reach a producer.
    fn dropped(records: Vec<Pending>, reason: Arc<str>) -> Batch {
        let seqs: Vec<u64> = records.iter().map(|p| p.seq).collect();
        let bytes = records.iter().map(|p| p.bytes).sum();
        let results = records
            .into_iter()
            .map(|p| {
                let reason = match p.message {
                    Ok(_) => reason.clone(),
                    Err(refused) => refused,
                };
                (p.seq, Delivery::Dropped { reason })
            })
            .collect();
        Batch {
            seqs,
            bytes,
            results: futures_util::future::ready(results).boxed(),
        }
    }

    /// Start one produce call for `records` and return its batch. Records
    /// that could not be encoded are dropped in their place.
    fn dispatch(name: &'static str, producer: &Arc<dyn Produce>, records: Vec<Pending>) -> Batch {
        let seqs: Vec<u64> = records.iter().map(|p| p.seq).collect();
        let bytes = records.iter().map(|p| p.bytes).sum();
        let mut slots = Vec::with_capacity(records.len());
        let mut messages = Vec::with_capacity(records.len());
        for Pending { seq, message, .. } in records {
            match message {
                Ok(message) => {
                    messages.push(message);
                    slots.push((seq, None));
                }
                Err(reason) => slots.push((seq, Some(reason))),
            }
        }
        let count = messages.len();
        // Spawned so calls overlap: the reporter polls only the oldest.
        let produced = (!messages.is_empty()).then(|| tokio::spawn(producer.produce(messages)));
        let results = async move {
            let mut produced = match produced {
                None => Vec::new(),
                Some(call) => match call.await {
                    Ok(results) => results,
                    Err(error) => {
                        tracing::warn!(sink = name, %error, records = count, "export produce task failed");
                        Vec::new()
                    }
                },
            }
            .into_iter();
            let mut short = false;
            let results: Vec<(u64, Delivery)> = slots
                .into_iter()
                .map(|(seq, refused)| {
                    let delivery = match refused {
                        Some(reason) => Delivery::Dropped { reason },
                        None => match produced.next() {
                            Some(Ok(object)) => Delivery::Acknowledged { object },
                            Some(Err(reason)) => Delivery::Dropped { reason },
                            None => {
                                short = true;
                                Delivery::Dropped {
                                    reason: format!("{name} producer returned no result").into(),
                                }
                            }
                        },
                    };
                    (seq, delivery)
                })
                .collect();
            if short {
                tracing::warn!(
                    sink = name,
                    records = count,
                    "export produce returned too few results"
                );
            }
            let dropped = results
                .iter()
                .filter(|(_, delivery)| !delivery.is_acknowledged())
                .count();
            if dropped > 0 {
                tracing::warn!(
                    sink = name,
                    dropped,
                    records = results.len(),
                    "export records dropped"
                );
            }
            results
        };
        Batch {
            seqs,
            bytes,
            results: results.boxed(),
        }
    }

    /// Report batches in submission order, releasing their bytes as they go.
    async fn report(
        name: &'static str,
        mut rx: mpsc::UnboundedReceiver<Batch>,
        outcomes: mpsc::UnboundedSender<Outcome>,
        buffered: Arc<AtomicU64>,
        stopped: watch::Sender<bool>,
    ) {
        while let Some(Batch {
            seqs,
            bytes,
            results,
        }) = rx.recv().await
        {
            // A panic in the produce future must not lose results: the
            // exporter counts on one per record.
            let results = std::panic::AssertUnwindSafe(results)
                .catch_unwind()
                .await
                .unwrap_or_else(|_| {
                    seqs.iter()
                        .map(|&seq| {
                            (
                                seq,
                                Delivery::Dropped {
                                    reason: format!("{name} produce panicked").into(),
                                },
                            )
                        })
                        .collect()
                });
            buffered.fetch_sub(bytes, Ordering::Relaxed);
            let _ = outcomes.send(Outcome {
                sink: name,
                results,
            });
        }
        let _ = stopped.send(true);
    }
}

#[cfg(test)]
pub(crate) mod tests;
