// The loader is a host tool outside celld's execution boundary: its
// threads, clock and config file are the host's.
#![allow(clippy::disallowed_methods)]

//! Feeding the loader from a Kafka topic (`kafka` feature).
//!
//! The loader joins a Kafka consumer group, `snowflake` by default, over the
//! topic the nodes' Kafka sink writes. [`KafkaSource`] is that consumer as a
//! [`Source`], so [`crate::source::run`] batches, lands and commits exactly
//! as it does for blob-stream. A message's source is
//! `kafka/<partition>/<offset>`, and `EXPORT_SKIP` names messages the same
//! way.
//!
//! Offsets are never committed automatically: the loop commits a batch's
//! offsets once the batch has landed, as Kafka's next-offset convention has
//! it (the landed offset plus one).
//!
//! librdkafka runs a group member's rebalance callbacks on the thread that
//! polls it, so a dedicated thread polls and hands messages to the loop
//! over a bounded channel. When the group revokes partitions, that thread
//! passes the revocation along and waits until the loop has landed and
//! committed what it read from them, so the next owner starts where this
//! member stopped.

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer as _, ConsumerContext, Rebalance};
use rdkafka::error::KafkaError;
use rdkafka::{ClientConfig, ClientContext, Message as _, Offset, TopicPartitionList};
use tokio::sync::{mpsc, oneshot};

use crate::source::{Next, Revoked, Source, DEFAULT_GROUP};

/// The transport's name, as a message's source spells it.
pub const NAME: &str = "kafka";

/// The topic the nodes write unless `CELLD_EXPORT_TOPIC` says otherwise.
pub const DEFAULT_TOPIC: &str = "celld-changes";

/// Messages read ahead of the loop. The poll thread waits when the loop
/// falls this far behind.
const READ_AHEAD: usize = 1024;

/// How long one poll waits, and so how quickly the poll thread notices a
/// shutdown.
const POLL: Duration = Duration::from_millis(100);

/// The consumer's settings.
#[derive(Clone, Debug)]
pub struct Settings {
    pub topic: String,
    /// Every librdkafka property the consumer is created with, in order:
    /// the loader's own, then the properties file's.
    pub properties: Vec<(String, String)>,
}

impl Settings {
    /// Settings for `brokers` and `topic`. `group` defaults to
    /// [`DEFAULT_GROUP`]; `member`, when given, is the client id brokers log
    /// this loader as. `properties` names a file of librdkafka properties
    /// applied over the loader's own.
    pub fn new(
        brokers: &str,
        topic: Option<&str>,
        group: Option<&str>,
        member: Option<&str>,
        properties: Option<&Path>,
    ) -> anyhow::Result<Settings> {
        let mut all: Vec<(String, String)> = [
            ("bootstrap.servers", brokers),
            ("group.id", group.unwrap_or(DEFAULT_GROUP)),
            ("client.id", member.unwrap_or("celld-export-loader")),
            // Offsets are committed by the loop, only once a batch lands.
            ("enable.auto.commit", "false"),
            ("enable.auto.offset.store", "false"),
            // A new group starts at the beginning of the topic's retention.
            ("auto.offset.reset", "earliest"),
        ]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
        if let Some(path) = properties {
            let text = std::fs::read_to_string(path)
                .with_context(|| format!("read EXPORT_KAFKA_PROPERTIES {}", path.display()))?;
            all.extend(
                parse_properties(&text)
                    .with_context(|| format!("EXPORT_KAFKA_PROPERTIES {}", path.display()))?,
            );
        }
        Ok(Settings {
            topic: topic.unwrap_or(DEFAULT_TOPIC).to_string(),
            properties: all,
        })
    }
}

/// librdkafka properties, one `name=value` per line. Blank lines and lines
/// starting with `#` or `!` are comments.
pub fn parse_properties(text: &str) -> anyhow::Result<Vec<(String, String)>> {
    let mut properties = Vec::new();
    for (number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') || line.starts_with('!') {
            continue;
        }
        let Some((name, value)) = line.split_once('=') else {
            bail!("line {}: expected name=value", number + 1);
        };
        let (name, value) = (name.trim(), value.trim());
        if name.is_empty() {
            bail!("line {}: a property needs a name", number + 1);
        }
        // An automatic commit would move past records that have not landed.
        if name == "enable.auto.commit" && value != "false" {
            bail!(
                "line {}: enable.auto.commit must stay false; the loader commits offsets \
                 only once their batch has landed",
                number + 1
            );
        }
        properties.push((name.to_string(), value.to_string()));
    }
    Ok(properties)
}

/// What the poll thread hands the loop.
enum Polled {
    Record {
        partition: u32,
        offset: u64,
        payload: Vec<u8>,
    },
    Revoked(Vec<u32>, oneshot::Sender<()>),
    Failed(KafkaError),
    Fatal(KafkaError),
}

/// Passes revocations to the loop and waits for it to let go.
struct Context {
    topic: String,
    polled: mpsc::Sender<Polled>,
}

impl ClientContext for Context {}

impl ConsumerContext for Context {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(list) = rebalance else {
            return;
        };
        let partitions: Vec<u32> = list
            .elements_for_topic(&self.topic)
            .iter()
            .filter_map(|e| u32::try_from(e.partition()).ok())
            .collect();
        if partitions.is_empty() {
            return;
        }
        let (done, landed) = oneshot::channel();
        // Runs on the poll thread, which is no runtime's. When the loop is
        // gone, both fail at once and the partitions are let go as they are.
        if self
            .polled
            .blocking_send(Polled::Revoked(partitions, done))
            .is_ok()
        {
            let _ = landed.blocking_recv();
        }
    }
}

/// A Kafka consumer-group member as a [`Source`].
pub struct KafkaSource {
    settings: Settings,
    consumer: Option<Arc<BaseConsumer<Context>>>,
    polled: Option<mpsc::Receiver<Polled>>,
    stop: Arc<AtomicBool>,
    poller: Option<std::thread::JoinHandle<()>>,
    /// The highest offset done with per partition, since the last commit.
    stored: BTreeMap<u32, u64>,
}

impl KafkaSource {
    pub fn new(settings: Settings) -> KafkaSource {
        KafkaSource {
            settings,
            consumer: None,
            polled: None,
            stop: Arc::new(AtomicBool::new(false)),
            poller: None,
            stored: BTreeMap::new(),
        }
    }
}

/// Partitions a Kafka group revoked. The poll thread lets them go once
/// [`Revoked::complete`] is called.
pub struct Revocation {
    partitions: Vec<u32>,
    done: oneshot::Sender<()>,
}

impl Revoked for Revocation {
    fn partitions(&self) -> Vec<u32> {
        self.partitions.clone()
    }

    async fn complete(self) {
        let _ = self.done.send(());
    }
}

impl Source for KafkaSource {
    type Revoked = Revocation;

    fn name(&self) -> &'static str {
        NAME
    }

    fn start(&mut self) -> anyhow::Result<()> {
        let (tx, rx) = mpsc::channel(READ_AHEAD);
        let mut config = ClientConfig::new();
        for (name, value) in &self.settings.properties {
            config.set(name, value);
        }
        let consumer: BaseConsumer<Context> = config
            .create_with_context(Context {
                topic: self.settings.topic.clone(),
                polled: tx.clone(),
            })
            .context("create the Kafka consumer")?;
        consumer
            .subscribe(&[&self.settings.topic])
            .with_context(|| format!("subscribe to Kafka topic {:?}", self.settings.topic))?;
        let consumer = Arc::new(consumer);
        let polling = consumer.clone();
        let stop = self.stop.clone();
        let topic = self.settings.topic.clone();
        let poller = std::thread::Builder::new()
            .name("export-kafka-poll".into())
            .spawn(move || poll(&polling, &topic, &tx, &stop))
            .context("start the Kafka poll thread")?;
        self.consumer = Some(consumer);
        self.polled = Some(rx);
        self.poller = Some(poller);
        Ok(())
    }

    async fn next(&mut self) -> anyhow::Result<Next<Revocation>> {
        let polled = self
            .polled
            .as_mut()
            .context("the Kafka consumer is not started")?;
        match polled.recv().await {
            None => Err(anyhow!("the Kafka poll thread stopped")),
            Some(Polled::Record {
                partition,
                offset,
                payload,
            }) => Ok(Next::Record {
                partition,
                offset,
                payload,
            }),
            Some(Polled::Revoked(partitions, done)) => {
                Ok(Next::Revoked(Revocation { partitions, done }))
            }
            Some(Polled::Failed(e)) => Ok(Next::Failed(anyhow!(e).context("read from Kafka"))),
            Some(Polled::Fatal(e)) => Err(anyhow!(e).context("the Kafka consumer failed")),
        }
    }

    fn store_offset(&mut self, partition: u32, offset: u64) -> anyhow::Result<()> {
        let highest = self.stored.entry(partition).or_insert(offset);
        *highest = (*highest).max(offset);
        Ok(())
    }

    async fn commit(&mut self) -> anyhow::Result<Vec<u32>> {
        // Taken whether or not the commit succeeds: offsets a failed commit
        // carried may belong to partitions another member owns now, and
        // committing them later could move that member back.
        let stored = std::mem::take(&mut self.stored);
        if stored.is_empty() {
            return Ok(Vec::new());
        }
        let consumer = self
            .consumer
            .clone()
            .context("the Kafka consumer is not started")?;
        let mut list = TopicPartitionList::new();
        for (&partition, &offset) in &stored {
            let partition = i32::try_from(partition).context("a Kafka partition out of range")?;
            let next = i64::try_from(offset + 1).context("a Kafka offset out of range")?;
            list.add_partition_offset(&self.settings.topic, partition, Offset::Offset(next))?;
        }
        tokio::task::spawn_blocking(move || consumer.commit(&list, CommitMode::Sync))
            .await
            .context("the Kafka commit task failed")?
            .context("commit Kafka offsets")?;
        Ok(Vec::new())
    }

    async fn shutdown(mut self) -> anyhow::Result<()> {
        // Stop reading first, so a revocation the close raises does not wait
        // on a loop that has finished.
        self.polled = None;
        self.stop.store(true, Ordering::Relaxed);
        // Whichever of this and the poll thread drops the consumer last
        // closes it, leaving the group, which polls and blocks: never on a
        // runtime thread.
        let consumer = self.consumer.take();
        let poller = self.poller.take();
        tokio::task::spawn_blocking(move || {
            drop(consumer);
            poller.map_or(Ok(()), |p| p.join())
        })
        .await
        .context("the Kafka shutdown task failed")?
        .map_err(|_| anyhow!("the Kafka poll thread panicked"))
    }
}

/// The poll thread: read until stopped or the loop is gone.
fn poll(
    consumer: &Arc<BaseConsumer<Context>>,
    topic: &str,
    polled: &mpsc::Sender<Polled>,
    stop: &AtomicBool,
) {
    while !stop.load(Ordering::Relaxed) {
        let next = match consumer.poll(POLL) {
            None => continue,
            Some(Ok(message)) => {
                if message.topic() != topic {
                    continue;
                }
                let (Ok(partition), Ok(offset)) = (
                    u32::try_from(message.partition()),
                    u64::try_from(message.offset()),
                ) else {
                    continue;
                };
                Polled::Record {
                    partition,
                    offset,
                    // An empty message is not a record; the loop says so.
                    payload: message.payload().unwrap_or_default().to_vec(),
                }
            }
            Some(Err(e @ KafkaError::MessageConsumptionFatal(_))) => Polled::Fatal(e),
            Some(Err(e)) => Polled::Failed(e),
        };
        if polled.blocking_send(next).is_err() {
            return;
        }
    }
}

#[cfg(test)]
mod tests;
