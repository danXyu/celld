// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Export sinks are shell tasks outside the execution boundary, like telemetry.
#![allow(clippy::disallowed_methods)]

//! The Kafka export sink (`docs/design/change-export.md#kafka`).
//!
//! A [`crate::export_topic`] sink whose producer is librdkafka, through
//! `rdkafka`. Each record is one message on `CELLD_EXPORT_TOPIC`, keyed by
//! its stream, so Kafka's partitioner keeps a stream in one partition. The
//! producer runs with `acks=all` and idempotence, so a message is
//! acknowledged only once every in-sync replica has it, and a retry neither
//! duplicates nor reorders it within its partition. `CELLD_EXPORT_RETRY_MS`
//! is the delivery timeout: a record still unacknowledged then is dropped,
//! and the exporter freezes the stream's delivered position with a `gap`.
//!
//! Connecting creates the producer and fetches the topic's metadata, so a
//! missing topic or unreachable cluster shows up as the reason records are
//! dropped rather than as librdkafka's own retries.
//!
//! The producer never asks the brokers to create a topic, and its message
//! limit is `CELLD_EXPORT_MAX_RECORD_BYTES` plus [`MESSAGE_OVERHEAD`], so any
//! record the exporter emits fits. The topic's own `max.message.bytes` must
//! allow the same.
//!
//! `CELLD_EXPORT_KAFKA_PROPERTIES` names a file of librdkafka properties,
//! applied over the sink's own: TLS, SASL, compression, batching. It may not
//! weaken `acks`, move the delivery deadline, silence successful delivery
//! reports, allow topic creation, or lower the message limit below a record.
//!
//! The client is behind the `export-kafka` Cargo feature. Without it,
//! [`start`] refuses, and a node configured for this sink does not start.

use crate::export::Config;
use crate::export_sink::ExportSink;
use crate::export_sink::Outcome;
use anyhow::bail;
use anyhow::Context as _;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

/// The sink's name, as outcomes and logs carry it.
pub const NAME: &str = "kafka";

/// Room in the producer's message limit beyond `CELLD_EXPORT_MAX_RECORD_BYTES`,
/// for the key, the timestamp and Kafka's framing.
pub const MESSAGE_OVERHEAD: usize = 64 * 1024;

/// librdkafka's floor and ceiling for `message.max.bytes`.
const MESSAGE_MAX_BYTES: std::ops::RangeInclusive<usize> = 1_000_000..=1_000_000_000;

/// Start the Kafka sink for `config`. Must be called inside a Tokio runtime.
/// Fails when the build does not carry the sink or its properties file
/// cannot be read.
#[cfg(feature = "export-kafka")]
pub fn start(
    config: &Config,
    outcomes: mpsc::UnboundedSender<Outcome>,
) -> anyhow::Result<Arc<dyn ExportSink>> {
    use crate::export_topic::Connect;
    use crate::export_topic::TopicSink;
    use crate::export_topic::TopicSinkConfig;

    let settings = Settings::from_config(config)?;
    tracing::info!(
        topic = %settings.topic,
        brokers = %settings.brokers,
        retry_ms = settings.retry.as_millis() as u64,
        properties = ?config.kafka_properties,
        "export kafka sink on"
    );
    let retry = settings.retry;
    let settings = Arc::new(settings);
    let connect: Connect = Box::new(move || {
        let settings = settings.clone();
        Box::pin(async move { client::connect(&settings).await })
    });
    Ok(Arc::new(TopicSink::start(
        NAME,
        connect,
        TopicSinkConfig {
            retry,
            ..TopicSinkConfig::default()
        },
        outcomes,
    )))
}

/// Start the Kafka sink for `config`. This build does not carry it.
#[cfg(not(feature = "export-kafka"))]
pub fn start(
    _config: &Config,
    _outcomes: mpsc::UnboundedSender<Outcome>,
) -> anyhow::Result<Arc<dyn ExportSink>> {
    bail!(
        "CELLD_EXPORT_SINK=kafka needs a celld built with the export-kafka feature \
         (cargo build --features export-kafka); this build does not carry its client"
    )
}

/// Producer settings from the export config.
#[derive(Clone, Debug)]
pub struct Settings {
    pub topic: String,
    pub brokers: String,
    pub retry: Duration,
    /// Every librdkafka property the producer is created with, in the order
    /// they are set: the sink's own, then the properties file's.
    pub properties: Vec<(String, String)>,
}

impl Settings {
    pub fn from_config(config: &Config) -> anyhow::Result<Settings> {
        let Some(brokers) = config.kafka_brokers.clone() else {
            bail!("CELLD_EXPORT_SINK includes kafka but CELLD_EXPORT_KAFKA_BROKERS is unset");
        };
        let overrides = match &config.kafka_properties {
            None => Vec::new(),
            Some(path) => {
                let text = std::fs::read_to_string(path).with_context(|| {
                    format!("read CELLD_EXPORT_KAFKA_PROPERTIES {}", path.display())
                })?;
                parse_properties(&text)
                    .with_context(|| format!("CELLD_EXPORT_KAFKA_PROPERTIES {}", path.display()))?
            }
        };
        Settings::new(
            config.topic.clone(),
            brokers,
            config.retry,
            config.max_record_bytes,
            overrides,
        )
    }

    fn new(
        topic: String,
        brokers: String,
        retry: Duration,
        max_record_bytes: usize,
        overrides: Vec<(String, String)>,
    ) -> anyhow::Result<Settings> {
        let message_max_bytes = max_record_bytes
            .saturating_add(MESSAGE_OVERHEAD)
            .clamp(*MESSAGE_MAX_BYTES.start(), *MESSAGE_MAX_BYTES.end());
        if max_record_bytes.saturating_add(MESSAGE_OVERHEAD) > message_max_bytes {
            bail!(
                "CELLD_EXPORT_MAX_RECORD_BYTES ({max_record_bytes}) is more than a Kafka \
                 message can carry"
            );
        }
        for (name, value) in &overrides {
            if name == "message.max.bytes"
                && value
                    .parse::<usize>()
                    .map_or(true, |v| v < message_max_bytes)
            {
                bail!(
                    "CELLD_EXPORT_KAFKA_PROPERTIES: message.max.bytes={value} is below \
                     {message_max_bytes}, CELLD_EXPORT_MAX_RECORD_BYTES plus framing; \
                     the largest records could never be sent"
                );
            }
        }
        let own = [
            ("bootstrap.servers", brokers.clone()),
            ("client.id", "celld-export".to_string()),
            // Acknowledged means durable: every in-sync replica has it.
            ("acks", "all".to_string()),
            // Retries neither duplicate nor reorder within a partition.
            ("enable.idempotence", "true".to_string()),
            // The whole delivery deadline, retries included.
            ("message.timeout.ms", retry.as_millis().to_string()),
            // Records are JSON; lz4 is cheap and built into librdkafka.
            ("compression.type", "lz4".to_string()),
            // Every record the exporter emits fits in one message.
            ("message.max.bytes", message_max_bytes.to_string()),
            // A missing topic is an error, not a topic with broker defaults.
            ("allow.auto.create.topics", "false".to_string()),
        ];
        let mut properties: Vec<(String, String)> = own
            .into_iter()
            .map(|(name, value)| (name.to_string(), value))
            .collect();
        properties.extend(overrides);
        Ok(Settings {
            topic,
            brokers,
            retry,
            properties,
        })
    }
}

/// librdkafka properties, one `name=value` per line. Blank lines and lines
/// starting with `#` or `!` are comments. Values are taken as written after
/// the first `=`, trimmed.
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
        // Acknowledged must mean durable, or the delivered position certifies
        // records a broker failure can still lose.
        if name == "acks" && !matches!(value, "all" | "-1") {
            bail!(
                "line {}: acks must stay all; the export counts a record delivered only \
                 once every in-sync replica has it",
                number + 1
            );
        }
        // The sink waits for every record's delivery report, and
        // CELLD_EXPORT_RETRY_MS bounds how long one takes.
        if matches!(name, "message.timeout.ms" | "delivery.timeout.ms") {
            bail!(
                "line {}: {name} is set from CELLD_EXPORT_RETRY_MS",
                number + 1
            );
        }
        if name == "delivery.report.only.error" && value != "false" {
            bail!(
                "line {}: delivery.report.only.error must stay false; the export counts a \
                 record delivered only when Kafka reports it",
                number + 1
            );
        }
        if name == "allow.auto.create.topics" && value != "false" {
            bail!(
                "line {}: allow.auto.create.topics must stay false; create the topic with \
                 the partitions and replication you want",
                number + 1
            );
        }
        properties.push((name.to_string(), value.to_string()));
    }
    Ok(properties)
}

/// The `rdkafka` client.
#[cfg(feature = "export-kafka")]
mod client {
    use super::*;
    use crate::export_topic::Message;
    use crate::export_topic::Produce;
    use futures_util::future::BoxFuture;
    use futures_util::FutureExt as _;
    use rdkafka::error::KafkaError;
    use rdkafka::error::RDKafkaErrorCode;
    use rdkafka::producer::FutureProducer;
    use rdkafka::producer::FutureRecord;
    use rdkafka::producer::Producer as _;
    use rdkafka::ClientConfig;

    /// How long connecting waits for the topic's metadata.
    const METADATA_TIMEOUT: Duration = Duration::from_secs(10);

    /// How long a send waits before trying again while librdkafka's queue
    /// is full.
    const QUEUE_FULL_PAUSE: Duration = Duration::from_millis(10);

    /// How long past the retry deadline a delivery report may take before
    /// the record counts as dropped. librdkafka reports every message by
    /// `message.timeout.ms`; this only keeps a missing report from holding
    /// every later record.
    const REPORT_GRACE: Duration = Duration::from_secs(10);

    /// Create the producer and check that the cluster answers for the topic.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Arc<dyn Produce>> {
        let mut config = ClientConfig::new();
        for (name, value) in &settings.properties {
            config.set(name, value);
        }
        let producer: FutureProducer = config.create().context("create the Kafka producer")?;
        let producer = Arc::new(producer);
        let topic = settings.topic.clone();
        let checked = producer.clone();
        // Metadata requests block the calling thread.
        let partitions = tokio::task::spawn_blocking(move || -> anyhow::Result<usize> {
            let metadata = checked
                .client()
                .fetch_metadata(Some(&topic), METADATA_TIMEOUT)
                .context("fetch the Kafka topic's metadata")?;
            let Some(found) = metadata.topics().iter().find(|t| t.name() == topic) else {
                bail!("the Kafka cluster did not describe topic {topic:?}");
            };
            if let Some(error) = found.error() {
                bail!("Kafka topic {topic:?}: {}", RDKafkaErrorCode::from(error));
            }
            Ok(found.partitions().len())
        })
        .await
        .context("the Kafka metadata task failed")??;
        tracing::info!(
            topic = %settings.topic,
            partitions,
            "export kafka producer connected"
        );
        Ok(Arc::new(Client {
            producer,
            topic: settings.topic.clone(),
            retry: settings.retry,
        }))
    }

    struct Client {
        producer: Arc<FutureProducer>,
        topic: String,
        retry: Duration,
    }

    impl Produce for Client {
        fn produce(
            &self,
            messages: Vec<Message>,
        ) -> BoxFuture<'static, Vec<Result<Arc<str>, Arc<str>>>> {
            let producer = self.producer.clone();
            let topic = self.topic.clone();
            let retry = self.retry;
            async move {
                // Enqueue every message in order before awaiting any, so
                // messages of one partition reach librdkafka in submission
                // order and share its batches.
                let deadline = tokio::time::Instant::now() + retry;
                let mut deliveries = Vec::with_capacity(messages.len());
                for message in &messages {
                    loop {
                        let record = FutureRecord::to(&topic)
                            .key(&message.key[..])
                            .payload(&message.payload[..])
                            .timestamp(message.event_ts_ms);
                        match producer.send_result(record) {
                            Ok(delivery) => deliveries.push(Ok(delivery)),
                            Err((
                                KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull),
                                _,
                            )) if tokio::time::Instant::now() < deadline => {
                                tokio::time::sleep(QUEUE_FULL_PAUSE).await;
                                continue;
                            }
                            Err((error, _)) => deliveries.push(Err(Arc::from(error.to_string()))),
                        }
                        break;
                    }
                }
                let mut results = Vec::with_capacity(deliveries.len());
                for delivery in deliveries {
                    results.push(match delivery {
                        Err(refused) => Err(refused),
                        Ok(delivery) => {
                            match tokio::time::timeout_at(deadline + REPORT_GRACE, delivery).await {
                                Ok(Ok(Ok(delivered))) => Ok(Arc::from(format!(
                                    "{topic}/{}/{}",
                                    delivered.partition, delivered.offset
                                ))),
                                Ok(Ok(Err((error, _)))) => Err(Arc::from(error.to_string())),
                                Ok(Err(_)) => Err(Arc::from("the Kafka producer went away")),
                                Err(_) => Err(Arc::from(
                                    "Kafka did not report the record's delivery by the \
                                     retry deadline",
                                )),
                            }
                        }
                    });
                }
                results
            }
            .boxed()
        }
    }
}

#[cfg(test)]
mod tests;
