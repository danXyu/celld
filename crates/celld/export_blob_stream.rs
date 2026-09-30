// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Export sinks are shell tasks outside the execution boundary, like telemetry.
#![allow(clippy::disallowed_methods)]

//! The blob-stream export sink (`docs/design/change-export.md#blob-stream`).
//!
//! A [`crate::export_topic`] sink whose producer is `blob-stream-producer`.
//! The producer acknowledges a message only once the broker has written its
//! segment to S3 and the segment's metadata row, so an acknowledged record
//! is durable in object storage. The producer needs the brokers' membership
//! before its first produce, which the topic sink's background connect
//! waits for.
//!
//! The client crates are behind the `export-blob-stream` Cargo feature.
//! Without it, [`start`] refuses, and a node configured for this sink does
//! not start.

use crate::export::Config;
use crate::export_sink::ExportSink;
use crate::export_sink::Outcome;
#[cfg(feature = "export-blob-stream")]
use crate::export_topic::Connect;
#[cfg(feature = "export-blob-stream")]
use crate::export_topic::TopicSink;
#[cfg(feature = "export-blob-stream")]
use crate::export_topic::TopicSinkConfig;
use anyhow::bail;
use std::sync::Arc;
use tokio::sync::mpsc;

/// The sink's name, as outcomes and logs carry it.
pub const NAME: &str = "blob-stream";

/// Start the blob-stream sink for `config`. Must be called inside a Tokio
/// runtime. Fails when the build does not carry the sink.
#[cfg(feature = "export-blob-stream")]
pub fn start(
    config: &Config,
    outcomes: mpsc::UnboundedSender<Outcome>,
) -> anyhow::Result<Arc<dyn ExportSink>> {
    let settings = client::Settings::from_config(config)?;
    tracing::info!(
        topic = %settings.topic,
        brokers = %settings.brokers,
        writer_id = settings.writer_id,
        partitions = settings.partitions,
        writers = settings.writers,
        retry_ms = settings.retry.as_millis() as u64,
        "export blob-stream sink on"
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

/// Start the blob-stream sink for `config`. This build does not carry it.
#[cfg(not(feature = "export-blob-stream"))]
pub fn start(
    _config: &Config,
    _outcomes: mpsc::UnboundedSender<Outcome>,
) -> anyhow::Result<Arc<dyn ExportSink>> {
    bail!(
        "CELLD_EXPORT_SINK=blob-stream needs a celld built with the export-blob-stream \
         feature (cargo build --features export-blob-stream); this build does not carry \
         its client"
    )
}

/// The `blob-stream-producer` client.
#[cfg(feature = "export-blob-stream")]
mod client {
    use super::*;
    use crate::export_topic::Message;
    use crate::export_topic::Produce;
    use anyhow::Context as _;
    use blob_stream_producer::ProducerClient;
    use blob_stream_producer::ProducerClientImpl;
    use blob_stream_producer::ProducerConfig;
    use blob_stream_producer::ProducerDiscoveryConfig;
    use blob_stream_producer::ProducerNodeConfig;
    use blob_stream_producer::ProducerRecord;
    use blob_stream_producer::ProducerRuntimeConfig;
    use blob_stream_producer::ProducerTopicConfig;
    use blob_stream_proto::protos::blobstream::v1::config::K8sServiceBrokerDiscoveryConfig;
    use blob_stream_proto::protos::blobstream::v1::config::StaticBrokerDiscoveryConfig;
    use blob_stream_types::ToProtoDuration as _;
    use futures_util::future::BoxFuture;
    use futures_util::FutureExt as _;
    use std::time::Duration;

    /// Producer settings from the export config.
    #[derive(Clone, Debug)]
    pub struct Settings {
        pub topic: String,
        pub brokers: String,
        pub writer_id: u32,
        pub partitions: u32,
        pub writers: u32,
        pub retry: Duration,
    }

    impl Settings {
        pub fn from_config(config: &Config) -> anyhow::Result<Settings> {
            let Some(brokers) = config.brokers.clone() else {
                bail!("CELLD_EXPORT_SINK includes blob-stream but CELLD_EXPORT_BROKERS is unset");
            };
            let Some(partitions) = config.partitions else {
                bail!(
                    "CELLD_EXPORT_SINK includes blob-stream but CELLD_EXPORT_PARTITIONS is unset"
                );
            };
            Ok(Settings {
                topic: config.topic.clone(),
                brokers,
                writer_id: config.blob_stream_writer_id()?,
                partitions,
                writers: config.blob_stream_writers(),
                retry: config.retry,
            })
        }

        /// The producer's runtime config. Batching, timeouts and concurrency
        /// keep the client's defaults.
        pub fn runtime(&self) -> anyhow::Result<ProducerRuntimeConfig> {
            let mut producer = ProducerConfig::new();
            producer.writer_id = Some(self.writer_id);
            producer.retry_deadline = self.retry.into_proto();

            let mut discovery = ProducerDiscoveryConfig::new();
            match self.brokers.strip_prefix("k8s://") {
                Some(service) => {
                    let (namespace, service_name) = service
                        .split_once('/')
                        .context("CELLD_EXPORT_BROKERS must be k8s://NAMESPACE/SERVICE")?;
                    let mut k8s = K8sServiceBrokerDiscoveryConfig::new();
                    k8s.namespace = namespace.to_string().into();
                    k8s.service_name = service_name.to_string().into();
                    discovery.set_k8s_service(k8s);
                }
                None => {
                    let mut nodes = StaticBrokerDiscoveryConfig::new();
                    for broker in self.brokers.split(',').map(str::trim) {
                        if broker.is_empty() {
                            continue;
                        }
                        // The broker's own node ID: the producer assigns
                        // partition owners by it.
                        let (node_id, address) = broker.split_once('=').with_context(|| {
                            format!("CELLD_EXPORT_BROKERS entries must be NODE_ID=host:port, not {broker:?}")
                        })?;
                        let mut node = ProducerNodeConfig::new();
                        node.node_id = node_id.trim().to_string().into();
                        node.address = address.trim().to_string().into();
                        nodes.nodes.push(node);
                    }
                    discovery.set_static(nodes);
                }
            }

            let mut topic = ProducerTopicConfig::new();
            topic.name = self.topic.clone().into();
            topic.partition_count = self.partitions;
            topic.num_writers = self.writers;
            // The producer routes by partition and writer counts only;
            // retention is required by the topic schema but drives the
            // brokers' storage, not a producer, so any positive value is
            // equivalent here.
            topic.retention = Duration::from_secs(24 * 60 * 60).into_proto();

            let mut runtime = ProducerRuntimeConfig::new();
            runtime.producer = Some(producer).into();
            runtime.discovery = Some(discovery).into();
            runtime.topics.push(topic);
            Ok(runtime)
        }
    }

    /// Connect a producer: validate the config, discover the brokers, and
    /// wait for their first membership.
    pub async fn connect(settings: &Settings) -> anyhow::Result<Arc<dyn Produce>> {
        let runtime = settings.runtime()?;
        let scope = bd_server_stats::stats::Collector::default().scope("celld_export_blob_stream");
        let producer = ProducerClientImpl::from_runtime_config(runtime, scope)
            .await
            .context("start the blob-stream producer")?;
        Ok(Arc::new(Client {
            producer: Arc::new(producer),
            topic: settings.topic.clone(),
        }))
    }

    struct Client {
        producer: Arc<ProducerClientImpl>,
        topic: String,
    }

    impl Produce for Client {
        fn produce(
            &self,
            messages: Vec<Message>,
        ) -> BoxFuture<'static, Vec<Result<Arc<str>, Arc<str>>>> {
            let producer = self.producer.clone();
            let records: Vec<ProducerRecord> = messages
                .into_iter()
                .map(|m| {
                    ProducerRecord::new(self.topic.clone().into(), m.key, m.payload, m.event_ts_ms)
                })
                .collect();
            async move {
                producer
                    .produce(records)
                    .await
                    .into_iter()
                    .map(|result| match result {
                        Ok(ack) => Ok(Arc::from(format!(
                            "{}/{}",
                            ack.topic, ack.virtual_partition_id
                        ))),
                        Err(error) => Err(Arc::from(error.to_string())),
                    })
                    .collect()
            }
            .boxed()
        }
    }
}

#[cfg(test)]
mod tests;
