// The loader is a host tool outside celld's execution boundary: its clock,
// timers, tasks and config file are the host's.
#![allow(clippy::disallowed_methods)]

//! Feeding the loader from the blob-stream topic (`blob-stream` feature).
//!
//! The loader is a member of a blob-stream consumer group, `snowflake` by
//! default, over the topic the nodes' blob-stream sink writes. [`BlobStream`]
//! is the consumer iterator as a [`Source`], and [`run`] is
//! [`crate::source::run`] over it: batching, landing, and committing offsets
//! only after a batch lands are the same for every transport. A message's
//! source is `blob-stream/<virtual partition>/<offset>`.
//!
//! The consumer's own settings (topic, S3 and DynamoDB, broker discovery) are
//! blob-stream's `ConsumerIteratorBootstrapConfig`, read by
//! [`bootstrap_config`] from a YAML or JSON file in the same form as the
//! brokers' config.

use std::path::Path;

use anyhow::{bail, Context as _};
use blob_stream_consumer::iterator::{ConsumerIterator, NextResult, RevokedPartitions};
use blob_stream_consumer::ConsumerConfigFactory;
use blob_stream_proto::protos::blobstream::v1::config::ConsumerIteratorBootstrapConfig;
use tokio_util::sync::CancellationToken;

use crate::consume::Land;
use crate::loader::{Loader, Warehouse};
pub use crate::source::{Event, Settings, DEFAULT_GROUP};
use crate::source::{Next, Revoked, Source};

/// The transport's name, as a message's source spells it.
pub const NAME: &str = "blob-stream";

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

/// Consume the blob-stream topic until `stop` is cancelled or the consumer
/// fails: [`crate::source::run`] over `iterator`.
///
/// Must run on a multi-threaded Tokio runtime: Snowflake requests block,
/// and run in place on this task's thread.
pub async fn run<W: Warehouse, L: Land>(
    iterator: Box<dyn ConsumerIterator>,
    loader: &mut Loader<W>,
    lander: &mut L,
    settings: &Settings,
    stop: CancellationToken,
    report: impl FnMut(Event<'_>),
) -> anyhow::Result<()> {
    crate::source::run(BlobStream(iterator), loader, lander, settings, stop, report).await
}

/// A blob-stream consumer iterator as a [`Source`].
pub struct BlobStream(pub Box<dyn ConsumerIterator>);

/// Partitions a blob-stream group revoked.
pub struct Revocation(Box<dyn RevokedPartitions>);

impl Revoked for Revocation {
    fn partitions(&self) -> Vec<u32> {
        self.0.partitions()
    }

    async fn complete(self) {
        self.0.complete().await;
    }
}

impl Source for BlobStream {
    type Revoked = Revocation;

    fn name(&self) -> &'static str {
        NAME
    }

    fn start(&mut self) -> anyhow::Result<()> {
        self.0.start()
    }

    async fn next(&mut self) -> anyhow::Result<Next<Revocation>> {
        Ok(match self.0.next().await? {
            NextResult::Record(r) => Next::Record {
                partition: r.virtual_partition_id,
                offset: r.offset,
                payload: r.record.payload.to_vec(),
            },
            NextResult::Revoked(revoked) => Next::Revoked(Revocation(revoked)),
        })
    }

    fn store_offset(&mut self, partition: u32, offset: u64) -> anyhow::Result<()> {
        self.0.store_offset(partition, offset)
    }

    async fn commit(&mut self) -> anyhow::Result<Vec<u32>> {
        Ok(self.0.commit().await?.fenced_partitions)
    }

    async fn shutdown(self) -> anyhow::Result<()> {
        self.0.shutdown().await
    }
}

#[cfg(test)]
mod tests;
