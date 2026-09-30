// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Change export configuration (`docs/design/change-export.md`).
//!
//! Off by default, and off is structural: `Config::from_env` answers `None`
//! and nothing downstream is constructed, so a node with `CELLD_EXPORT` unset
//! or `0` opens no session, holds no queue, and spawns no task. The values of
//! the other `CELLD_EXPORT_*` variables are still checked, the same way the
//! telemetry group is, so a typo in a unit file fails the boot that carries it
//! rather than the later one that turns export on.

use crate::telemetry::Retention;
use anyhow::anyhow;
use anyhow::bail;
use std::collections::BTreeSet;
use std::time::Duration;

/// Where the bucket sink writes, under the sink bucket.
pub const CHANGES_PREFIX: &str = "export/changes";

pub const DEFAULT_TOPIC: &str = "celld-changes";
pub const DEFAULT_MAX_TX_BYTES: usize = 4 * 1024 * 1024;
pub const DEFAULT_MAX_RECORD_BYTES: usize = 1024 * 1024;
pub const DEFAULT_QUEUE_BYTES: usize = 256 * 1024 * 1024;
pub const DEFAULT_FLUSH: Duration = Duration::from_secs(10);
pub const DEFAULT_FLUSH_BYTES: usize = 8 * 1024 * 1024;
pub const DEFAULT_RETRY: Duration = Duration::from_secs(30);
pub const DEFAULT_RECONCILE: Duration = Duration::from_secs(24 * 60 * 60);

/// Runtime-supplied classes that are never exported, whatever the allow
/// list says: a Queue broker's messages and a Workflow's state are excluded
/// by policy, and a cron cell holds only its schedule.
///
/// Workflow classes are script-scoped (`__Workflow.<script>`, see
/// `deploy::workflow_class`), so the bare name here also covers every
/// `__Workflow.`-prefixed class. See `is_never_exported`.
const NEVER_EXPORTED: &[&str] = &[
    crate::deploy::QUEUE_CLASS,
    crate::deploy::WORKFLOW_CLASS,
    celld_logic::cron::RESERVED_CLASS,
];

/// The runtime-supplied classes the default allow list adds to the
/// application's own classes.
pub const DEFAULT_RESERVED_CLASSES: &[&str] = &[crate::deploy::D1_CLASS, crate::deploy::KV_CLASS];

/// Whether `class` is one of the runtime classes export always skips.
pub fn is_never_exported(class: &str) -> bool {
    NEVER_EXPORTED.iter().any(|reserved| {
        class == *reserved
            || class
                .strip_prefix(reserved)
                .is_some_and(|rest| rest.starts_with('.'))
    })
}

/// `CELLD_EXPORT_SINK`: `bucket`, `blob-stream` or `kafka`, comma-separated.
/// A node and the export CLI run one of them; the list form is parsed so the
/// refusal can say so.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Sinks {
    pub bucket: bool,
    pub blob_stream: bool,
    pub kafka: bool,
}

impl Sinks {
    /// How many sinks the list names.
    pub fn count(&self) -> usize {
        [self.bucket, self.blob_stream, self.kafka]
            .into_iter()
            .filter(|on| *on)
            .count()
    }
}

/// `CELLD_EXPORT_CLASSES`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Classes {
    /// Unset: every application class, plus `DEFAULT_RESERVED_CLASSES`.
    Default,
    /// Exactly these classes. Never contains a never-exported class; the
    /// parser refuses one rather than drop it silently.
    Only(BTreeSet<String>),
}

#[derive(Debug)]
pub struct Config {
    pub sinks: Sinks,
    /// `CELLD_EXPORT_BUCKET`, same endpoint/region/credentials as the fleet
    /// bucket. `None` means the fleet bucket itself.
    pub bucket_override: Option<String>,
    pub classes: Classes,
    /// `CELLD_EXPORT_TABLES`: `(class, table)` pairs never exported.
    pub denied_tables: BTreeSet<(String, String)>,
    /// Session memory above which a transaction becomes `bulk`; also the
    /// largest table snapshotted inline on DDL.
    pub max_tx_bytes: usize,
    /// Fragment size.
    pub max_record_bytes: usize,
    /// Shared budget for pending commits and the node buffer.
    pub queue_bytes: usize,
    /// Bucket sink flush interval and watermark cadence.
    pub flush: Duration,
    /// Bucket sink early flush.
    pub flush_bytes: usize,
    pub retention: Retention,
    pub topic: String,
    /// `CELLD_EXPORT_BROKERS`: static `NODE_ID=host:port` brokers or one
    /// `k8s://NAMESPACE/SERVICE`. Required when the blob-stream sink is on.
    pub brokers: Option<String>,
    /// `CELLD_EXPORT_WRITER_ID`: the zone whose blob-stream writer this
    /// node produces as. `None` means the node's zone, [`Config::zone`].
    pub writer_id: Option<String>,
    /// `CELLD_ZONE`: the node's zone, a node-level setting.
    pub zone: Option<String>,
    /// `CELLD_EXPORT_ZONES`: the topic's writer zones in writer order, so a
    /// zone's writer number is its position. Every producer of a topic must
    /// list them alike. Empty means a single-writer topic, writer 0.
    pub zones: Vec<String>,
    /// `CELLD_EXPORT_PARTITIONS`: the topic's logical partition count.
    /// Every producer and consumer of a topic must agree on it, so it has no
    /// default. Required when the blob-stream sink is on.
    pub partitions: Option<u32>,
    /// `CELLD_EXPORT_KAFKA_BROKERS`: the Kafka bootstrap servers,
    /// `host:port` comma-separated. Required when the Kafka sink is on.
    pub kafka_brokers: Option<String>,
    /// `CELLD_EXPORT_KAFKA_PROPERTIES`: a file of librdkafka producer
    /// properties, `name=value` per line, applied over the sink's own
    /// (security, SASL credentials, compression).
    pub kafka_properties: Option<std::path::PathBuf>,
    /// blob-stream and Kafka retry deadline before a record counts as
    /// dropped.
    pub retry: Duration,
    /// Reconciler interval. Read by the loader deployment, not the node; it
    /// is parsed here so a node and the loader agree on one grammar.
    pub reconcile: Duration,
}

impl Config {
    /// `None` when `CELLD_EXPORT` is unset or `0` — the default, and the only
    /// zero-cost state.
    pub fn from_env() -> anyhow::Result<Option<Config>> {
        Self::from_lookup(crate::env_vars::value)
    }

    #[doc(hidden)]
    pub fn from_lookup(
        get: impl Fn(&str) -> anyhow::Result<Option<String>>,
    ) -> anyhow::Result<Option<Config>> {
        let enabled =
            crate::env_vars::parse_flag("CELLD_EXPORT", get("CELLD_EXPORT")?.as_deref(), false)?;
        let sinks = match get("CELLD_EXPORT_SINK")? {
            None => Sinks {
                bucket: true,
                blob_stream: false,
                kafka: false,
            },
            Some(list) => parse_sinks(&list)?,
        };
        let bucket_override = get("CELLD_EXPORT_BUCKET")?;
        if bucket_override.as_deref() == Some("") {
            bail!("CELLD_EXPORT_BUCKET must name a bucket; unset it for the fleet bucket");
        }
        let classes = match get("CELLD_EXPORT_CLASSES")? {
            None => Classes::Default,
            Some(list) => Classes::Only(parse_classes(&list)?),
        };
        let denied_tables = match get("CELLD_EXPORT_TABLES")? {
            None => BTreeSet::new(),
            Some(list) => parse_tables(&list)?,
        };
        let size = |name: &str, default: usize| -> anyhow::Result<usize> {
            Ok(crate::env_vars::parse_positive::<usize>(name, get(name)?)?.unwrap_or(default))
        };
        let millis = |name: &str, default: Duration| -> anyhow::Result<Duration> {
            Ok(crate::env_vars::parse_positive::<u64>(name, get(name)?)?
                .map(Duration::from_millis)
                .unwrap_or(default))
        };
        let max_tx_bytes = size("CELLD_EXPORT_MAX_TX_BYTES", DEFAULT_MAX_TX_BYTES)?;
        let max_record_bytes = size("CELLD_EXPORT_MAX_RECORD_BYTES", DEFAULT_MAX_RECORD_BYTES)?;
        let queue_bytes = size("CELLD_EXPORT_QUEUE_BYTES", DEFAULT_QUEUE_BYTES)?;
        let flush = millis("CELLD_EXPORT_FLUSH_MS", DEFAULT_FLUSH)?;
        let flush_bytes = size("CELLD_EXPORT_FLUSH_BYTES", DEFAULT_FLUSH_BYTES)?;
        let retry = millis("CELLD_EXPORT_RETRY_MS", DEFAULT_RETRY)?;
        let retention = match get("CELLD_EXPORT_RETENTION")?.as_deref() {
            None | Some("none") => Retention::None,
            Some(value) => {
                let days = value
                    .strip_suffix('d')
                    .and_then(|days| days.parse::<u32>().ok())
                    .filter(|days| *days > 0)
                    .ok_or_else(|| {
                        anyhow!("CELLD_EXPORT_RETENTION must be <n>d or none: {value:?}")
                    })?;
                Retention::Days(days)
            }
        };
        let topic = non_empty("CELLD_EXPORT_TOPIC", get("CELLD_EXPORT_TOPIC")?)?
            .unwrap_or_else(|| DEFAULT_TOPIC.to_string());
        let brokers = match non_empty("CELLD_EXPORT_BROKERS", get("CELLD_EXPORT_BROKERS")?)? {
            Some(brokers) => Some(parse_brokers(&brokers)?),
            None => None,
        };
        let writer_id = match non_empty("CELLD_EXPORT_WRITER_ID", get("CELLD_EXPORT_WRITER_ID")?)? {
            Some(zone) => Some(parse_zone("CELLD_EXPORT_WRITER_ID", &zone)?),
            None => None,
        };
        let zone = match get("CELLD_ZONE")? {
            Some(zone) => Some(parse_zone("CELLD_ZONE", &zone)?),
            None => None,
        };
        let zones = match get("CELLD_EXPORT_ZONES")? {
            None => Vec::new(),
            Some(list) => parse_zones(&list)?,
        };
        let partitions = crate::env_vars::parse_positive::<u32>(
            "CELLD_EXPORT_PARTITIONS",
            get("CELLD_EXPORT_PARTITIONS")?,
        )?;
        let kafka_brokers = match non_empty(
            "CELLD_EXPORT_KAFKA_BROKERS",
            get("CELLD_EXPORT_KAFKA_BROKERS")?,
        )? {
            Some(brokers) => Some(parse_kafka_brokers(&brokers)?),
            None => None,
        };
        let kafka_properties = non_empty(
            "CELLD_EXPORT_KAFKA_PROPERTIES",
            get("CELLD_EXPORT_KAFKA_PROPERTIES")?,
        )?
        .map(std::path::PathBuf::from);
        let reconcile = match get("CELLD_EXPORT_RECONCILE")? {
            None => DEFAULT_RECONCILE,
            Some(value) => parse_interval("CELLD_EXPORT_RECONCILE", &value)?,
        };
        if !enabled {
            return Ok(None);
        }
        // Cross-field rules bind only a node that will act on them, so an
        // operator can stage the group with export still off.
        if sinks.blob_stream && brokers.is_none() {
            bail!("CELLD_EXPORT_SINK includes blob-stream but CELLD_EXPORT_BROKERS is unset");
        }
        if sinks.blob_stream {
            let Some(partitions) = partitions else {
                bail!(
                    "CELLD_EXPORT_SINK includes blob-stream but CELLD_EXPORT_PARTITIONS is unset; \
                     set it to the topic's partition count"
                );
            };
            let writers = writer_count(&zones);
            if partitions.checked_mul(writers).is_none() {
                bail!(
                    "CELLD_EXPORT_PARTITIONS ({partitions}) times the {writers} zones of \
                     CELLD_EXPORT_ZONES overflows the topic's partition space"
                );
            }
            blob_stream_writer_id(writer_id.as_deref(), zone.as_deref(), &zones)?;
        }
        if sinks.kafka && kafka_brokers.is_none() {
            bail!("CELLD_EXPORT_SINK includes kafka but CELLD_EXPORT_KAFKA_BROKERS is unset");
        }
        if max_record_bytes > queue_bytes {
            bail!(
                "CELLD_EXPORT_MAX_RECORD_BYTES ({max_record_bytes}) exceeds \
                 CELLD_EXPORT_QUEUE_BYTES ({queue_bytes}); one fragment must fit the budget"
            );
        }
        Ok(Some(Config {
            sinks,
            bucket_override,
            classes,
            denied_tables,
            max_tx_bytes,
            max_record_bytes,
            queue_bytes,
            flush,
            flush_bytes,
            retention,
            topic,
            brokers,
            writer_id,
            zone,
            zones,
            partitions,
            kafka_brokers,
            kafka_properties,
            retry,
            reconcile,
        }))
    }

    /// The blob-stream writer number: the position of this node's writer
    /// zone in `CELLD_EXPORT_ZONES`. Checked when the config is read, so
    /// this only fails for a config that does not use the blob-stream sink.
    pub fn blob_stream_writer_id(&self) -> anyhow::Result<u32> {
        blob_stream_writer_id(self.writer_id.as_deref(), self.zone.as_deref(), &self.zones)
    }

    /// The topic's writer count: one per zone, or one for a topic without
    /// zones.
    pub fn blob_stream_writers(&self) -> u32 {
        writer_count(&self.zones)
    }

    /// Whether cells of `class` are exported. A facet asks with its root's
    /// class.
    pub fn exports_class(&self, class: &str) -> bool {
        if is_never_exported(class) {
            return false;
        }
        match &self.classes {
            Classes::Default => {
                DEFAULT_RESERVED_CLASSES.contains(&class)
                    || !crate::deploy::RESERVED_CLASSES.contains(&class)
            }
            Classes::Only(classes) => classes.contains(class),
        }
    }

    /// Whether the operator's deny list names `table` of `class`. The
    /// built-in exclusions (`_cf_`, `sqlite_`, control tables) are the
    /// capture filter's, not this list's.
    pub fn denies_table(&self, class: &str, table: &str) -> bool {
        self.denied_tables
            .contains(&(class.to_string(), table.to_string()))
    }
}

fn writer_count(zones: &[String]) -> u32 {
    // parse_zones caps the list far below u32::MAX.
    zones.len().max(1) as u32
}

/// The writer zone is `CELLD_EXPORT_WRITER_ID`, else the node's zone.
fn blob_stream_writer_id(
    writer_id: Option<&str>,
    zone: Option<&str>,
    zones: &[String],
) -> anyhow::Result<u32> {
    if zones.is_empty() {
        if writer_id.is_some() {
            bail!(
                "CELLD_EXPORT_WRITER_ID names a zone but CELLD_EXPORT_ZONES is unset; list the \
                 topic's zones, or unset it for a single-writer topic"
            );
        }
        return Ok(0);
    }
    let (name, zone) = match (writer_id, zone) {
        (Some(zone), _) => ("CELLD_EXPORT_WRITER_ID", zone),
        (None, Some(zone)) => ("CELLD_ZONE", zone),
        (None, None) => bail!(
            "CELLD_EXPORT_ZONES lists the topic's zones, so this node needs its own: set \
             CELLD_ZONE (or CELLD_EXPORT_WRITER_ID)"
        ),
    };
    match zones.iter().position(|listed| listed == zone) {
        Some(index) => Ok(index as u32),
        None => bail!(
            "{name} is {zone:?}, which CELLD_EXPORT_ZONES does not list ({})",
            zones.join(",")
        ),
    }
}

/// A zone name: 1 to 128 ASCII letters, numbers, dots, dashes or
/// underscores, the node-name alphabet.
pub fn parse_zone(name: &str, value: &str) -> anyhow::Result<String> {
    let zone = value.trim();
    let valid = !zone.is_empty()
        && zone.len() <= 128
        && zone
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b'_'));
    if !valid {
        bail!("{name} must be a zone name (ASCII letters, numbers, '.', '-', '_'), not {value:?}");
    }
    Ok(zone.to_string())
}

fn parse_zones(list: &str) -> anyhow::Result<Vec<String>> {
    let mut zones: Vec<String> = Vec::new();
    for item in items(list) {
        let zone = parse_zone("CELLD_EXPORT_ZONES", item)?;
        if zones.contains(&zone) {
            bail!("CELLD_EXPORT_ZONES lists {zone:?} twice");
        }
        zones.push(zone);
    }
    if zones.is_empty() {
        bail!("CELLD_EXPORT_ZONES must list at least one zone; unset it for a single-writer topic");
    }
    if zones.len() > 1024 {
        bail!(
            "CELLD_EXPORT_ZONES lists {} zones; at most 1024",
            zones.len()
        );
    }
    Ok(zones)
}

fn non_empty(name: &str, value: Option<String>) -> anyhow::Result<Option<String>> {
    match value {
        Some(value) if value.trim().is_empty() => {
            bail!("{name} must not be empty; unset it for the default")
        }
        value => Ok(value),
    }
}

fn items(list: &str) -> impl Iterator<Item = &str> {
    list.split(',')
        .map(str::trim)
        .filter(|item| !item.is_empty())
}

fn parse_sinks(list: &str) -> anyhow::Result<Sinks> {
    let mut sinks = Sinks {
        bucket: false,
        blob_stream: false,
        kafka: false,
    };
    for item in items(list) {
        match item {
            "bucket" => sinks.bucket = true,
            "blob-stream" => sinks.blob_stream = true,
            "kafka" => sinks.kafka = true,
            other => {
                bail!("CELLD_EXPORT_SINK must be bucket, blob-stream or kafka, not {other:?}")
            }
        }
    }
    if sinks.count() == 0 {
        bail!("CELLD_EXPORT_SINK must be bucket, blob-stream or kafka");
    }
    Ok(sinks)
}

fn parse_classes(list: &str) -> anyhow::Result<BTreeSet<String>> {
    let mut classes = BTreeSet::new();
    for class in items(list) {
        if is_never_exported(class) {
            bail!("CELLD_EXPORT_CLASSES names {class:?}, which is never exported");
        }
        classes.insert(class.to_string());
    }
    if classes.is_empty() {
        bail!("CELLD_EXPORT_CLASSES must list at least one class; set CELLD_EXPORT=0 to export nothing");
    }
    Ok(classes)
}

/// `Class.table` entries. Split at the first `.`: an exportable class name
/// never contains one (application classes are JavaScript identifiers, and
/// the dotted runtime classes are never exported), while a SQLite table name
/// may.
fn parse_tables(list: &str) -> anyhow::Result<BTreeSet<(String, String)>> {
    let mut tables = BTreeSet::new();
    for entry in items(list) {
        let (class, table) = entry
            .split_once('.')
            .filter(|(class, table)| !class.is_empty() && !table.is_empty())
            .ok_or_else(|| {
                anyhow!("CELLD_EXPORT_TABLES entries must be Class.table, not {entry:?}")
            })?;
        tables.insert((class.to_string(), table.to_string()));
    }
    Ok(tables)
}

/// Static `NODE_ID=host:port` brokers, comma-separated, or one
/// `k8s://NAMESPACE/SERVICE`. Returned as written; the sink resolves it.
///
/// A static broker needs its node ID as the broker itself is configured
/// with it: the producer assigns partitions to owners by node ID, so an ID
/// that differs from the broker's routes writes to a broker that does not
/// hold the partition's lease.
fn parse_brokers(value: &str) -> anyhow::Result<String> {
    if let Some(service) = value.strip_prefix("k8s://") {
        let valid = service.split_once('/').is_some_and(|(namespace, name)| {
            !namespace.is_empty() && !name.is_empty() && !name.contains('/')
        });
        if !valid {
            bail!("CELLD_EXPORT_BROKERS must be k8s://NAMESPACE/SERVICE, not {value:?}");
        }
        return Ok(value.to_string());
    }
    let mut ids = BTreeSet::new();
    for broker in items(value) {
        let valid = broker.split_once('=').is_some_and(|(id, address)| {
            !id.trim().is_empty()
                && address.trim().rsplit_once(':').is_some_and(|(host, port)| {
                    !host.is_empty() && port.parse::<u16>().is_ok_and(|port| port > 0)
                })
        });
        if !valid {
            bail!("CELLD_EXPORT_BROKERS entries must be NODE_ID=host:port, not {broker:?}");
        }
        let id = broker
            .split_once('=')
            .map(|(id, _)| id.trim())
            .unwrap_or_default();
        if !ids.insert(id) {
            bail!("CELLD_EXPORT_BROKERS lists node ID {id:?} twice");
        }
    }
    Ok(value.to_string())
}

/// Kafka bootstrap servers: `host:port`, comma-separated. Returned as
/// written; librdkafka takes the same list.
fn parse_kafka_brokers(value: &str) -> anyhow::Result<String> {
    let mut any = false;
    for broker in items(value) {
        let valid = broker.rsplit_once(':').is_some_and(|(host, port)| {
            !host.is_empty() && port.parse::<u16>().is_ok_and(|port| port > 0)
        });
        if !valid {
            bail!("CELLD_EXPORT_KAFKA_BROKERS entries must be host:port, not {broker:?}");
        }
        any = true;
    }
    if !any {
        bail!("CELLD_EXPORT_KAFKA_BROKERS must list at least one host:port");
    }
    Ok(value.to_string())
}

/// `<n>s`, `<n>m`, `<n>h` or `<n>d`, with `n > 0`.
pub(crate) fn parse_interval(name: &str, value: &str) -> anyhow::Result<Duration> {
    let unit = match value.chars().last() {
        Some('s') => 1,
        Some('m') => 60,
        Some('h') => 60 * 60,
        Some('d') => 24 * 60 * 60,
        _ => bail!("{name} must be <n>s, <n>m, <n>h or <n>d: {value:?}"),
    };
    value[..value.len() - 1]
        .parse::<u64>()
        .ok()
        .filter(|count| *count > 0)
        .and_then(|count| count.checked_mul(unit))
        .map(Duration::from_secs)
        .ok_or_else(|| anyhow!("{name} must be <n>s, <n>m, <n>h or <n>d: {value:?}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn config(vars: &[(&str, &str)]) -> anyhow::Result<Option<Config>> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        Config::from_lookup(|name| Ok(vars.get(name).cloned()))
    }

    fn enabled(vars: &[(&str, &str)]) -> Config {
        let mut all = vec![("CELLD_EXPORT", "1")];
        all.extend_from_slice(vars);
        config(&all).unwrap().unwrap()
    }

    fn error(vars: &[(&str, &str)]) -> String {
        config(vars).unwrap_err().to_string()
    }

    #[test]
    fn off_by_default() {
        assert!(config(&[]).unwrap().is_none());
        assert!(config(&[("CELLD_EXPORT", "0")]).unwrap().is_none());
    }

    #[test]
    fn off_still_checks_values_but_not_cross_field_rules() {
        assert!(error(&[("CELLD_EXPORT_FLUSH_MS", "soon")]).contains("CELLD_EXPORT_FLUSH_MS"));
        // blob-stream without brokers binds only an enabled node.
        assert!(config(&[("CELLD_EXPORT_SINK", "blob-stream")])
            .unwrap()
            .is_none());
    }

    #[test]
    fn flag_is_strict() {
        assert!(error(&[("CELLD_EXPORT", "true")]).contains("must be 0 or 1"));
    }

    #[test]
    fn defaults_match_the_design() {
        let config = enabled(&[]);
        assert_eq!(
            config.sinks,
            Sinks {
                bucket: true,
                blob_stream: false,
                kafka: false,
            }
        );
        assert_eq!(config.bucket_override, None);
        assert_eq!(config.classes, Classes::Default);
        assert!(config.denied_tables.is_empty());
        assert_eq!(config.max_tx_bytes, 4_194_304);
        assert_eq!(config.max_record_bytes, 1_048_576);
        assert_eq!(config.queue_bytes, 268_435_456);
        assert_eq!(config.flush, Duration::from_millis(10_000));
        assert_eq!(config.flush_bytes, 8_388_608);
        assert_eq!(config.retention, Retention::None);
        assert_eq!(config.topic, "celld-changes");
        assert_eq!(config.brokers, None);
        assert_eq!(config.writer_id, None);
        assert_eq!(config.zone, None);
        assert!(config.zones.is_empty());
        assert_eq!(config.partitions, None);
        assert_eq!(config.kafka_brokers, None);
        assert_eq!(config.kafka_properties, None);
        assert_eq!(config.retry, Duration::from_millis(30_000));
        assert_eq!(config.reconcile, Duration::from_secs(24 * 3600));
    }

    #[test]
    fn every_variable_is_read() {
        let config = enabled(&[
            ("CELLD_EXPORT_SINK", "bucket, blob-stream,kafka"),
            ("CELLD_EXPORT_BUCKET", "changes"),
            ("CELLD_EXPORT_CLASSES", "Chat,__D1Database"),
            ("CELLD_EXPORT_TABLES", "Chat.drafts, Chat.my.table"),
            ("CELLD_EXPORT_MAX_TX_BYTES", "100"),
            ("CELLD_EXPORT_MAX_RECORD_BYTES", "10"),
            ("CELLD_EXPORT_QUEUE_BYTES", "1000"),
            ("CELLD_EXPORT_FLUSH_MS", "5"),
            ("CELLD_EXPORT_FLUSH_BYTES", "7"),
            ("CELLD_EXPORT_RETENTION", "14d"),
            ("CELLD_EXPORT_TOPIC", "prod-changes"),
            ("CELLD_EXPORT_BROKERS", "k8s://streams/blob-stream"),
            ("CELLD_EXPORT_WRITER_ID", "us-east-1c"),
            ("CELLD_ZONE", "us-east-1a"),
            ("CELLD_EXPORT_ZONES", "us-east-1a, us-east-1b,us-east-1c"),
            ("CELLD_EXPORT_PARTITIONS", "64"),
            ("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092, k2.internal:9093"),
            (
                "CELLD_EXPORT_KAFKA_PROPERTIES",
                "/etc/celld/kafka.properties",
            ),
            ("CELLD_EXPORT_RETRY_MS", "9"),
            ("CELLD_EXPORT_RECONCILE", "6h"),
        ]);
        assert_eq!(
            config.sinks,
            Sinks {
                bucket: true,
                blob_stream: true,
                kafka: true,
            }
        );
        assert_eq!(config.sinks.count(), 3);
        assert_eq!(config.bucket_override.as_deref(), Some("changes"));
        assert_eq!(
            config.classes,
            Classes::Only(["Chat".to_string(), "__D1Database".to_string()].into())
        );
        assert!(config.denies_table("Chat", "drafts"));
        assert!(config.denies_table("Chat", "my.table"));
        assert!(!config.denies_table("Chat", "messages"));
        assert_eq!(config.max_tx_bytes, 100);
        assert_eq!(config.max_record_bytes, 10);
        assert_eq!(config.queue_bytes, 1000);
        assert_eq!(config.flush, Duration::from_millis(5));
        assert_eq!(config.flush_bytes, 7);
        assert_eq!(config.retention, Retention::Days(14));
        assert_eq!(config.topic, "prod-changes");
        assert_eq!(config.brokers.as_deref(), Some("k8s://streams/blob-stream"));
        assert_eq!(config.writer_id.as_deref(), Some("us-east-1c"));
        assert_eq!(config.zone.as_deref(), Some("us-east-1a"));
        assert_eq!(config.zones, ["us-east-1a", "us-east-1b", "us-east-1c"]);
        // The explicit writer zone wins over the node's zone.
        assert_eq!(config.blob_stream_writer_id().unwrap(), 2);
        assert_eq!(config.blob_stream_writers(), 3);
        assert_eq!(config.partitions, Some(64));
        assert_eq!(
            config.kafka_brokers.as_deref(),
            Some("k1:9092, k2.internal:9093")
        );
        assert_eq!(
            config.kafka_properties.as_deref(),
            Some(std::path::Path::new("/etc/celld/kafka.properties"))
        );
        assert_eq!(config.retry, Duration::from_millis(9));
        assert_eq!(config.reconcile, Duration::from_secs(6 * 3600));
    }

    #[test]
    fn default_allow_list_is_application_classes_plus_d1_and_kv() {
        let config = enabled(&[]);
        for class in ["Chat", "__D1Database", "__KvNamespace"] {
            assert!(config.exports_class(class), "{class}");
        }
        for class in ["__Queue", "__Workflow", "__Workflow.billing", ".cron"] {
            assert!(!config.exports_class(class), "{class}");
        }
    }

    #[test]
    fn explicit_allow_list_is_exact() {
        let config = enabled(&[("CELLD_EXPORT_CLASSES", "Chat")]);
        assert!(config.exports_class("Chat"));
        assert!(!config.exports_class("Room"));
        assert!(!config.exports_class("__D1Database"));
    }

    #[test]
    fn allow_list_refuses_never_exported_classes() {
        for class in ["__Queue", "__Workflow", "__Workflow.billing", ".cron"] {
            let message = error(&[("CELLD_EXPORT_CLASSES", class)]);
            assert!(message.contains("never exported"), "{class}: {message}");
        }
        assert!(error(&[("CELLD_EXPORT_CLASSES", " , ")]).contains("at least one class"));
    }

    #[test]
    fn workflow_prefix_does_not_catch_lookalikes() {
        assert!(!is_never_exported("__WorkflowRunner"));
        assert!(!is_never_exported("__QueueStats"));
    }

    #[test]
    fn malformed_values_are_refused() {
        for (name, value) in [
            ("CELLD_EXPORT_SINK", "pulsar"),
            ("CELLD_EXPORT_SINK", ""),
            ("CELLD_EXPORT_BUCKET", ""),
            ("CELLD_EXPORT_TABLES", "drafts"),
            ("CELLD_EXPORT_TABLES", "Chat."),
            ("CELLD_EXPORT_MAX_TX_BYTES", "0"),
            ("CELLD_EXPORT_QUEUE_BYTES", "-1"),
            ("CELLD_EXPORT_FLUSH_BYTES", "8MB"),
            ("CELLD_EXPORT_RETRY_MS", "0"),
            ("CELLD_EXPORT_RETENTION", "30"),
            ("CELLD_EXPORT_RETENTION", "0d"),
            ("CELLD_EXPORT_TOPIC", " "),
            ("CELLD_EXPORT_WRITER_ID", ""),
            ("CELLD_EXPORT_WRITER_ID", "us east"),
            ("CELLD_ZONE", ""),
            ("CELLD_ZONE", "a/b"),
            ("CELLD_EXPORT_ZONES", " , "),
            ("CELLD_EXPORT_ZONES", "a,b,a"),
            ("CELLD_EXPORT_PARTITIONS", "0"),
            ("CELLD_EXPORT_BROKERS", "k8s://streams"),
            ("CELLD_EXPORT_BROKERS", "broker-a"),
            ("CELLD_EXPORT_BROKERS", "broker-a:0"),
            ("CELLD_EXPORT_BROKERS", "a:9092"),
            ("CELLD_EXPORT_BROKERS", "=a:9092"),
            ("CELLD_EXPORT_BROKERS", "b0=a:9092,b0=b:9092"),
            ("CELLD_EXPORT_KAFKA_BROKERS", " "),
            ("CELLD_EXPORT_KAFKA_BROKERS", ","),
            ("CELLD_EXPORT_KAFKA_BROKERS", "k1"),
            ("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092,:9092"),
            ("CELLD_EXPORT_KAFKA_BROKERS", "k1:none"),
            ("CELLD_EXPORT_KAFKA_PROPERTIES", ""),
            ("CELLD_EXPORT_RECONCILE", "24"),
            ("CELLD_EXPORT_RECONCILE", "0h"),
            ("CELLD_EXPORT_RECONCILE", "1w"),
        ] {
            let message = error(&[(name, value)]);
            assert!(message.contains(name), "{name}={value:?}: {message}");
        }
    }

    #[test]
    fn static_brokers_are_accepted() {
        let config = enabled(&[
            ("CELLD_EXPORT_SINK", "blob-stream"),
            (
                "CELLD_EXPORT_BROKERS",
                "broker-a=a:9092,broker-b=b.internal:9092",
            ),
            ("CELLD_EXPORT_PARTITIONS", "16"),
        ]);
        assert!(!config.sinks.bucket);
        assert_eq!(
            config.brokers.as_deref(),
            Some("broker-a=a:9092,broker-b=b.internal:9092")
        );
    }

    #[test]
    fn enabled_kafka_needs_brokers_and_nothing_of_blob_streams() {
        let message = error(&[("CELLD_EXPORT", "1"), ("CELLD_EXPORT_SINK", "kafka")]);
        assert!(message.contains("CELLD_EXPORT_KAFKA_BROKERS"), "{message}");
        // Kafka partitions its topic itself: no partition count or zones.
        let config = enabled(&[
            ("CELLD_EXPORT_SINK", "kafka"),
            ("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092"),
        ]);
        assert_eq!(
            config.sinks,
            Sinks {
                bucket: false,
                blob_stream: false,
                kafka: true,
            }
        );
        assert_eq!(config.sinks.count(), 1);
    }

    #[test]
    fn enabled_blob_stream_needs_brokers() {
        let message = error(&[("CELLD_EXPORT", "1"), ("CELLD_EXPORT_SINK", "blob-stream")]);
        assert!(message.contains("CELLD_EXPORT_BROKERS"), "{message}");
    }

    #[test]
    fn enabled_blob_stream_needs_the_topic_shape() {
        let blob_stream = [
            ("CELLD_EXPORT", "1"),
            ("CELLD_EXPORT_SINK", "blob-stream"),
            ("CELLD_EXPORT_BROKERS", "b0=a:9092"),
        ];
        let message = error(&blob_stream);
        assert!(message.contains("CELLD_EXPORT_PARTITIONS"), "{message}");
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut vars = blob_stream.to_vec();
            vars.push(("CELLD_EXPORT_PARTITIONS", "16"));
            vars.extend_from_slice(extra);
            config(&vars)
        };
        let writer = |extra: &[(&'static str, &'static str)]| {
            let config = with(extra).unwrap().unwrap();
            (
                config.blob_stream_writer_id().unwrap(),
                config.blob_stream_writers(),
            )
        };
        // No zones: one writer, whatever the node's zone.
        assert_eq!(writer(&[]), (0, 1));
        assert_eq!(writer(&[("CELLD_ZONE", "us-east-1b")]), (0, 1));
        // Zones: the node's zone picks the writer by position.
        let zones = ("CELLD_EXPORT_ZONES", "us-east-1a,us-east-1b,us-east-1c");
        assert_eq!(writer(&[zones, ("CELLD_ZONE", "us-east-1b")]), (1, 3));
        assert_eq!(
            writer(&[zones, ("CELLD_EXPORT_WRITER_ID", "us-east-1c")]),
            (2, 3)
        );
        for (extra, needle) in [
            (vec![zones], "CELLD_ZONE"),
            (vec![zones, ("CELLD_ZONE", "eu-west-1a")], "does not list"),
            (
                vec![zones, ("CELLD_EXPORT_WRITER_ID", "eu-west-1a")],
                "does not list",
            ),
            (
                vec![("CELLD_EXPORT_WRITER_ID", "us-east-1a")],
                "CELLD_EXPORT_ZONES is unset",
            ),
        ] {
            let message = with(&extra).unwrap_err().to_string();
            assert!(message.contains(needle), "{extra:?}: {message}");
        }
        let message = with(&[
            ("CELLD_EXPORT_PARTITIONS", "4294967295"),
            ("CELLD_EXPORT_ZONES", "a,b"),
        ])
        .unwrap_err()
        .to_string();
        assert!(message.contains("overflows"), "{message}");
        // The bucket sink alone never reads them.
        assert!(
            config(&[("CELLD_EXPORT", "1"), ("CELLD_EXPORT_ZONES", "a,b"),])
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn a_fragment_must_fit_the_budget() {
        let message = error(&[
            ("CELLD_EXPORT", "1"),
            ("CELLD_EXPORT_MAX_RECORD_BYTES", "2048"),
            ("CELLD_EXPORT_QUEUE_BYTES", "1024"),
        ]);
        assert!(message.contains("CELLD_EXPORT_QUEUE_BYTES"), "{message}");
    }
}
