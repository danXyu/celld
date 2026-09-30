use std::sync::{Arc, Mutex};
use std::time::Instant;

use celld_export_format::{Body, Envelope, Origin, Position, Record, StreamId, WatermarkBody};
use tokio_util::sync::CancellationToken;

use super::*;
use crate::consume::Land;
use crate::loader::{Loader, LoaderConfig, Rows, Warehouse, WarehouseError};
use crate::source::Settings as LoopSettings;
use crate::{Deployment, LandingRow};

fn property<'a>(settings: &'a Settings, name: &str) -> Option<&'a str> {
    // The last setting of a name wins, as in librdkafka.
    settings
        .properties
        .iter()
        .rev()
        .find(|(n, _)| n == name)
        .map(|(_, value)| value.as_str())
}

#[test]
fn the_consumer_commits_only_what_the_loop_commits() {
    let settings = Settings::new("k1:9092,k2:9092", None, None, Some("loader-0"), None).unwrap();
    assert_eq!(settings.topic, "celld-changes");
    assert_eq!(
        property(&settings, "bootstrap.servers"),
        Some("k1:9092,k2:9092")
    );
    assert_eq!(property(&settings, "group.id"), Some("snowflake"));
    assert_eq!(property(&settings, "client.id"), Some("loader-0"));
    assert_eq!(property(&settings, "enable.auto.commit"), Some("false"));
    assert_eq!(property(&settings, "auto.offset.reset"), Some("earliest"));
    let settings = Settings::new("k1:9092", Some("prod"), Some("wh"), None, None).unwrap();
    assert_eq!(settings.topic, "prod");
    assert_eq!(property(&settings, "group.id"), Some("wh"));
}

#[test]
fn a_properties_file_applies_over_the_loaders_own() {
    let path = std::env::temp_dir().join(format!(
        "celld-export-loader-{}-kafka.properties",
        std::process::id()
    ));
    std::fs::write(
        &path,
        "# MSK over TLS\nsecurity.protocol = SSL\n\n! note\nauto.offset.reset=latest\n",
    )
    .unwrap();
    let settings = Settings::new("k1:9092", None, None, None, Some(&path)).unwrap();
    std::fs::remove_file(&path).unwrap();
    assert_eq!(property(&settings, "security.protocol"), Some("SSL"));
    assert_eq!(property(&settings, "auto.offset.reset"), Some("latest"));
    let missing = Settings::new("k1:9092", None, None, None, Some(&path)).unwrap_err();
    assert!(format!("{missing:#}").contains("EXPORT_KAFKA_PROPERTIES"));
}

#[test]
fn properties_may_not_commit_automatically_or_be_malformed() {
    for (text, expected) in [
        (
            "enable.auto.commit=true",
            "enable.auto.commit must stay false",
        ),
        ("a=1\nb\n", "line 2: expected name=value"),
        (" = 1", "line 1: a property needs a name"),
    ] {
        let message = format!("{:#}", parse_properties(text).unwrap_err());
        assert!(message.contains(expected), "{text:?}: {message}");
    }
    assert_eq!(
        parse_properties("sasl.password=a=b\nenable.auto.commit=false").unwrap(),
        [
            ("sasl.password".to_string(), "a=b".to_string()),
            ("enable.auto.commit".to_string(), "false".to_string())
        ]
    );
}

// ------------------------------------------------ against a real broker

/// The Dynamic Table sync's warehouse, which has no schemas.
struct NoSchemas;

impl Warehouse for NoSchemas {
    fn execute_bound(
        &mut self,
        _sql: &str,
        _binds: &[serde_json::Value],
    ) -> Result<Rows, WarehouseError> {
        Ok(Rows::default())
    }
}

/// Lands into a shared list.
#[derive(Clone, Default)]
struct Landed(Arc<Mutex<Vec<LandingRow>>>);

impl Land for Landed {
    fn land(&mut self, rows: &[LandingRow]) -> Result<(), WarehouseError> {
        self.0.lock().unwrap().extend_from_slice(rows);
        Ok(())
    }
}

impl Landed {
    fn sources(&self) -> Vec<String> {
        let mut sources: Vec<String> = self
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|r| r.source.clone())
            .collect();
        sources.sort();
        sources
    }
}

fn record(txid: u64) -> Record {
    Record {
        envelope: Envelope {
            stream: StreamId {
                script: "app".into(),
                class: "Room".into(),
                cell: "r1".into(),
                facet: None,
                incarnation: 1,
            },
            cell_name: None,
            position: Position::new(1, txid, 1),
            committed_at: 1_790_000_000_000,
            node: "node-a".into(),
            origin: Origin::Live,
            fragment: 1,
            fragments: 1,
        },
        body: Body::Watermark(WatermarkBody {
            from: None,
            through: Position::new(1, txid, 1),
            commits: 1,
            records: 1,
        }),
    }
}

/// The broker CI runs, or none: these tests pass without one.
fn brokers() -> Option<String> {
    let brokers = std::env::var("CELLD_TEST_KAFKA_BROKERS").ok();
    if brokers.is_none() {
        eprintln!("skipped: set CELLD_TEST_KAFKA_BROKERS to run against a broker");
    }
    brokers
}

/// A fresh topic with `partitions` partitions, and `messages` produced to
/// it as (partition, payload). Each test's group is named after its topic:
/// tests share a broker, and one group's members rebalance together.
async fn topic(brokers: &str, partitions: i32, messages: &[(i32, Vec<u8>)]) -> String {
    use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
    use rdkafka::client::DefaultClientContext;
    use rdkafka::producer::{FutureProducer, FutureRecord};

    let topic = format!(
        "celld-loader-test-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    for result in admin
        .create_topics(
            &[NewTopic::new(
                &topic,
                partitions,
                TopicReplication::Fixed(1),
            )],
            &AdminOptions::new(),
        )
        .await
        .unwrap()
    {
        result.unwrap();
    }
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .create()
        .unwrap();
    for (partition, payload) in messages {
        producer
            .send(
                FutureRecord::<(), _>::to(&topic)
                    .partition(*partition)
                    .payload(payload),
                Duration::from_secs(10),
            )
            .await
            .unwrap();
    }
    topic
}

struct Member {
    landed: Landed,
    events: Arc<Mutex<Vec<String>>>,
    stop: CancellationToken,
    done: tokio::task::JoinHandle<anyhow::Result<()>>,
}

fn member(brokers: &str, topic: &str, group: &str, linger: Duration) -> Member {
    let source =
        KafkaSource::new(Settings::new(brokers, Some(topic), Some(group), None, None).unwrap());
    let landed = Landed::default();
    let mut lander = landed.clone();
    let events = Arc::new(Mutex::new(Vec::new()));
    let stop = CancellationToken::new();
    let (e, s) = (events.clone(), stop.clone());
    let done = tokio::spawn(async move {
        let mut loader = Loader::new(
            NoSchemas,
            LoaderConfig {
                deployment: Deployment {
                    warehouse: "WH".into(),
                },
                target_lag: "1 minute".into(),
                dynamic_table_prefix: "CF".into(),
            },
        );
        let settings = LoopSettings {
            linger,
            ..LoopSettings::default()
        };
        crate::source::run(source, &mut loader, &mut lander, &settings, s, |event| {
            e.lock().unwrap().push(format!("{event:?}"));
        })
        .await
    });
    Member {
        landed,
        events,
        stop,
        done,
    }
}

impl Member {
    async fn stop(self) -> (Landed, Vec<String>) {
        self.stop.cancel();
        self.done.await.unwrap().unwrap();
        let events = self.events.lock().unwrap().clone();
        (self.landed, events)
    }
}

async fn until(what: &str, mut ready: impl FnMut() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !ready() {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Committed offsets of `group` on `topic`, as Kafka's next offsets.
fn committed(brokers: &str, topic: &str, group: &str, partitions: i32) -> Vec<i64> {
    let consumer: BaseConsumer = ClientConfig::new()
        .set("bootstrap.servers", brokers)
        .set("group.id", group)
        .create()
        .unwrap();
    let mut list = TopicPartitionList::new();
    for partition in 0..partitions {
        list.add_partition(topic, partition);
    }
    let committed = consumer
        .committed_offsets(list, Duration::from_secs(10))
        .unwrap();
    (0..partitions)
        .map(
            |p| match committed.find_partition(topic, p).unwrap().offset() {
                Offset::Offset(o) => o,
                _ => -1,
            },
        )
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn records_land_and_their_offsets_commit() {
    let Some(brokers) = brokers() else { return };
    let messages: Vec<(i32, Vec<u8>)> = (1..=5)
        .map(|txid| ((txid % 2) as i32, record(txid).to_json()))
        .collect();
    let topic = topic(&brokers, 2, &messages).await;
    let m = member(&brokers, &topic, &topic, Duration::from_millis(200));
    let landed = m.landed.clone();
    until("five landed records", || {
        landed.0.lock().unwrap().len() == 5
    })
    .await;
    let (landed, events) = m.stop().await;
    assert_eq!(
        landed.sources(),
        [
            "kafka/0/0",
            "kafka/0/1",
            "kafka/1/0",
            "kafka/1/1",
            "kafka/1/2"
        ],
        "{events:?}"
    );
    let mut txids: Vec<u64> = landed.0.lock().unwrap().iter().map(|r| r.txid).collect();
    txids.sort();
    assert_eq!(txids, [1, 2, 3, 4, 5]);
    // Kafka's convention: the next offset to read.
    assert_eq!(committed(&brokers, &topic, &topic, 2), [2, 3]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_that_is_not_a_record_is_named_for_export_skip() {
    let Some(brokers) = brokers() else { return };
    let messages = vec![(0, record(1).to_json()), (0, b"{\"kind\":".to_vec())];
    let topic = topic(&brokers, 1, &messages).await;
    let m = member(&brokers, &topic, &topic, Duration::from_secs(3600));
    let err = tokio::time::timeout(Duration::from_secs(60), m.done)
        .await
        .expect("the loop stops")
        .unwrap()
        .unwrap_err();
    let err = format!("{err:#}");
    assert!(err.contains("kafka/0/1 is not a record"), "{err}");
    assert_eq!(m.landed.sources(), ["kafka/0/0"]);
    assert_eq!(committed(&brokers, &topic, &topic, 1), [1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_revoked_member_lands_what_it_holds_before_letting_go() {
    let Some(brokers) = brokers() else { return };
    let messages: Vec<(i32, Vec<u8>)> = (1..=6)
        .map(|txid| ((txid % 2) as i32, record(txid).to_json()))
        .collect();
    let topic = topic(&brokers, 2, &messages).await;
    // A lingers forever, so only a revocation (or a stop) lands its batch.
    let a = member(&brokers, &topic, &topic, Duration::from_secs(3600));
    // Time for A to join and read the topic; nothing A does is visible
    // until it lands.
    tokio::time::sleep(Duration::from_secs(5)).await;
    let a_events = a.events.clone();
    // B joining makes the group take A's partitions back.
    let b = member(&brokers, &topic, &topic, Duration::from_millis(100));
    until("A's revocation", || {
        a_events
            .lock()
            .unwrap()
            .iter()
            .any(|e| e.starts_with("Revoked"))
    })
    .await;
    assert_eq!(a.landed.0.lock().unwrap().len(), 6, "landed on revocation");
    // Whatever B is given, it starts past what A landed: nothing twice.
    tokio::time::sleep(Duration::from_secs(3)).await;
    let (b_landed, _) = b.stop().await;
    let (a_landed, _) = a.stop().await;
    assert!(b_landed.sources().is_empty(), "{:?}", b_landed.sources());
    assert_eq!(a_landed.sources().len(), 6);
    assert_eq!(committed(&brokers, &topic, &topic, 2), [3, 3]);
}
