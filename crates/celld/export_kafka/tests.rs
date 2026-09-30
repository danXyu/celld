// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;

fn config(vars: &[(&str, &str)]) -> Config {
    Config::from_lookup(|name| {
        let found = [("CELLD_EXPORT", "1"), ("CELLD_EXPORT_SINK", "kafka")]
            .iter()
            .chain(vars)
            .rev()
            .find(|(n, _)| *n == name)
            .map(|(_, value)| value.to_string());
        Ok(found)
    })
    .unwrap()
    .unwrap()
}

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
fn the_producer_is_durable_and_bounded_by_the_retry_deadline() {
    let settings = Settings::from_config(&config(&[
        ("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092,k2:9092"),
        ("CELLD_EXPORT_TOPIC", "prod-changes"),
        ("CELLD_EXPORT_RETRY_MS", "9000"),
    ]))
    .unwrap();
    assert_eq!(settings.topic, "prod-changes");
    assert_eq!(settings.retry, Duration::from_millis(9000));
    assert_eq!(
        property(&settings, "bootstrap.servers"),
        Some("k1:9092,k2:9092")
    );
    assert_eq!(property(&settings, "acks"), Some("all"));
    assert_eq!(property(&settings, "enable.idempotence"), Some("true"));
    assert_eq!(property(&settings, "message.timeout.ms"), Some("9000"));
    assert_eq!(
        property(&settings, "allow.auto.create.topics"),
        Some("false")
    );
    // The default record limit plus framing.
    assert_eq!(property(&settings, "message.max.bytes"), Some("1114112"));
}

#[test]
fn the_message_limit_follows_the_record_limit() {
    let settings = |max: &str| {
        Settings::from_config(&config(&[
            ("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092"),
            ("CELLD_EXPORT_MAX_RECORD_BYTES", max),
            ("CELLD_EXPORT_QUEUE_BYTES", "1073741824"),
        ]))
    };
    let large = settings("4194304").unwrap();
    assert_eq!(property(&large, "message.max.bytes"), Some("4259840"));
    // librdkafka's floor.
    let small = settings("2048").unwrap();
    assert_eq!(property(&small, "message.max.bytes"), Some("1000000"));
    // librdkafka's ceiling.
    let message = format!("{:#}", settings("999999999").unwrap_err());
    assert!(message.contains("more than a Kafka message"), "{message}");

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kafka.properties");
    for (text, refused) in [
        ("message.max.bytes=1000000\n", true),
        ("message.max.bytes=lots\n", true),
        ("message.max.bytes=2000000\n", false),
    ] {
        std::fs::write(&path, text).unwrap();
        let result = Settings::from_config(&config(&[
            ("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092"),
            ("CELLD_EXPORT_KAFKA_PROPERTIES", path.to_str().unwrap()),
        ]));
        match result {
            Err(e) if refused => assert!(format!("{e:#}").contains("message.max.bytes")),
            Ok(settings) if !refused => {
                assert_eq!(property(&settings, "message.max.bytes"), Some("2000000"))
            }
            other => panic!("{text:?}: {other:?}"),
        }
    }
}

#[test]
fn a_properties_file_applies_over_the_sinks_own() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kafka.properties");
    std::fs::write(
        &path,
        "# Confluent Cloud\n\
         security.protocol=SASL_SSL\n\
         sasl.mechanisms = PLAIN\n\
         ! a comment too\n\
         \n\
         sasl.password=a=b==\n\
         compression.type=zstd\n\
         acks=-1\n",
    )
    .unwrap();
    let settings = Settings::from_config(&config(&[
        ("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092"),
        ("CELLD_EXPORT_KAFKA_PROPERTIES", path.to_str().unwrap()),
    ]))
    .unwrap();
    assert_eq!(property(&settings, "security.protocol"), Some("SASL_SSL"));
    assert_eq!(property(&settings, "sasl.mechanisms"), Some("PLAIN"));
    // Only the first `=` separates.
    assert_eq!(property(&settings, "sasl.password"), Some("a=b=="));
    assert_eq!(property(&settings, "compression.type"), Some("zstd"));
    assert_eq!(property(&settings, "acks"), Some("-1"));
}

#[test]
fn a_properties_file_may_not_weaken_acks_or_be_malformed() {
    for (text, expected) in [
        ("acks=1\n", "acks must stay all"),
        ("acks = 0", "acks must stay all"),
        (
            "delivery.report.only.error=true",
            "delivery.report.only.error must stay false",
        ),
        ("message.timeout.ms=0", "set from CELLD_EXPORT_RETRY_MS"),
        (
            "delivery.timeout.ms=600000",
            "set from CELLD_EXPORT_RETRY_MS",
        ),
        (
            "allow.auto.create.topics=true",
            "allow.auto.create.topics must stay false",
        ),
        ("linger.ms=5\nbatch.size\n", "line 2: expected name=value"),
        ("=5", "line 1: a property needs a name"),
    ] {
        let message = format!("{:#}", parse_properties(text).unwrap_err());
        assert!(message.contains(expected), "{text:?}: {message}");
    }
    let message = format!(
        "{:#}",
        Settings::from_config(&config(&[
            ("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092"),
            (
                "CELLD_EXPORT_KAFKA_PROPERTIES",
                "/nonexistent/kafka.properties"
            ),
        ]))
        .unwrap_err()
    );
    assert!(
        message.contains("CELLD_EXPORT_KAFKA_PROPERTIES"),
        "{message}"
    );
}

#[cfg(not(feature = "export-kafka"))]
#[tokio::test]
async fn a_build_without_the_feature_refuses_the_sink() {
    let (tx, _rx) = mpsc::unbounded_channel();
    let config = config(&[("CELLD_EXPORT_KAFKA_BROKERS", "k1:9092")]);
    let message = start(&config, tx).err().expect("refused").to_string();
    assert!(message.contains("export-kafka"), "{message}");
}

#[cfg(feature = "export-kafka")]
mod client {
    use super::*;
    use crate::export_sink::Closed;
    use crate::export_sink::Delivery;
    use crate::export_topic::tests::next_outcome;
    use crate::export_topic::tests::seqs;
    use crate::export_topic::tests::submitted;

    /// Against a broker address nothing listens on, connecting never
    /// succeeds, and every record is dropped once the retry deadline
    /// passes, with the reason.
    #[tokio::test]
    async fn the_real_producer_drops_what_it_cannot_deliver() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        drop(listener);
        let config = config(&[
            ("CELLD_EXPORT_KAFKA_BROKERS", &address.to_string()),
            ("CELLD_EXPORT_RETRY_MS", "500"),
        ]);
        let (tx, mut outcomes) = mpsc::unbounded_channel();
        let sink = start(&config, tx).unwrap();
        assert_eq!(sink.name(), "kafka");
        sink.submit(submitted(0..2)).unwrap();
        let outcome = next_outcome(&mut outcomes).await;
        assert_eq!(outcome.sink, "kafka");
        assert_eq!(seqs(&outcome), vec![0, 1]);
        for (_, delivery) in &outcome.results {
            let Delivery::Dropped { reason } = delivery else {
                panic!("dropped: {delivery:?}");
            };
            assert!(reason.contains("kafka"), "{reason}");
        }
        sink.close().await;
        assert_eq!(sink.submit(submitted(2..3)), Err(Closed));
    }

    /// Against a real broker, when `CELLD_TEST_KAFKA_BROKERS` names one (CI
    /// runs one): every record lands as one message keyed by its stream,
    /// with its commit time, and is acknowledged with where it landed.
    #[tokio::test]
    async fn records_land_on_a_real_broker() {
        use rdkafka::consumer::BaseConsumer;
        use rdkafka::consumer::Consumer as _;
        use rdkafka::Message as _;

        let Ok(brokers) = std::env::var("CELLD_TEST_KAFKA_BROKERS") else {
            eprintln!("skipped: set CELLD_TEST_KAFKA_BROKERS to run against a broker");
            return;
        };
        let topic = format!("celld-export-test-{}", rand::random::<u64>());
        create_topic(&brokers, &topic).await;
        let config = config(&[
            ("CELLD_EXPORT_KAFKA_BROKERS", &brokers),
            ("CELLD_EXPORT_TOPIC", &topic),
            ("CELLD_EXPORT_RETRY_MS", "30000"),
        ]);
        let (tx, mut outcomes) = mpsc::unbounded_channel();
        let sink = start(&config, tx).unwrap();
        let records = submitted(0..5);
        let expected: Vec<crate::export_topic::Message> = records
            .iter()
            .map(|r| crate::export_topic::message(&r.record).unwrap())
            .collect();
        sink.submit(records).unwrap();
        let outcome = next_outcome(&mut outcomes).await;
        assert_eq!(seqs(&outcome), vec![0, 1, 2, 3, 4]);
        for (_, delivery) in &outcome.results {
            let Delivery::Acknowledged { object } = delivery else {
                panic!("acknowledged: {delivery:?}");
            };
            assert!(object.starts_with(&format!("{topic}/")), "{object}");
        }
        sink.close().await;

        let consumer: BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .set("group.id", format!("{topic}-check"))
            .set("auto.offset.reset", "earliest")
            .create()
            .unwrap();
        consumer.subscribe(&[&topic]).unwrap();
        let read = tokio::task::spawn_blocking(move || {
            let mut read = Vec::new();
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            while read.len() < 5 && std::time::Instant::now() < deadline {
                if let Some(message) = consumer.poll(Duration::from_millis(200)) {
                    let message = message.unwrap();
                    read.push(crate::export_topic::Message {
                        key: message.key().unwrap().to_vec(),
                        payload: message.payload().unwrap().to_vec().into(),
                        event_ts_ms: message.timestamp().to_millis().unwrap(),
                    });
                }
            }
            read
        })
        .await
        .unwrap();
        // One stream, so one partition, in submission order.
        assert_eq!(read, expected);
    }

    /// Against a real broker that creates topics on demand (CI's does): a
    /// missing topic fails the connection, with its name, and stays missing.
    #[tokio::test]
    async fn a_missing_topic_is_an_error_not_created() {
        use rdkafka::consumer::BaseConsumer;
        use rdkafka::consumer::Consumer as _;

        let Ok(brokers) = std::env::var("CELLD_TEST_KAFKA_BROKERS") else {
            eprintln!("skipped: set CELLD_TEST_KAFKA_BROKERS to run against a broker");
            return;
        };
        let topic = format!("celld-export-missing-{}", rand::random::<u64>());
        let settings = Settings::from_config(&config(&[
            ("CELLD_EXPORT_KAFKA_BROKERS", &brokers),
            ("CELLD_EXPORT_TOPIC", &topic),
        ]))
        .unwrap();
        let message = match crate::export_kafka::client::connect(&settings).await {
            Ok(_) => panic!("connected to a missing topic"),
            Err(e) => format!("{e:#}"),
        };
        assert!(message.contains(&topic), "{message}");

        // Consumers never ask for topics to be created.
        let consumer: BaseConsumer = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", &brokers)
            .create()
            .unwrap();
        let exists = tokio::task::spawn_blocking(move || {
            let metadata = consumer
                .fetch_metadata(None, Duration::from_secs(10))
                .unwrap();
            metadata.topics().iter().any(|t| t.name() == topic)
        })
        .await
        .unwrap();
        assert!(!exists, "the sink created the topic");
    }

    /// Against a real broker: a record at `CELLD_EXPORT_MAX_RECORD_BYTES`,
    /// which does not compress, is delivered to a topic whose own limit
    /// allows it.
    #[tokio::test]
    async fn a_record_at_the_size_limit_is_delivered() {
        use crate::export_topic::Message;

        let Ok(brokers) = std::env::var("CELLD_TEST_KAFKA_BROKERS") else {
            eprintln!("skipped: set CELLD_TEST_KAFKA_BROKERS to run against a broker");
            return;
        };
        let topic = format!("celld-export-large-{}", rand::random::<u64>());
        create_topic_with(&brokers, &topic, &[("max.message.bytes", "2097152")]).await;
        let config = config(&[
            ("CELLD_EXPORT_KAFKA_BROKERS", &brokers),
            ("CELLD_EXPORT_TOPIC", &topic),
            ("CELLD_EXPORT_RETRY_MS", "30000"),
        ]);
        let settings = Settings::from_config(&config).unwrap();
        let producer = crate::export_kafka::client::connect(&settings)
            .await
            .unwrap();
        let payload: Vec<u8> = (0..config.max_record_bytes)
            .map(|_| rand::random::<u8>())
            .collect();
        let results = producer
            .produce(vec![Message {
                key: b"a stream".to_vec(),
                payload: payload.into(),
                event_ts_ms: 1_790_685_296_000,
            }])
            .await;
        assert_eq!(results.len(), 1);
        let landed = results[0].as_ref().expect("delivered");
        assert!(landed.starts_with(&format!("{topic}/")), "{landed}");
    }

    /// Against a real broker: a delivery report that never comes (as with
    /// `delivery.report.only.error`, which the properties file refuses)
    /// drops the record once the retry deadline passes rather than holding
    /// every later one.
    #[tokio::test]
    async fn a_missing_delivery_report_drops_the_record_by_the_deadline() {
        use crate::export_topic::Message;

        let Ok(brokers) = std::env::var("CELLD_TEST_KAFKA_BROKERS") else {
            eprintln!("skipped: set CELLD_TEST_KAFKA_BROKERS to run against a broker");
            return;
        };
        let topic = format!("celld-export-silent-{}", rand::random::<u64>());
        create_topic(&brokers, &topic).await;
        let settings = Settings::new(
            topic.clone(),
            brokers.clone(),
            Duration::from_secs(2),
            crate::export::DEFAULT_MAX_RECORD_BYTES,
            vec![("delivery.report.only.error".into(), "true".into())],
        )
        .unwrap();
        let producer = crate::export_kafka::client::connect(&settings)
            .await
            .unwrap();
        let results = tokio::time::timeout(
            Duration::from_secs(60),
            producer.produce(vec![Message {
                key: b"a stream".to_vec(),
                payload: b"{}".to_vec().into(),
                event_ts_ms: 1_790_685_296_000,
            }]),
        )
        .await
        .expect("the produce finished");
        let reason = results[0].as_ref().expect_err("dropped");
        assert!(reason.contains("delivery"), "{reason}");
    }

    async fn create_topic(brokers: &str, topic: &str) {
        create_topic_with(brokers, topic, &[]).await
    }

    async fn create_topic_with(brokers: &str, topic: &str, configs: &[(&str, &str)]) {
        use rdkafka::admin::AdminClient;
        use rdkafka::admin::AdminOptions;
        use rdkafka::admin::NewTopic;
        use rdkafka::admin::TopicReplication;
        use rdkafka::client::DefaultClientContext;

        let admin: AdminClient<DefaultClientContext> = rdkafka::ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .create()
            .unwrap();
        let mut new = NewTopic::new(topic, 3, TopicReplication::Fixed(1));
        for (name, value) in configs {
            new = new.set(name, value);
        }
        let created = admin
            .create_topics(&[new], &AdminOptions::new())
            .await
            .unwrap();
        for result in created {
            result.unwrap();
        }
    }
}
