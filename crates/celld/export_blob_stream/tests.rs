// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
#[cfg(feature = "export-blob-stream")]
use crate::export_sink::Closed;
#[cfg(feature = "export-blob-stream")]
use crate::export_topic::tests::next_outcome;
#[cfg(feature = "export-blob-stream")]
use crate::export_topic::tests::seqs;
#[cfg(feature = "export-blob-stream")]
use crate::export_topic::tests::submitted;

#[cfg(not(feature = "export-blob-stream"))]
#[tokio::test]
async fn a_build_without_the_feature_refuses_the_sink() {
    let config = crate::export::Config::from_lookup(|name| {
        Ok(match name {
            "CELLD_EXPORT" => Some("1".into()),
            "CELLD_EXPORT_SINK" => Some("blob-stream".into()),
            "CELLD_EXPORT_BROKERS" => Some("b0=a:9092".into()),
            "CELLD_EXPORT_PARTITIONS" => Some("16".into()),
            _ => None,
        })
    })
    .unwrap()
    .unwrap();
    let (tx, _rx) = mpsc::unbounded_channel();
    let message = start(&config, tx).err().expect("refused").to_string();
    assert!(message.contains("export-blob-stream"), "{message}");
}

#[cfg(feature = "export-blob-stream")]
mod client_config {
    use super::super::client::Settings;
    use std::time::Duration;

    fn settings(brokers: &str) -> Settings {
        Settings {
            topic: "celld-changes".into(),
            brokers: brokers.into(),
            writer_id: 1,
            partitions: 64,
            writers: 3,
            retry: Duration::from_millis(9_000),
        }
    }

    #[test]
    fn static_brokers_become_static_discovery() {
        let runtime = settings("local-broker-1=a:9092, local-broker-2=b.internal:9092")
            .runtime()
            .unwrap();
        let producer = runtime.producer.as_ref().unwrap();
        assert_eq!(producer.writer_id, Some(1));
        assert_eq!(producer.retry_deadline.as_ref().unwrap().seconds, 9);
        let nodes = &runtime.discovery.as_ref().unwrap().static_().nodes;
        let addresses: Vec<&str> = nodes.iter().map(|n| &*n.address).collect();
        assert_eq!(addresses, ["a:9092", "b.internal:9092"]);
        // The brokers' own IDs, not their addresses: routing hashes them.
        let ids: Vec<&str> = nodes.iter().map(|n| &*n.node_id).collect();
        assert_eq!(ids, ["local-broker-1", "local-broker-2"]);
        let topic = &runtime.topics[0];
        assert_eq!(&*topic.name, "celld-changes");
        assert_eq!(topic.partition_count, 64);
        assert_eq!(topic.num_writers, 3);
    }

    #[test]
    fn a_k8s_service_becomes_service_discovery() {
        let runtime = settings("k8s://streams/blob-stream").runtime().unwrap();
        let k8s = runtime.discovery.as_ref().unwrap().k8s_service();
        assert_eq!(&*k8s.namespace, "streams");
        assert_eq!(&*k8s.service_name, "blob-stream");
    }
}

/// The real client against a broker address nothing listens on: it
/// connects (static discovery needs no broker), then every record is
/// dropped once the retry deadline passes, with the producer's reason.
#[cfg(feature = "export-blob-stream")]
#[tokio::test]
async fn the_real_producer_drops_what_it_cannot_deliver() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    drop(listener);
    let config = crate::export::Config::from_lookup(|name| {
        Ok(match name {
            "CELLD_EXPORT" => Some("1".into()),
            "CELLD_EXPORT_SINK" => Some("blob-stream".into()),
            "CELLD_EXPORT_BROKERS" => Some(format!("dead={address}")),
            "CELLD_EXPORT_PARTITIONS" => Some("4".into()),
            "CELLD_EXPORT_RETRY_MS" => Some("500".into()),
            _ => None,
        })
    })
    .unwrap()
    .unwrap();
    let (tx, mut outcomes) = mpsc::unbounded_channel();
    let sink = start(&config, tx).unwrap();
    assert_eq!(sink.name(), "blob-stream");
    sink.submit(submitted(0..2)).unwrap();
    let outcome = next_outcome(&mut outcomes).await;
    assert_eq!(seqs(&outcome), vec![0, 1]);
    for (_, delivery) in &outcome.results {
        assert!(!delivery.is_acknowledged(), "{delivery:?}");
    }
    sink.close().await;
    assert_eq!(sink.submit(submitted(2..3)), Err(Closed));
}
