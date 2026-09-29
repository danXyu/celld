// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::export_sink::Closed;
use crate::export_sink::Delivery;
use crate::export_sink::SinkRecord;
use celld_export_format::Body;
use celld_export_format::Envelope;
use celld_export_format::GapBody;
use celld_export_format::Origin;
use celld_export_format::Position;
use celld_export_format::Record;
use celld_export_format::StreamId;
use std::time::Duration;
use tokio::sync::oneshot;

type Results = Vec<Result<Arc<str>, Arc<str>>>;

/// One produce call the test answers.
struct Call {
    messages: Vec<Message>,
    reply: oneshot::Sender<Results>,
}

/// A producer whose every call waits for the test to answer it.
struct Fake {
    calls: mpsc::UnboundedSender<Call>,
}

impl Produce for Fake {
    fn produce(&self, messages: Vec<Message>) -> BoxFuture<'static, Results> {
        let (reply, answer) = oneshot::channel();
        let _ = self.calls.send(Call { messages, reply });
        Box::pin(async move { answer.await.unwrap_or_default() })
    }
}

/// A sink whose connects the test settles one at a time.
struct Harness {
    sink: BlobStreamSink,
    outcomes: mpsc::UnboundedReceiver<Outcome>,
    connects: mpsc::UnboundedSender<anyhow::Result<Arc<dyn Produce>>>,
}

fn harness(retry: Duration) -> Harness {
    let (connects, answers) = mpsc::unbounded_channel::<anyhow::Result<Arc<dyn Produce>>>();
    let answers = Arc::new(tokio::sync::Mutex::new(answers));
    let connect: Connect = Box::new(move || {
        let answers = answers.clone();
        Box::pin(async move {
            match answers.lock().await.recv().await {
                Some(answer) => answer,
                None => std::future::pending().await,
            }
        })
    });
    let (tx, outcomes) = mpsc::unbounded_channel();
    let sink = BlobStreamSink::start(
        connect,
        BlobStreamSinkConfig {
            retry,
            reconnect: Duration::from_millis(1),
            reconnect_max: Duration::from_millis(1),
        },
        tx,
    );
    Harness {
        sink,
        outcomes,
        connects,
    }
}

fn fake() -> (Arc<dyn Produce>, mpsc::UnboundedReceiver<Call>) {
    let (calls, rx) = mpsc::unbounded_channel();
    (Arc::new(Fake { calls }), rx)
}

fn stream(cell: &str) -> StreamId {
    StreamId {
        script: "app".into(),
        class: "Counter".into(),
        cell: cell.into(),
        facet: None,
        incarnation: 7,
    }
}

fn record(cell: &str, txid: u64) -> Record {
    Record {
        envelope: Envelope {
            stream: stream(cell),
            cell_name: None,
            position: Position::new(1, txid, txid),
            committed_at: 1_790_685_296_000 + txid as i64,
            node: "node-1".into(),
            origin: Origin::Live,
            fragment: 1,
            fragments: 1,
        },
        body: Body::Gap(GapBody {
            from: Position::new(1, 0, 0),
            to: Position::new(1, txid, txid),
            reason: "test".into(),
        }),
    }
}

fn submitted(seqs: std::ops::Range<u64>) -> Vec<SinkRecord> {
    seqs.map(|seq| SinkRecord {
        seq,
        record: record("c", seq),
    })
    .collect()
}

fn acked(object: &str) -> Result<Arc<str>, Arc<str>> {
    Ok(object.into())
}

fn seqs(outcome: &Outcome) -> Vec<u64> {
    outcome.results.iter().map(|(seq, _)| *seq).collect()
}

async fn next_outcome(outcomes: &mut mpsc::UnboundedReceiver<Outcome>) -> Outcome {
    tokio::time::timeout(Duration::from_secs(10), outcomes.recv())
        .await
        .expect("an outcome in time")
        .expect("the sink is running")
}

async fn next_call(calls: &mut mpsc::UnboundedReceiver<Call>) -> Call {
    tokio::time::timeout(Duration::from_secs(10), calls.recv())
        .await
        .expect("a produce call in time")
        .expect("the producer is alive")
}

#[test]
fn a_message_is_the_record_json_keyed_by_stream() {
    let one = message(&record("a", 1)).unwrap();
    let decoded: Record = serde_json::from_slice(&one.payload).unwrap();
    assert_eq!(decoded, record("a", 1));
    assert_eq!(one.event_ts_ms, record("a", 1).envelope.committed_at);
    // Every record of a stream shares a key; other streams do not.
    assert_eq!(message(&record("a", 2)).unwrap().key, one.key);
    assert_ne!(message(&record("b", 1)).unwrap().key, one.key);
    let mut facet = record("a", 1);
    facet.envelope.stream.facet = Some("f".into());
    assert_ne!(message(&facet).unwrap().key, one.key);
    let mut reborn = record("a", 1);
    reborn.envelope.stream.incarnation = 8;
    assert_ne!(message(&reborn).unwrap().key, one.key);
}

#[tokio::test]
async fn results_follow_submission_order_when_calls_finish_out_of_order() {
    let mut h = harness(Duration::from_secs(60));
    let (producer, mut calls) = fake();
    h.connects.send(Ok(producer)).unwrap();
    h.sink.submit(submitted(0..2)).unwrap();
    h.sink.submit(submitted(2..3)).unwrap();
    let first = next_call(&mut calls).await;
    let second = next_call(&mut calls).await;
    assert_eq!(first.messages.len(), 2);
    assert_eq!(second.messages.len(), 1);
    // Both calls are in flight at once; the second finishes first.
    second.reply.send(vec![acked("t/2")]).unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(h.outcomes.try_recv().is_err(), "held behind the first call");
    first
        .reply
        .send(vec![acked("t/0"), Err("rejected".into())])
        .unwrap();
    let outcome = next_outcome(&mut h.outcomes).await;
    assert_eq!(outcome.sink, "blob-stream");
    assert_eq!(
        outcome.results,
        vec![
            (
                0,
                Delivery::Acknowledged {
                    object: "t/0".into()
                }
            ),
            (
                1,
                Delivery::Dropped {
                    reason: "rejected".into()
                }
            ),
        ]
    );
    let outcome = next_outcome(&mut h.outcomes).await;
    assert_eq!(
        outcome.results,
        vec![(
            2,
            Delivery::Acknowledged {
                object: "t/2".into()
            }
        )]
    );
    assert_eq!(h.sink.buffered_bytes(), 0);
}

#[tokio::test]
async fn records_wait_for_the_first_connect_and_count_as_buffered() {
    let mut h = harness(Duration::from_secs(60));
    h.sink.submit(submitted(0..3)).unwrap();
    let expected: u64 = (0..3)
        .map(|seq| {
            let m = message(&record("c", seq)).unwrap();
            (m.key.len() + m.payload.len()) as u64
        })
        .sum();
    assert_eq!(h.sink.buffered_bytes(), expected);
    // A failed connect is retried; nothing is lost meanwhile.
    h.connects.send(Err(anyhow::anyhow!("no brokers"))).unwrap();
    let (producer, mut calls) = fake();
    h.connects.send(Ok(producer)).unwrap();
    let call = next_call(&mut calls).await;
    assert_eq!(call.messages.len(), 3);
    assert_eq!(h.sink.buffered_bytes(), expected, "held until the results");
    call.reply
        .send(vec![acked("t/0"), acked("t/0"), acked("t/0")])
        .unwrap();
    let outcome = next_outcome(&mut h.outcomes).await;
    assert_eq!(seqs(&outcome), vec![0, 1, 2]);
    assert!(outcome.results.iter().all(|(_, d)| d.is_acknowledged()));
    assert_eq!(h.sink.buffered_bytes(), 0);
}

#[tokio::test]
async fn records_that_wait_out_the_deadline_without_a_producer_are_dropped() {
    let mut h = harness(Duration::from_millis(100));
    h.connects
        .send(Err(anyhow::anyhow!("brokers unreachable")))
        .unwrap();
    h.sink.submit(submitted(0..2)).unwrap();
    let outcome = next_outcome(&mut h.outcomes).await;
    assert_eq!(seqs(&outcome), vec![0, 1]);
    for (_, delivery) in &outcome.results {
        let Delivery::Dropped { reason } = delivery else {
            panic!("dropped: {delivery:?}");
        };
        assert!(reason.contains("brokers unreachable"), "{reason}");
    }
    assert_eq!(h.sink.buffered_bytes(), 0);
    // Once connected, later records go through.
    let (producer, mut calls) = fake();
    h.connects.send(Ok(producer)).unwrap();
    // Wait for the connect to land before submitting, so the record does
    // not race the deadline.
    tokio::time::sleep(Duration::from_millis(50)).await;
    h.sink.submit(submitted(2..3)).unwrap();
    next_call(&mut calls)
        .await
        .reply
        .send(vec![acked("t/1")])
        .unwrap();
    let outcome = next_outcome(&mut h.outcomes).await;
    assert_eq!(
        outcome.results,
        vec![(
            2,
            Delivery::Acknowledged {
                object: "t/1".into()
            }
        )]
    );
}

#[tokio::test]
async fn a_short_answer_drops_the_records_it_left_out() {
    let mut h = harness(Duration::from_secs(60));
    let (producer, mut calls) = fake();
    h.connects.send(Ok(producer)).unwrap();
    h.sink.submit(submitted(0..2)).unwrap();
    next_call(&mut calls)
        .await
        .reply
        .send(vec![acked("t/0")])
        .unwrap();
    let outcome = next_outcome(&mut h.outcomes).await;
    assert!(outcome.results[0].1.is_acknowledged());
    assert!(!outcome.results[1].1.is_acknowledged());
}

#[tokio::test]
async fn close_waits_for_calls_in_flight_and_refuses_later_submits() {
    let mut h = harness(Duration::from_secs(60));
    let (producer, mut calls) = fake();
    h.connects.send(Ok(producer)).unwrap();
    h.sink.submit(submitted(0..1)).unwrap();
    let call = next_call(&mut calls).await;
    let mut closing = h.sink.close();
    assert_eq!(h.sink.submit(submitted(1..2)), Err(Closed));
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut closing)
            .await
            .is_err(),
        "close waits for the call in flight"
    );
    call.reply.send(vec![acked("t/0")]).unwrap();
    closing.await;
    let outcome = h.outcomes.try_recv().expect("sent before close resolved");
    assert_eq!(seqs(&outcome), vec![0]);
    // A second close only waits.
    h.sink.close().await;
}

#[tokio::test]
async fn close_before_connecting_drops_what_waits() {
    let mut h = harness(Duration::from_secs(60));
    h.sink.submit(submitted(0..2)).unwrap();
    h.sink.close().await;
    let outcome = h.outcomes.try_recv().expect("sent before close resolved");
    assert_eq!(seqs(&outcome), vec![0, 1]);
    assert!(outcome.results.iter().all(|(_, d)| !d.is_acknowledged()));
    assert_eq!(h.sink.buffered_bytes(), 0);
}

#[cfg(not(feature = "export-blob-stream"))]
#[tokio::test]
async fn a_build_without_the_feature_refuses_the_sink() {
    let config = crate::export::Config::from_lookup(|name| {
        Ok(match name {
            "CELLD_EXPORT" => Some("1".into()),
            "CELLD_EXPORT_SINK" => Some("blob-stream".into()),
            "CELLD_EXPORT_BROKERS" => Some("a:9092".into()),
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
        let runtime = settings("a:9092, b.internal:9092").runtime().unwrap();
        let producer = runtime.producer.as_ref().unwrap();
        assert_eq!(producer.writer_id, Some(1));
        assert_eq!(producer.retry_deadline.as_ref().unwrap().seconds, 9);
        let nodes = &runtime.discovery.as_ref().unwrap().static_().nodes;
        let addresses: Vec<&str> = nodes.iter().map(|n| &*n.address).collect();
        assert_eq!(addresses, ["a:9092", "b.internal:9092"]);
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
            "CELLD_EXPORT_BROKERS" => Some(address.to_string()),
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
