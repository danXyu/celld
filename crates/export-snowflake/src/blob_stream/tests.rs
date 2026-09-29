use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use blob_stream_consumer::iterator::{ConsumerRecord, ConsumerSeekTarget, RevokedPartitions};
use blob_stream_consumer::{ConsumerBootstrapConfig, HeartbeatReport};
use blob_stream_types::{CommittedSourceCheckpoint, Record as Message};
use celld_export_format::{Body, Envelope, Origin, Position, Record, StreamId, WatermarkBody};
use tokio::sync::{mpsc, oneshot};

use super::*;
use crate::loader::{LoaderConfig, Rows, WarehouseError};
use crate::{Deployment, LandingRow};

// ---------------------------------------------------------------- fakes

/// What the fake consumer was told, in order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Call {
    Start,
    Store(u32, u64),
    Commit,
    Shutdown,
}

enum Next {
    Record(u32, u64, Vec<u8>),
    Revoke(Vec<u32>, oneshot::Sender<()>),
}

struct FakeIterator {
    next: mpsc::UnboundedReceiver<Next>,
    calls: Arc<Mutex<Vec<Call>>>,
}

struct FakeRevoked(Vec<u32>, oneshot::Sender<()>);

#[async_trait]
impl RevokedPartitions for FakeRevoked {
    fn partitions(&self) -> Vec<u32> {
        self.0.clone()
    }
    async fn complete(self: Box<Self>) {
        let _ = self.1.send(());
    }
}

#[async_trait]
impl ConsumerIterator for FakeIterator {
    fn start(&mut self) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(Call::Start);
        Ok(())
    }
    async fn next(&mut self) -> anyhow::Result<NextResult> {
        match self.next.recv().await {
            Some(Next::Record(partition, offset, payload)) => {
                Ok(NextResult::Record(ConsumerRecord {
                    virtual_partition_id: partition,
                    offset,
                    source_checkpoint: CommittedSourceCheckpoint {
                        window_start_unix_seconds: 0,
                        snowflake_id: 0,
                    },
                    record: Message {
                        payload: payload.into(),
                        ..Default::default()
                    },
                }))
            }
            Some(Next::Revoke(partitions, done)) => {
                Ok(NextResult::Revoked(Box::new(FakeRevoked(partitions, done))))
            }
            None => std::future::pending().await,
        }
    }
    fn store_offset(&mut self, partition: u32, offset: u64) -> anyhow::Result<()> {
        self.calls
            .lock()
            .unwrap()
            .push(Call::Store(partition, offset));
        Ok(())
    }
    async fn commit(&mut self) -> anyhow::Result<HeartbeatReport> {
        self.calls.lock().unwrap().push(Call::Commit);
        Ok(HeartbeatReport {
            renewed_partitions: vec![],
            fenced_partitions: vec![],
        })
    }
    async fn shutdown(self: Box<Self>) -> anyhow::Result<()> {
        self.calls.lock().unwrap().push(Call::Shutdown);
        Ok(())
    }
    async fn seek(&mut self, _: u32, _: ConsumerSeekTarget) -> anyhow::Result<()> {
        unreachable!("the loader never seeks")
    }
}

/// Lands into a shared list, failing the next `fail` appends.
#[derive(Clone, Default)]
struct Shared {
    landed: Arc<Mutex<Vec<Vec<LandingRow>>>>,
    fail: Arc<AtomicUsize>,
    attempts: Arc<AtomicUsize>,
}

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

struct FakeLand(Shared);

impl Land for FakeLand {
    fn land(&mut self, rows: &[LandingRow]) -> Result<(), WarehouseError> {
        self.0.attempts.fetch_add(1, Ordering::SeqCst);
        if self
            .0
            .fail
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok()
        {
            return Err(WarehouseError::other("Snowpipe Streaming unavailable"));
        }
        self.0.landed.lock().unwrap().push(rows.to_vec());
        Ok(())
    }
}

struct Harness {
    feed: mpsc::UnboundedSender<Next>,
    calls: Arc<Mutex<Vec<Call>>>,
    shared: Shared,
    stop: CancellationToken,
    events: Arc<Mutex<Vec<String>>>,
    done: tokio::task::JoinHandle<anyhow::Result<()>>,
}

fn start(settings: Settings, shared: Shared) -> Harness {
    let (feed, next) = mpsc::unbounded_channel();
    let calls = Arc::new(Mutex::new(Vec::new()));
    let iterator = Box::new(FakeIterator {
        next,
        calls: calls.clone(),
    });
    let mut lander = FakeLand(shared.clone());
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
    let stop = CancellationToken::new();
    let events = Arc::new(Mutex::new(Vec::new()));
    let (s, e) = (stop.clone(), events.clone());
    let done = tokio::spawn(async move {
        run(iterator, &mut loader, &mut lander, &settings, s, |event| {
            e.lock().unwrap().push(format!("{event:?}"));
        })
        .await
    });
    Harness {
        feed,
        calls,
        shared,
        stop,
        events,
        done,
    }
}

impl Harness {
    fn record(&self, partition: u32, offset: u64, txid: u64) {
        let _ = self
            .feed
            .send(Next::Record(partition, offset, record(txid).to_json()));
    }

    fn calls(&self) -> Vec<Call> {
        self.calls.lock().unwrap().clone()
    }

    fn landed(&self) -> Vec<Vec<LandingRow>> {
        self.shared.landed.lock().unwrap().clone()
    }

    async fn until(&self, what: &str, mut ready: impl FnMut(&Self) -> bool) {
        let deadline = Instant::now() + Duration::from_secs(10);
        while !ready(self) {
            assert!(
                Instant::now() < deadline,
                "timed out waiting for {what}: calls {:?}, events {:?}",
                self.calls(),
                self.events.lock().unwrap()
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    async fn stop(self) -> (Vec<Call>, Vec<Vec<LandingRow>>) {
        self.stop.cancel();
        let (calls, shared) = (self.calls.clone(), self.shared.clone());
        self.done.await.unwrap().unwrap();
        let calls = calls.lock().unwrap().clone();
        let landed = shared.landed.lock().unwrap().clone();
        (calls, landed)
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

fn settings(records: usize, linger: Duration) -> Settings {
    Settings {
        limits: Limits {
            records,
            bytes: 1 << 20,
        },
        linger,
        sync_every: Duration::from_secs(3600),
        retry: Duration::from_millis(10),
        retry_max: Duration::from_millis(20),
    }
}

fn txids(batch: &[LandingRow]) -> Vec<u64> {
    batch.iter().map(|r| r.txid).collect()
}

// ---------------------------------------------------------------- loop

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_lands_after_linger_and_then_commits_its_offsets() {
    let h = start(settings(100, Duration::from_millis(50)), Shared::default());
    h.record(1, 10, 1);
    h.record(2, 4, 2);
    h.record(1, 11, 3);
    h.until("a commit", |h| h.calls().contains(&Call::Commit))
        .await;
    assert_eq!(
        h.calls(),
        [
            Call::Start,
            Call::Store(1, 11),
            Call::Store(2, 4),
            Call::Commit
        ]
    );
    let (calls, landed) = h.stop().await;
    assert_eq!(landed.len(), 1);
    assert_eq!(txids(&landed[0]), [1, 2, 3]);
    assert_eq!(landed[0][0].source, "blob-stream/1/10");
    assert_eq!(calls.last(), Some(&Call::Shutdown));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_full_batch_lands_at_once() {
    let h = start(settings(2, Duration::from_secs(3600)), Shared::default());
    for (offset, txid) in [(1, 1), (2, 2), (3, 3)] {
        h.record(0, offset, txid);
    }
    h.until("the first batch", |h| !h.landed().is_empty()).await;
    assert_eq!(txids(&h.landed()[0]), [1, 2]);
    assert_eq!(&h.calls()[1..], [Call::Store(0, 2), Call::Commit]);
    // Stopping lands what is left.
    let (calls, landed) = h.stop().await;
    assert_eq!(txids(&landed[1]), [3]);
    assert_eq!(
        &calls[3..],
        [Call::Store(0, 3), Call::Commit, Call::Shutdown]
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_batch_is_retried_and_nothing_commits_until_it_lands() {
    let shared = Shared::default();
    shared.fail.store(3, Ordering::SeqCst);
    let h = start(settings(2, Duration::from_secs(3600)), shared);
    h.record(0, 1, 1);
    h.record(0, 2, 2);
    h.until("the batch to land", |h| !h.landed().is_empty())
        .await;
    assert_eq!(h.shared.attempts.load(Ordering::SeqCst), 4);
    assert_eq!(h.landed().len(), 1, "the same batch, landed once");
    assert_eq!(&h.calls()[1..], [Call::Store(0, 2), Call::Commit]);
    let failures = h
        .events
        .lock()
        .unwrap()
        .iter()
        .filter(|e| e.starts_with("LandFailed"))
        .count();
    assert_eq!(failures, 3);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopping_while_the_warehouse_is_down_commits_nothing() {
    let shared = Shared::default();
    shared.fail.store(usize::MAX, Ordering::SeqCst);
    let h = start(settings(1, Duration::from_secs(3600)), shared);
    h.record(0, 1, 1);
    h.until("a failed landing", |h| {
        h.shared.attempts.load(Ordering::SeqCst) >= 2
    })
    .await;
    let (calls, landed) = h.stop().await;
    assert!(landed.is_empty());
    assert_eq!(calls, [Call::Start, Call::Shutdown]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn revoked_partitions_are_let_go_only_after_their_batch_lands() {
    let h = start(settings(100, Duration::from_secs(3600)), Shared::default());
    h.record(4, 7, 1);
    let (done, completed) = oneshot::channel();
    let _ = h.feed.send(Next::Revoke(vec![4], done));
    completed.await.unwrap();
    // By the time the revocation completes, the batch has landed and its
    // offset is committed.
    assert_eq!(txids(&h.landed()[0]), [1]);
    assert_eq!(&h.calls()[1..], [Call::Store(4, 7), Call::Commit]);
    h.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_message_that_is_not_a_record_is_skipped_and_committed() {
    let h = start(settings(2, Duration::from_secs(3600)), Shared::default());
    let _ = h.feed.send(Next::Record(0, 1, b"{\"kind\":".to_vec()));
    h.record(0, 2, 2);
    h.record(0, 3, 3);
    h.until("the batch", |h| !h.landed().is_empty()).await;
    assert_eq!(txids(&h.landed()[0]), [2, 3]);
    assert_eq!(&h.calls()[1..], [Call::Store(0, 3), Call::Commit]);
    assert!(h.events.lock().unwrap()[0].starts_with("Skipped"));
    h.stop().await;
}

// ---------------------------------------------------------------- config

#[test]
fn the_example_config_fills_in_group_member_and_topics() {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/consumer.yaml");
    let config = bootstrap_config(&path, None, Some("loader-0")).unwrap();
    let runtime = config.runtime.as_ref().unwrap();
    let group = runtime.group.as_ref().unwrap();
    assert_eq!(group.group_id.to_string(), DEFAULT_GROUP);
    assert_eq!(group.member_id.to_string(), "loader-0");
    assert_eq!(group.topic.to_string(), "celld-changes");
    assert_eq!(
        runtime.read.as_ref().unwrap().topic.to_string(),
        "celld-changes"
    );
    // blob-stream accepts it as a complete bootstrap config.
    ConsumerBootstrapConfig::from_proto_config(&config).unwrap();

    let other = bootstrap_config(&path, Some("snowflake-staging"), Some("m")).unwrap();
    assert_eq!(
        other
            .runtime
            .as_ref()
            .unwrap()
            .group
            .as_ref()
            .unwrap()
            .group_id
            .to_string(),
        "snowflake-staging"
    );
}

#[test]
fn a_member_id_is_required_and_the_format_follows_the_extension() {
    let dir = std::env::temp_dir().join(format!("celld-export-loader-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let example = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/consumer.yaml");
    let yaml = std::fs::read_to_string(&example).unwrap();
    let value: serde_json::Value = serde_yaml::from_str(&yaml).unwrap();

    let json = dir.join("consumer.json");
    std::fs::write(&json, value.to_string()).unwrap();
    let error = bootstrap_config(&json, None, None).unwrap_err().to_string();
    assert!(error.contains("EXPORT_MEMBER_ID"), "{error}");
    bootstrap_config(&json, None, Some("m")).unwrap();

    let txt = dir.join("consumer.txt");
    std::fs::write(&txt, &yaml).unwrap();
    assert!(bootstrap_config(&txt, None, Some("m")).is_err());
    std::fs::remove_dir_all(&dir).unwrap();
}
