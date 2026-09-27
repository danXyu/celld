// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Idle follower probes: a follower that departs while the leader takes no
//! writes must still lead the ensemble off it.

use super::*;
use celld_logic::log_evict::PROBE_FAILURES_TO_DEGRADE;

fn run(future: impl std::future::Future<Output = ()>) {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    let runtime = RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap()
    });
    crate::asyncrt::set_host_handle(runtime.handle().clone());
    runtime.block_on(future);
}

/// `live` answers every append; `gone` refuses the connection, the way a
/// deleted pod's address does.
struct OneGone;

impl LogTransport for OneGone {
    fn post<'a>(
        &'a self,
        node: &'a str,
        _addr: &'a str,
        _path: &'a str,
        _body: Vec<u8>,
        _deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async move {
            anyhow::ensure!(node == "live", "connection refused");
            Ok(bytes::Bytes::from_static(
                br#"{"ok":true,"end":0,"epoch":1}"#,
            ))
        })
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    manager: Arc<NodeLogManager>,
    tasks: crate::ltx_repl::LtxTaskOwner,
}

impl Fixture {
    fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("bucket.sqlite");
        let bucket = Arc::new(Bucket::open_dev(&database).unwrap());
        let ownership = Arc::new(
            crate::ownership_store::BucketOwnership::new(
                (*bucket).clone(),
                (*bucket).clone(),
                "leader".into(),
                "g".into(),
            )
            .with_lease_ttl_ms(10_000),
        );
        let own_log = Arc::new(OwnLog {
            ownership,
            nudge: Box::new(|| {}),
            write_lock: tokio::sync::Mutex::new(()),
        });
        let ltx = Arc::new(
            crate::ltx_repl::LtxRepl::start(
                dir.path(),
                crate::bucket::StorageBackend::Local,
                database.to_str().unwrap().into(),
                String::new(),
                None,
                "auto".into(),
                None,
            )
            .unwrap(),
        );
        let tasks = ltx.take_task_owner();
        let manager = Arc::new(NodeLogManager::new_with_log_transport(
            "leader/g",
            bucket,
            own_log,
            ltx,
            Arc::new(OneGone),
            Default::default(),
        ));
        Self {
            _dir: dir,
            manager,
            tasks,
        }
    }

    /// Install an idle shipper over `live` and `gone` without lanes: the
    /// probes post through the shipper's transport directly.
    fn install(&self) -> Arc<FleetShipper> {
        let manager = &self.manager;
        let members = vec![
            Member {
                node: "live".into(),
                addr: "live:1".into(),
            },
            Member {
                node: "gone".into(),
                addr: "gone:1".into(),
            },
        ];
        let ensemble = members.iter().map(|member| member.node.clone()).collect();
        let record = log_tier::create_record(ensemble, 0).unwrap();
        let shipper = Arc::new(FleetShipper {
            node: manager.session.clone(),
            transport: manager.transport.clone(),
            live_log: manager.live_log.clone(),
            epoch: record.epoch,
            record,
            activated: Arc::new(std::sync::atomic::AtomicBool::new(true)),
            members,
            lanes: Vec::new(),
            pipeline: 1,
            seq: std::sync::atomic::AtomicU64::new(0),
            degraded: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            outstanding: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            policy: manager.policy.clone(),
            stream: None,
        });
        *manager.inner.lock().unwrap() = Some(shipper.clone());
        shipper
    }

    /// One probe round with no quiet interval, awaited until both members'
    /// probes have settled.
    async fn probe_round(&self) {
        // The quiet check is strict: let the clock move past the last
        // settled probe.
        crate::asyncrt::sleep(std::time::Duration::from_millis(2)).await;
        let before = self.settled();
        self.manager.probe_followers_quiet(0);
        let deadline = mono_ms() + 5_000;
        loop {
            let now = self.settled();
            if now.0 > before.0 && now.1 > before.1 {
                break;
            }
            assert!(mono_ms() < deadline, "probe round never settled");
            crate::asyncrt::sleep(std::time::Duration::from_millis(2)).await;
        }
    }

    fn settled(&self) -> (usize, u32) {
        let health = self.manager.health.lock().unwrap();
        (health.sample_count("live"), health.probe_failures("gone"))
    }

    async fn stop(self) {
        self.manager.task_stop.request_stop();
        self.manager.child_tasks.join().await;
        self.tasks.request_stop();
        self.tasks.join().await;
    }
}

#[test]
fn idle_probes_degrade_off_a_departed_follower() {
    run(async {
        let fixture = Fixture::new();
        let shipper = fixture.install();
        for round in 1..=PROBE_FAILURES_TO_DEGRADE {
            fixture.probe_round().await;
            let health = fixture.manager.health.lock().unwrap();
            // The refused probe is a failure, never a healthy sample.
            assert_eq!(health.sample_count("gone"), 0);
            assert_eq!(health.probe_failures("gone"), round);
            // The answering member keeps feeding the ledger as before.
            assert_eq!(health.probe_failures("live"), 0);
            assert!(health.sample_count("live") >= 1);
            drop(health);
            assert_eq!(
                shipper.is_active(),
                round < PROBE_FAILURES_TO_DEGRADE,
                "round {round}"
            );
        }
        // Degraded: acks ride the bucket and maintenance takes it from here.
        assert!(!fixture.manager.healthy());
        fixture.stop().await;
    });
}

#[test]
fn an_answered_probe_keeps_a_blipping_follower() {
    run(async {
        let fixture = Fixture::new();
        let shipper = fixture.install();
        let health = &fixture.manager.health;
        let suspect_self = &fixture.manager.suspect_self;
        let failed = || AppendSend::Failed(anyhow!("connection reset"));
        let answered = || {
            AppendSend::Answered(AppendResp {
                ok: true,
                end: 0,
                epoch: Some(shipper.epoch),
            })
        };
        // Short of the threshold, a blip leaves the ensemble alone ...
        for _ in 1..PROBE_FAILURES_TO_DEGRADE {
            settle_probe(&shipper, health, suspect_self, "gone", mono_ms(), failed());
        }
        assert!(shipper.is_active());
        // ... and an answer starts the count over.
        settle_probe(
            &shipper,
            health,
            suspect_self,
            "gone",
            mono_ms(),
            answered(),
        );
        assert_eq!(health.lock().unwrap().probe_failures("gone"), 0);
        assert_eq!(health.lock().unwrap().sample_count("gone"), 1);
        for _ in 1..PROBE_FAILURES_TO_DEGRADE {
            settle_probe(&shipper, health, suspect_self, "gone", mono_ms(), failed());
        }
        assert!(shipper.is_active());
        settle_probe(&shipper, health, suspect_self, "gone", mono_ms(), failed());
        assert!(!shipper.is_active());
        fixture.stop().await;
    });
}

#[test]
fn an_answered_probe_lifts_self_suspicion() {
    run(async {
        let fixture = Fixture::new();
        let shipper = fixture.install();
        let manager = &fixture.manager;
        manager.suspect_self.store(true, Ordering::SeqCst);
        settle_probe(
            &shipper,
            &manager.health,
            &manager.suspect_self,
            "gone",
            mono_ms(),
            AppendSend::Failed(anyhow!("connection refused")),
        );
        // A failure is no evidence of our own connectivity.
        assert!(manager.suspect_self.load(Ordering::SeqCst));
        fixture.probe_round().await;
        assert!(!manager.suspect_self.load(Ordering::SeqCst));
        assert!(shipper.is_active());
        fixture.stop().await;
    });
}
