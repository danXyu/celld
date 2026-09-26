// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};

const PREDECESSOR: &str = "leader/old";
const LOSS: &str = "log/leader/old.e1.loss.json";
const STALE_BUNDLE: &str = "log/leader/older/bundle/retained";

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

struct RetainedWitness {
    online: AtomicBool,
    attempts: AtomicUsize,
    follower: FollowerStore,
}

impl LogTransport for RetainedWitness {
    fn post<'a>(
        &'a self,
        node: &'a str,
        addr: &'a str,
        path: &'a str,
        body: Vec<u8>,
        _deadline: Option<std::time::Duration>,
    ) -> std::pin::Pin<
        Box<dyn std::future::Future<Output = anyhow::Result<bytes::Bytes>> + Send + 'a>,
    > {
        Box::pin(async move {
            assert_eq!(node, "witness");
            assert_eq!(addr, "witness.test:8081");
            self.attempts.fetch_add(1, Ordering::SeqCst);
            anyhow::ensure!(self.online.load(Ordering::SeqCst), "witness is offline");
            match path {
                "/peer/log/seal" => Ok(serde_json::to_vec(
                    &self.follower.seal(&serde_json::from_slice(&body)?).await?,
                )?
                .into()),
                "/peer/log/tail" => {
                    let req: TailWireReq = serde_json::from_slice(&body)?;
                    Ok(req
                        .encode_response(&self.follower.checked_tail(&req.request).await?)?
                        .into())
                }
                _ => panic!("unexpected recovery request {path}"),
            }
        })
    }
}

struct Fixture {
    _dir: tempfile::TempDir,
    bucket: Arc<Bucket>,
    manager: NodeLogManager,
    witness: Arc<RetainedWitness>,
    acknowledged: Vec<u8>,
    tasks: crate::ltx_repl::LtxTaskOwner,
}

impl Fixture {
    async fn new(retain_fragment: bool, bucket_complete: bool) -> Self {
        Self::with_witness_store(retain_fragment, bucket_complete, "witness").await
    }

    /// A fixture whose process at the witness address runs its follower
    /// store under `store_node`, which differs from "witness" when another
    /// node answers at the member's address.
    async fn with_witness_store(
        retain_fragment: bool,
        bucket_complete: bool,
        store_node: &str,
    ) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let database = dir.path().join("bucket.sqlite");
        let bucket = Arc::new(Bucket::open_dev(&database).unwrap());
        // Much older than the former three-TTL grace. Expiry proves fencing,
        // not the fate of the retained follower disk.
        bucket
            .put(
                "nodes/leader.json",
                serde_json::to_vec(&serde_json::json!({
                    "node": "leader", "expires_ms": 1, "ownership_index_generation": "old",
                    "log": {"epoch": 1, "state": "open", "tiered": 0,
                            "ensemble": ["witness"], "active": true,
                            "bucket_complete": bucket_complete}
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        bucket
            .put(
                "nodes/witness.json",
                serde_json::to_vec(&serde_json::json!({
                    "node": "witness", "expires_ms": 1, "addr": "witness.test:8081",
                    "ownership_index_generation": "retained"
                }))
                .unwrap(),
            )
            .await
            .unwrap();
        bucket
            .put(STALE_BUNDLE, b"must survive unsuccessful boot".to_vec())
            .await
            .unwrap();
        let follower = FollowerStore::new(dir.path(), Some(bucket.clone()), store_node);
        let mut page = vec![0_u8; 512];
        let tail = b"acknowledged write retained only on the follower";
        page[..tail.len()].copy_from_slice(tail);
        let acknowledged = celld_ltx::ltx::encode_file(
            &celld_ltx::ltx::Header {
                version: celld_ltx::ltx::VERSION,
                flags: celld_ltx::ltx::HEADER_FLAG_NO_CHECKSUM,
                page_size: 512,
                commit: 1,
                min_txid: celld_ltx::TXID(1),
                max_txid: celld_ltx::TXID(1),
                ..Default::default()
            },
            &[(1, page)],
            0,
        )
        .unwrap();
        if retain_fragment {
            let response = follower
                .append(AppendReq {
                    leader: PREDECESSOR.into(),
                    epoch: 1,
                    truncate_to: 0,
                    entries: vec![Entry {
                        seq: 1,
                        cell: "acknowledged".into(),
                        cell_epoch: 1,
                        txid: 1,
                        bytes: acknowledged.clone(),
                    }],
                })
                .await;
            assert!(
                append_confirms(1, 1, &response),
                "fixture must fsync the acknowledged tail"
            );
        }
        let witness = Arc::new(RetainedWitness {
            online: AtomicBool::new(false),
            attempts: AtomicUsize::new(0),
            follower,
        });
        let ownership = Arc::new(
            crate::ownership_store::BucketOwnership::new(
                (*bucket).clone(),
                (*bucket).clone(),
                "leader".into(),
                "new".into(),
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
        let manager = NodeLogManager::new_with_log_transport(
            "leader/new",
            bucket.clone(),
            own_log,
            ltx,
            witness.clone(),
            Default::default(),
        );
        Self {
            _dir: dir,
            bucket,
            manager,
            witness,
            acknowledged,
            tasks,
        }
    }

    /// Republish the witness lease naming a disk incarnation, as a witness
    /// process on this fork does on every renewal.
    async fn publish_witness_incarnation(&self, incarnation: &str) {
        self.bucket
            .put(
                "nodes/witness.json",
                serde_json::to_vec(&serde_json::json!({
                    "node": "witness", "expires_ms": 1, "addr": "witness.test:8081",
                    "ownership_index_generation": "retained",
                    "disk_incarnation": incarnation,
                }))
                .unwrap(),
            )
            .await
            .unwrap();
    }

    async fn assert_undecided(&self) {
        let folded = read_record(&self.bucket, PREDECESSOR)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(folded.wire.generation, "old");
        assert_eq!(folded.record.state, LogState::Recovering);
        assert!(!folded.record.bucket_complete);
        assert!(!self.manager.predecessors_clean.load(Ordering::SeqCst));
        assert!(self.bucket.get(LOSS).await.unwrap().is_none());
        assert!(self.bucket.get(STALE_BUNDLE).await.unwrap().is_some());
        assert!(self.bucket.list("cells/").await.unwrap().is_empty());
    }

    async fn stop(self) {
        self.tasks.request_stop();
        self.tasks.join().await;
    }
}

#[test]
fn expired_unavailable_witness_never_seals_or_cleans_up() {
    run(async {
        let fixture = Fixture::new(true, false).await;
        // Exhaust the same number of attempts as startup, without sleeping:
        // no amount of elapsed lease age supplies evidence about this disk.
        for _ in 0..4 {
            let error = fixture.manager.recover_self().await.unwrap_err();
            assert!(
                error.to_string().contains("member(s) undecided"),
                "{error:#}"
            );
            fixture.assert_undecided().await;
            assert_eq!(
                fixture
                    .witness
                    .follower
                    .tail(&TailReq {
                        leader: PREDECESSOR.into(),
                        member: None,
                        incarnation: None,
                    }).await.unwrap()
                    .entries[0]
                    .bytes,
                fixture.acknowledged
            );
        }
        assert_eq!(fixture.witness.attempts.load(Ordering::SeqCst), 4);
        fixture.stop().await;
    });
}

#[test]
fn unavailable_retained_witness_recovers_exact_tail_when_it_returns() {
    run(async {
        let fixture = Fixture::new(true, false).await;
        assert!(fixture.manager.recover_self().await.is_err());
        fixture.assert_undecided().await;
        fixture.witness.online.store(true, Ordering::SeqCst);
        fixture.manager.recover_self().await.unwrap();
        let folded = read_record(&fixture.bucket, PREDECESSOR)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(folded.record.state, LogState::Sealed);
        let objects = fixture
            .bucket
            .list("cells/acknowledged/ltx/e1/")
            .await
            .unwrap();
        assert_eq!(objects.len(), 1);
        assert_eq!(
            fixture
                .bucket
                .get(objects[0].location.as_ref())
                .await
                .unwrap()
                .unwrap()
                .0
                .as_ref(),
            fixture.acknowledged
        );
        assert!(fixture.bucket.get(LOSS).await.unwrap().is_none());
        assert!(fixture.manager.predecessors_clean.load(Ordering::SeqCst));
        fixture.stop().await;
    });
}

#[test]
fn missing_witness_address_remains_undecided() {
    run(async {
        let fixture = Fixture::new(true, false).await;
        fixture.bucket.delete("nodes/witness.json").await.unwrap();
        assert!(fixture.manager.recover_self().await.is_err());
        fixture.assert_undecided().await;
        assert_eq!(fixture.witness.attempts.load(Ordering::SeqCst), 0);
        fixture.stop().await;
    });
}

#[test]
fn reachable_witness_without_fragment_keeps_explicit_loss_policy() {
    run(async {
        let fixture = Fixture::new(false, false).await;
        fixture.witness.online.store(true, Ordering::SeqCst);
        fixture.manager.recover_self().await.unwrap();
        assert_eq!(
            read_record(&fixture.bucket, PREDECESSOR)
                .await
                .unwrap()
                .unwrap()
                .record
                .state,
            LogState::Sealed
        );
        assert!(fixture.bucket.get(LOSS).await.unwrap().is_some());
        fixture.stop().await;
    });
}

#[test]
fn bucket_complete_does_not_require_an_unavailable_witness() {
    run(async {
        let fixture = Fixture::new(false, true).await;
        fixture.manager.recover_self().await.unwrap();
        assert_eq!(
            read_record(&fixture.bucket, PREDECESSOR)
                .await
                .unwrap()
                .unwrap()
                .record
                .state,
            LogState::Sealed
        );
        assert_eq!(fixture.witness.attempts.load(Ordering::SeqCst), 0);
        assert!(fixture.bucket.get(LOSS).await.unwrap().is_none());
        fixture.stop().await;
    });
}

#[test]
fn a_replacement_disk_under_the_member_name_records_the_loss() {
    run(async {
        // The witness lease still names the disk that held the fragment,
        // because the replacement has not yet installed a lease of its own.
        // The member's name now answers from an empty replacement disk: the
        // named disk is gone, so the answer is conclusive and recovery
        // records the bounded loss instead of waiting for a disk that
        // cannot return.
        let fixture = Fixture::new(false, false).await;
        let superseded = "0123456789abcdef0123456789abcdef";
        assert_ne!(fixture.witness.follower.incarnation().unwrap(), superseded);
        fixture.publish_witness_incarnation(superseded).await;
        fixture.witness.online.store(true, Ordering::SeqCst);
        fixture.manager.recover_self().await.unwrap();
        assert_eq!(
            read_record(&fixture.bucket, PREDECESSOR)
                .await
                .unwrap()
                .unwrap()
                .record
                .state,
            LogState::Sealed
        );
        assert!(fixture.bucket.get(LOSS).await.unwrap().is_some());
        assert!(fixture.manager.predecessors_clean.load(Ordering::SeqCst));
        // The replacement disk sealed the epoch like any conclusive member,
        // so a straggling append from the dead leader is refused there too.
        assert_eq!(fixture.witness.follower.load(PREDECESSOR).unwrap().sealed_to, 1);
        fixture.stop().await;
    });
}

#[test]
fn another_member_at_the_witness_address_stays_undecided() {
    run(async {
        // A different node answers at the witness's address. It says
        // nothing about the witness's disk, whatever its incarnation.
        let fixture = Fixture::with_witness_store(false, false, "someone-else").await;
        let incarnation = fixture.witness.follower.incarnation().unwrap();
        fixture.publish_witness_incarnation(&incarnation).await;
        fixture.witness.online.store(true, Ordering::SeqCst);
        let error = fixture.manager.recover_self().await.unwrap_err();
        assert!(
            error.to_string().contains("member(s) undecided"),
            "{error:#}"
        );
        fixture.assert_undecided().await;
        // The refusal came before the seal mark: the other node's disk
        // carries no trace of a seal it was never entitled to answer.
        assert_eq!(fixture.witness.follower.load(PREDECESSOR).unwrap().sealed_to, 0);
        assert_eq!(fixture.witness.attempts.load(Ordering::SeqCst), 1);
        fixture.stop().await;
    });
}

#[test]
fn the_disk_its_lease_names_keeps_the_explicit_loss_policy() {
    run(async {
        // Once the machine's own lease names its empty disk, that disk is
        // the member's disk of record and its answer is conclusive again.
        let fixture = Fixture::new(false, false).await;
        let incarnation = fixture.witness.follower.incarnation().unwrap();
        fixture.publish_witness_incarnation(&incarnation).await;
        fixture.witness.online.store(true, Ordering::SeqCst);
        fixture.manager.recover_self().await.unwrap();
        assert!(fixture.bucket.get(LOSS).await.unwrap().is_some());
        fixture.stop().await;
    });
}

#[test]
fn a_matching_incarnation_recovers_the_retained_tail() {
    run(async {
        let fixture = Fixture::new(true, false).await;
        let incarnation = fixture.witness.follower.incarnation().unwrap();
        fixture.publish_witness_incarnation(&incarnation).await;
        fixture.witness.online.store(true, Ordering::SeqCst);
        fixture.manager.recover_self().await.unwrap();
        assert_eq!(
            read_record(&fixture.bucket, PREDECESSOR)
                .await
                .unwrap()
                .unwrap()
                .record
                .state,
            LogState::Sealed
        );
        assert_eq!(
            fixture
                .bucket
                .list("cells/acknowledged/ltx/e1/")
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(fixture.bucket.get(LOSS).await.unwrap().is_none());
        fixture.stop().await;
    });
}
