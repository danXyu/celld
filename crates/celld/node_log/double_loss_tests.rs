// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Two fleet members lose their disks at once and both come back under their
//! own names on empty disks. Each replacement must recover its predecessor
//! session before it installs a lease, and the only ensemble member of that
//! session is the other replacement, whose lease still names its lost disk.
//! Refusing the replacement's answer there deadlocked the pair forever; the
//! fleet must record a bounded loss for each session and continue. A
//! three-member fleet that loses two disks recovers from the survivor when it
//! holds a copy, and records the loss when it does not.

use super::*;
use std::sync::atomic::AtomicUsize;

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

const PAIR: [&str; 2] = ["pair-0", "pair-1"];

fn address(node: &str) -> String {
    format!("{node}.pair.test:8081")
}

/// Routes each peer request to the follower store of the named node, the way
/// the stable per-name DNS address reaches whichever Pod holds that name.
struct PairTransport {
    followers: BTreeMap<String, FollowerStore>,
    requests: AtomicUsize,
}

impl LogTransport for PairTransport {
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
            self.requests.fetch_add(1, Ordering::SeqCst);
            assert_eq!(addr, address(node));
            let follower = &self.followers[node];
            match path {
                "/peer/log/seal" => Ok(serde_json::to_vec(
                    &follower.seal(&serde_json::from_slice(&body)?).await?,
                )?
                .into()),
                "/peer/log/tail" => {
                    let req: TailWireReq = serde_json::from_slice(&body)?;
                    Ok(req
                        .encode_response(&follower.checked_tail(&req.request).await?)?
                        .into())
                }
                _ => panic!("unexpected recovery request {path}"),
            }
        })
    }
}

struct Replacement {
    manager: NodeLogManager,
    tasks: crate::ltx_repl::LtxTaskOwner,
}

async fn replacement(
    node: &str,
    bucket: &Arc<Bucket>,
    database: &std::path::Path,
    data: &std::path::Path,
    transport: Arc<PairTransport>,
) -> Replacement {
    let ownership = Arc::new(
        crate::ownership_store::BucketOwnership::new(
            (**bucket).clone(),
            (**bucket).clone(),
            node.into(),
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
            data,
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
        &format!("{node}/new"),
        bucket.clone(),
        own_log,
        ltx,
        transport,
        Default::default(),
    );
    Replacement { manager, tasks }
}

/// A fleet in which `lost` members lost their disks at once. Every lost
/// member's old session was an active fleet log whose ensemble is every other
/// fleet member, and every lease still names the disk it had: no replacement
/// has installed a lease yet. Survivors keep their disks and, when
/// `survivors_retain` is set, hold each lost session's acknowledged frame.
struct Fleet {
    _shared: tempfile::TempDir,
    _disks: Vec<tempfile::TempDir>,
    bucket: Arc<Bucket>,
    transport: Arc<PairTransport>,
    replacements: Vec<Replacement>,
}

fn frame(cell: &str) -> Vec<u8> {
    let mut page = vec![0_u8; 512];
    page[..cell.len()].copy_from_slice(cell.as_bytes());
    celld_ltx::ltx::encode_file(
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
    .unwrap()
}

impl Fleet {
    async fn new(members: &[&str], lost: usize, survivors_retain: bool) -> Self {
        let shared = tempfile::tempdir().unwrap();
        let database = shared.path().join("bucket.sqlite");
        let bucket = Arc::new(Bucket::open_dev(&database).unwrap());
        let superseded: Vec<String> = (0..members.len())
            .map(|index| format!("{index:032x}"))
            .collect();
        for (index, node) in members.iter().enumerate() {
            let ensemble: Vec<&str> = members.iter().copied().filter(|m| m != node).collect();
            let mut lease = serde_json::json!({
                "node": node, "expires_ms": 1, "addr": address(node),
                "ownership_index_generation": "old",
                "disk_incarnation": superseded[index],
            });
            if index < lost {
                lease["log"] = serde_json::json!({
                    "epoch": 1, "state": "open", "tiered": 0,
                    "ensemble": ensemble, "active": true,
                    "bucket_complete": false,
                });
            }
            bucket
                .put(
                    &format!("nodes/{node}.json"),
                    serde_json::to_vec(&lease).unwrap(),
                )
                .await
                .unwrap();
        }
        let disks: Vec<tempfile::TempDir> = members
            .iter()
            .map(|_| tempfile::tempdir().unwrap())
            .collect();
        let mut followers = BTreeMap::new();
        for (index, (node, disk)) in members.iter().zip(&disks).enumerate() {
            let store = FollowerStore::new(disk.path(), Some(bucket.clone()), node);
            if index < lost {
                // A replacement: a fresh, empty disk under the same name.
                assert!(!superseded.contains(&store.incarnation().unwrap()));
            } else {
                // A survivor: the disk its lease names.
                store.filesystem.create_dir_all(&store.root).unwrap();
                store
                    .filesystem
                    .write(
                        &store.root.join(INCARNATION_FILE),
                        superseded[index].as_bytes(),
                    )
                    .unwrap();
                assert_eq!(store.incarnation().unwrap(), superseded[index]);
                if survivors_retain {
                    for dead in &members[..lost] {
                        let response = store
                            .append(AppendReq {
                                leader: format!("{dead}/old"),
                                epoch: 1,
                                truncate_to: 0,
                                entries: vec![Entry {
                                    seq: 1,
                                    cell: format!("cell-{dead}"),
                                    cell_epoch: 1,
                                    txid: 1,
                                    bytes: frame(&format!("cell-{dead}")),
                                }],
                            })
                            .await;
                        assert!(append_confirms(1, 1, &response));
                    }
                }
            }
            followers.insert(node.to_string(), store);
        }
        let transport = Arc::new(PairTransport {
            followers,
            requests: AtomicUsize::new(0),
        });
        let mut replacements = Vec::new();
        for (node, disk) in members[..lost].iter().zip(&disks) {
            replacements
                .push(replacement(node, &bucket, &database, disk.path(), transport.clone()).await);
        }
        Self {
            _shared: shared,
            _disks: disks,
            bucket,
            transport,
            replacements,
        }
    }

    /// Every replacement boots at once and runs its predecessor recovery.
    async fn recover_all(&self) {
        let results = futures_util::future::join_all(
            self.replacements
                .iter()
                .map(|replacement| replacement.manager.recover_self()),
        )
        .await;
        for result in results {
            result.unwrap();
        }
    }

    async fn loss(&self, node: &str) -> Option<serde_json::Value> {
        self.bucket
            .get(&format!("log/{node}/old.e1.loss.json"))
            .await
            .unwrap()
            .map(|(bytes, _)| serde_json::from_slice(&bytes).unwrap())
    }

    async fn assert_sealed(&self, node: &str) {
        let folded = read_record(&self.bucket, node).await.unwrap().unwrap();
        assert_eq!(folded.record.state, LogState::Sealed, "{node}");
    }

    async fn stop(self) {
        for replacement in self.replacements {
            replacement.tasks.request_stop();
            replacement.tasks.join().await;
        }
    }
}

#[test]
fn a_pair_that_loses_both_disks_records_the_loss_and_recovers() {
    run(async {
        // Each old session's sole ensemble member is the other node, whose
        // lease still names its lost disk. Both boots need the other
        // replacement's answer, and neither lease changes.
        let fleet = Fleet::new(&PAIR, 2, false).await;
        fleet.recover_all().await;
        for (index, node) in PAIR.iter().enumerate() {
            fleet.assert_sealed(node).await;
            let loss = fleet
                .loss(node)
                .await
                .unwrap_or_else(|| panic!("{node} recorded no loss"));
            assert_eq!(loss["leader"], format!("{node}/old"));
            assert_eq!(loss["ensemble"], serde_json::json!([PAIR[1 - index]]));
            assert!(fleet.replacements[index]
                .manager
                .predecessors_clean
                .load(Ordering::SeqCst));
            // The other replacement sealed this session's epoch on its disk.
            assert_eq!(
                fleet.transport.followers[PAIR[1 - index]]
                    .load(&format!("{node}/old"))
                    .unwrap().sealed_to,
                1
            );
        }
        // One seal and one tail per session: no refusal loop.
        assert_eq!(fleet.transport.requests.load(Ordering::SeqCst), 4);
        fleet.stop().await;
    });
}

#[test]
fn three_members_losing_two_disks_recover_from_the_survivor() {
    run(async {
        // The survivor holds each lost session's acknowledged frame, and the
        // other lost member answers conclusively from its replacement disk.
        // Recovery restores the frame and records no loss.
        let members = ["trio-0", "trio-1", "trio-2"];
        let fleet = Fleet::new(&members, 2, true).await;
        fleet.recover_all().await;
        for node in &members[..2] {
            fleet.assert_sealed(node).await;
            assert!(fleet.loss(node).await.is_none(), "{node}");
            assert_eq!(
                fleet
                    .bucket
                    .list(&format!("cells/cell-{node}/ltx/e1/"))
                    .await
                    .unwrap()
                    .len(),
                1,
                "{node}"
            );
        }
        fleet.stop().await;
    });
}

#[test]
fn three_members_losing_two_disks_without_a_survivor_copy_record_the_loss() {
    run(async {
        // The survivor answers but never held the frame (it joined after the
        // last append), so no complete copy exists anywhere. Every member is
        // conclusive, and each lost session records its bounded loss.
        let members = ["trio-0", "trio-1", "trio-2"];
        let fleet = Fleet::new(&members, 2, false).await;
        fleet.recover_all().await;
        for node in &members[..2] {
            fleet.assert_sealed(node).await;
            assert!(fleet.loss(node).await.is_some(), "{node}");
        }
        fleet.stop().await;
    });
}
