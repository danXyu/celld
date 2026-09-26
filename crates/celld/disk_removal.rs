// Copyright 2026 Deno Land Inc. Apache-2.0 license.
//! Adapter for the process-local disk-removal shutdown contract.
use crate::{
    bucket::Bucket,
    node_log::{FollowerStore, NodeLogManager},
    ownership_store::NodeLeaseWire,
};
use anyhow::{ensure, Context};
use celld_logic::disk_removal::{follower_covered, Control, Coverage, Phase};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

pub struct State {
    pub control: Mutex<Control>,
    pub control_only: AtomicBool,
    pub supported: bool,
}
impl State {
    pub fn new(generation: String, supported: bool) -> Self {
        Self {
            control: Mutex::new(Control::new(generation)),
            control_only: AtomicBool::new(false),
            supported,
        }
    }
    pub fn snapshot(&self) -> serde_json::Value {
        let state = self.control.lock().unwrap();
        serde_json::json!({
            "schema_version": 1,
            "runtime_generation": state.generation,
            "capabilities": {"strict_disk_removal": self.supported},
            "control_only": self.control_only.load(Ordering::SeqCst),
            "operation": state.operation.as_ref().map(|op| serde_json::json!({
                "operation_id": op.id, "expected_generation": state.generation,
                "mode": "remove-disk",
                "phase": match op.phase { Phase::Draining => "draining", Phase::DataSafe => "data_safe", Phase::Failed => "failed" },
                "blocker": op.blocker,
            })),
        })
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Obligation {
    pub session: String,
    pub epoch: u64,
}

async fn nodes(bucket: &Bucket) -> anyhow::Result<Vec<NodeLeaseWire>> {
    let mut nodes = Vec::new();
    for meta in bucket.list("nodes/").await? {
        let key = meta.location.as_ref();
        let (bytes, _) = bucket
            .get(key)
            .await?
            .context("node disappeared during inventory")?;
        let node: NodeLeaseWire = serde_json::from_slice(&bytes)?;
        ensure!(
            key == format!("nodes/{}.json", node.node),
            "node record identity mismatch"
        );
        nodes.push(node);
    }
    Ok(nodes)
}
/// Freeze joins every append admitted before the cut, including its fsync.
/// Inventory both disk fragments and ensembles (an empty follower is still a witness).
pub async fn capture(
    bucket: &Bucket,
    node: &str,
    follower: &FollowerStore,
) -> anyhow::Result<Vec<Obligation>> {
    follower.freeze_for_disk_removal().await;
    let mut obligations = follower.disk_removal_obligations()?;
    for record in nodes(bucket).await? {
        if let Some(log) = record.log {
            if log.ensemble.iter().any(|member| member == node) {
                obligations.push(Obligation {
                    session: format!("{}/{}", record.node, record.generation),
                    epoch: log.epoch,
                });
            }
        }
    }
    obligations.sort();
    obligations.dedup();
    Ok(obligations)
}

/// A successful read, never record absence, proves an obligation. A successor
/// incarnation is installed only after predecessor recovery. Inspect loss markers
/// only for this session: unrelated historical damage is not this disk's duty.
async fn covered(
    bucket: &Bucket,
    obligation: &Obligation,
    manager: Option<&NodeLogManager>,
) -> anyhow::Result<bool> {
    let (node, generation) = obligation
        .session
        .split_once('/')
        .context("unversioned obligation")?;
    let (bytes, _) = bucket
        .get(&format!("nodes/{node}.json"))
        .await?
        .context("missing obligation record")?;
    let record: NodeLeaseWire = serde_json::from_slice(&bytes)?;
    ensure!(record.node == node, "obligation identity mismatch");
    let complete = if record.generation != generation {
        follower_covered(obligation.epoch, Coverage::RecoveredSuccessor)
    } else {
        let log = record.log.context("missing obligation log")?;
        ensure!(
            matches!(log.state.as_str(), "open" | "recovering" | "sealed"),
            "unknown obligation log state"
        );
        let complete = follower_covered(
            obligation.epoch,
            Coverage::Current {
                epoch: log.epoch,
                sealed: log.state == "sealed",
                bucket_complete: log.bucket_complete,
            },
        );
        if !complete && record.expires_ms <= crate::ownership_store::now_ms() {
            if let Some(manager) = manager {
                manager.recover(&obligation.session).await?;
            }
        }
        complete
    };
    if !complete {
        return Ok(false);
    }
    let loss = bucket.list(&format!("log/{node}/")).await?.iter().any(|m| {
        m.location
            .as_ref()
            .starts_with(&format!("log/{}.", obligation.session))
            && m.location.as_ref().ends_with(".loss.json")
    });
    ensure!(!loss, "recovery reported loss for {}", obligation.session);
    Ok(true)
}

/// The caller must first close application admission, drain application work,
/// join local durability tasks and stop/join the actor. This method may then
/// perform ordinary recovery while the frozen follower remains readable.
pub async fn prove(
    bucket: Bucket,
    own_session: String,
    obligations: Vec<Obligation>,
    manager: Option<Arc<NodeLogManager>>,
    state: Arc<State>,
) -> anyhow::Result<()> {
    loop {
        let (node, generation) = own_session.split_once('/').context("invalid own session")?;
        let (bytes, _) = bucket
            .get(&format!("nodes/{node}.json"))
            .await?
            .context("missing own record")?;
        let own: NodeLeaseWire = serde_json::from_slice(&bytes)?;
        ensure!(
            own.node == node && own.generation == generation,
            "own runtime generation changed"
        );
        let own_covered = match own.log {
            None => true, // no fleet log was ever opened in this incarnation
            Some(log) => {
                covered(
                    &bucket,
                    &Obligation {
                        session: own_session.clone(),
                        epoch: log.epoch,
                    },
                    manager.as_deref(),
                )
                .await?
            }
        };
        let mut followers = Vec::new();
        for obligation in &obligations {
            followers.push(covered(&bucket, obligation, manager.as_deref()).await?);
        }
        if celld_logic::disk_removal::may_complete(true, own_covered, followers.iter().copied()) {
            return Ok(());
        }
        state.control.lock().unwrap().progress(format!(
            "own_covered={own_covered}, follower_obligations_pending={}",
            followers.iter().filter(|v| !**v).count()
        ));
        crate::asyncrt::sleep(std::time::Duration::from_millis(100)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node_log::{AppendReq, Entry, TailReq};
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
    async fn node(
        bucket: &Bucket,
        name: &str,
        generation: &str,
        epoch: u64,
        state: &str,
        complete: bool,
        ensemble: &[&str],
    ) {
        bucket.put(&format!("nodes/{name}.json"), serde_json::to_vec(&serde_json::json!({
            "node": name, "expires_ms": u64::MAX, "ownership_index_generation": generation,
            "log": {"epoch":epoch,"state":state,"tiered":0,"ensemble":ensemble,"active":true,"bucket_complete":complete}
        })).unwrap()).await.unwrap();
    }
    #[test]
    fn other_leaders_and_missing_evidence_block_even_when_own_log_is_sealed() {
        run(async {
            let dir = tempfile::tempdir().unwrap();
            let bucket = Bucket::open_dev(&dir.path().join("bucket.sqlite")).unwrap();
            node(&bucket, "donor", "g1", 1, "sealed", false, &[]).await;
            node(&bucket, "leader", "g2", 4, "open", false, &["donor"]).await;
            let follower = FollowerStore::new(dir.path(), Some(Arc::new(bucket.clone())), "donor");
            // The disk incarnation lives beside the fragments and is not one.
            follower.incarnation().unwrap();
            let obligations = capture(&bucket, "donor", &follower).await.unwrap();
            assert_eq!(obligations.len(), 1);
            assert!(!covered(&bucket, &obligations[0], None).await.unwrap());
            let state = Arc::new(State::new("g1".into(), true));
            state.control.lock().unwrap().request("op", "g1").unwrap();
            assert!(crate::asyncrt::timeout(
                std::time::Duration::from_millis(10),
                prove(
                    bucket.clone(),
                    "donor/g1".into(),
                    obligations.clone(),
                    None,
                    state.clone()
                )
            )
            .await
            .is_err());
            assert_eq!(
                state
                    .control
                    .lock()
                    .unwrap()
                    .operation
                    .as_ref()
                    .unwrap()
                    .phase,
                Phase::Draining
            );
            node(&bucket, "leader", "g2", 4, "open", true, &["donor"]).await;
            assert!(covered(&bucket, &obligations[0], None).await.unwrap());
            // Proof survives serialization and recovery's claim transition.
            let (bytes, _) = bucket.get("nodes/leader.json").await.unwrap().unwrap();
            let wire: NodeLeaseWire = serde_json::from_slice(&bytes).unwrap();
            assert!(wire.log.unwrap().bucket_complete);
            bucket.delete("nodes/leader.json").await.unwrap();
            assert!(covered(&bucket, &obligations[0], None).await.is_err());
        });
    }
    #[test]
    fn scoped_recovery_loss_blocks_success_but_unrelated_history_does_not() {
        run(async {
            let dir = tempfile::tempdir().unwrap();
            let bucket = Bucket::open_dev(&dir.path().join("bucket.sqlite")).unwrap();
            node(&bucket, "leader", "g2", 4, "sealed", false, &["donor"]).await;
            let obligation = Obligation {
                session: "leader/g2".into(),
                epoch: 4,
            };
            bucket
                .put("log/unrelated/old.e1.loss.json", b"{}".to_vec())
                .await
                .unwrap();
            assert!(covered(&bucket, &obligation, None).await.unwrap());
            bucket
                .put("log/leader/g2.e4.loss.json", b"{}".to_vec())
                .await
                .unwrap();
            assert!(covered(&bucket, &obligation, None).await.is_err());
        });
    }
    #[test]
    fn append_racing_freeze_is_joined_or_refused_and_tail_remains_recoverable() {
        run(async {
            let dir = tempfile::tempdir().unwrap();
            let bucket = Bucket::open_dev(&dir.path().join("bucket.sqlite")).unwrap();
            node(&bucket, "leader", "g2", 4, "open", false, &["donor"]).await;
            let follower = FollowerStore::new(dir.path(), Some(Arc::new(bucket)), "donor");
            let append = |seq| AppendReq {
                leader: "leader/g2".into(),
                epoch: 4,
                truncate_to: 0,
                entries: vec![Entry {
                    seq,
                    cell: "cell".into(),
                    cell_epoch: 1,
                    txid: seq,
                    bytes: vec![42],
                }],
            };
            let (reply, ()) = tokio::join!(
                follower.append(append(1)),
                follower.freeze_for_disk_removal()
            );
            let tail = follower.tail(&TailReq {
                leader: "leader/g2".into(),
                member: None,
                incarnation: None,
            }).await.unwrap();
            assert!(reply.ok, "the append polled before freeze must finish");
            assert_eq!(tail.entries.len(), 1);
            let refused = follower.append(append(2)).await;
            assert!(!refused.ok);
            assert!(refused.quiesced);
            assert_eq!(
                follower
                    .tail(&TailReq {
                        leader: "leader/g2".into(),
                        member: None,
                        incarnation: None,
                    }).await.unwrap()
                    .entries
                    .len(),
                tail.entries.len()
            );
        });
    }
}
