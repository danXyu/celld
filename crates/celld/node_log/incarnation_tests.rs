// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;

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

const LEADER: &str = "leader/g1";

fn seal(member: Option<&str>, incarnation: Option<&str>) -> SealReq {
    SealReq {
        leader: LEADER.into(),
        epoch: 1,
        member: member.map(str::to_string),
        incarnation: incarnation.map(str::to_string),
    }
}

fn tail(member: Option<&str>, incarnation: Option<&str>) -> TailReq {
    TailReq {
        leader: LEADER.into(),
        member: member.map(str::to_string),
        incarnation: incarnation.map(str::to_string),
    }
}

#[test]
fn incarnation_survives_a_restart_and_is_new_on_an_empty_disk() {
    run(async {
        let disk = tempfile::tempdir().unwrap();
        let first = FollowerStore::new(disk.path(), None, "member")
            .incarnation()
            .unwrap();
        assert!(valid_incarnation(&first));
        // A later process on the same disk reads the same identity.
        let again = FollowerStore::new(disk.path(), None, "member")
            .incarnation()
            .unwrap();
        assert_eq!(first, again);
        // The same name on an empty disk is another disk.
        let empty = tempfile::tempdir().unwrap();
        let replaced = FollowerStore::new(empty.path(), None, "member")
            .incarnation()
            .unwrap();
        assert_ne!(first, replaced);
        // The identity is durable before it is returned: no temporary file is
        // left behind, and the peerlog listing still holds no fragment.
        let store = FollowerStore::new(disk.path(), None, "member");
        assert!(store.followed_sessions().is_empty());
        assert_eq!(
            store
                .filesystem
                .read_dir(&disk.path().join("peerlog"))
                .unwrap()
                .into_iter()
                .map(|entry| entry.file_name.to_string_lossy().into_owned())
                .collect::<Vec<_>>(),
            [INCARNATION_FILE]
        );
    });
}

#[test]
fn an_unreadable_incarnation_is_an_error_not_a_new_identity() {
    run(async {
        let disk = tempfile::tempdir().unwrap();
        let store = FollowerStore::new(disk.path(), None, "member");
        store.incarnation().unwrap();
        store
            .filesystem
            .write(&disk.path().join("peerlog").join(INCARNATION_FILE), b"torn")
            .unwrap();
        assert!(FollowerStore::new(disk.path(), None, "member")
            .incarnation()
            .is_err());
    });
}

#[test]
fn a_seal_for_another_member_is_refused_before_the_seal_mark() {
    run(async {
        let disk = tempfile::tempdir().unwrap();
        let store = FollowerStore::new(disk.path(), None, "member");
        store
            .persist(
                LEADER,
                FollowerState {
                    fragment_epoch: 1,
                    ..FollowerState::default()
                },
            )
            .unwrap();
        let own = store.incarnation().unwrap();
        let other = "0123456789abcdef0123456789abcdef";
        // Another member's name, with or without an incarnation, and an
        // unknown incarnation that claims no member name.
        for refused in [
            seal(Some("someone-else"), None),
            seal(Some("someone-else"), Some(&own)),
            seal(None, Some(other)),
        ] {
            assert!(store.seal(&refused).await.is_err());
            assert_eq!(store.load(LEADER).unwrap().sealed_to, 0);
        }
        for refused in [
            tail(Some("someone-else"), Some(&own)),
            tail(Some("someone-else"), None),
            tail(None, Some(other)),
        ] {
            assert!(store.checked_tail(&refused).await.is_err());
        }
        // The addressed disk answers, and so does an older caller that
        // names nobody.
        let entries = store.tail(&tail(None, None)).await.unwrap().entries.len();
        assert_eq!(
            store
                .checked_tail(&tail(Some("member"), Some(&own)))
                .await
                .unwrap()
                .entries
                .len(),
            entries
        );
        assert_eq!(
            store
                .checked_tail(&tail(None, None))
                .await
                .unwrap()
                .entries
                .len(),
            entries
        );
        store.seal(&seal(Some("member"), Some(&own))).await.unwrap();
        assert_eq!(store.load(LEADER).unwrap().sealed_to, 1);
        store.seal(&seal(None, None)).await.unwrap();
    });
}

#[test]
fn the_member_on_a_replacement_disk_answers_conclusively_from_it() {
    run(async {
        // The member's record still names the disk it lost. The same name
        // on the replacement disk answers from its own (empty) store: the
        // named disk is gone, and "no fragment" is the verdict recovery
        // needs to record the loss.
        let lost = tempfile::tempdir().unwrap();
        let superseded = FollowerStore::new(lost.path(), None, "member")
            .incarnation()
            .unwrap();
        let fresh = tempfile::tempdir().unwrap();
        let store = FollowerStore::new(fresh.path(), None, "member");
        assert_ne!(store.incarnation().unwrap(), superseded);
        assert!(store
            .checked_tail(&tail(Some("member"), Some(&superseded)))
            .await
            .unwrap()
            .entries
            .is_empty());
        let sealed = store
            .seal(&seal(Some("member"), Some(&superseded)))
            .await
            .unwrap();
        assert_eq!(sealed.end, 0);
        assert_eq!(sealed.fragment_epoch, 0);
        assert_eq!(sealed.held_fragment_epoch, Some(0));
        // The seal mark is durable like any other: the dead leader's epoch
        // is refused on the replacement disk from here on.
        assert_eq!(store.load(LEADER).unwrap().sealed_to, 1);
        // Answering never rewrote this disk's own identity.
        assert_ne!(
            FollowerStore::new(fresh.path(), None, "member")
                .incarnation()
                .unwrap(),
            superseded
        );
    });
}

#[test]
fn requests_and_records_without_the_binding_keep_their_old_shape() {
    // An older recovery caller sends neither field.
    let old_seal: SealReq = serde_json::from_str(r#"{"leader":"leader/g1","epoch":3}"#).unwrap();
    assert_eq!(old_seal.member, None);
    assert_eq!(old_seal.incarnation, None);
    let old_tail: TailReq = serde_json::from_str(r#"{"leader":"leader/g1"}"#).unwrap();
    assert_eq!(old_tail.member, None);
    assert_eq!(old_tail.incarnation, None);
    // An unbound request serializes exactly as it did before the fields.
    assert_eq!(
        serde_json::to_value(seal(None, None)).unwrap(),
        serde_json::json!({"leader": LEADER, "epoch": 1})
    );
    assert_eq!(
        serde_json::to_value(tail(None, None)).unwrap(),
        serde_json::json!({"leader": LEADER})
    );
    // A bound request reaches an older follower as fields it ignores.
    let bound = serde_json::to_value(seal(Some("member"), Some("ab"))).unwrap();
    assert_eq!(bound["member"], "member");
    assert_eq!(bound["incarnation"], "ab");

    // A lease from an older node has no incarnation, and one written by
    // this fork keeps it through recovery's full-record rewrite.
    let old: crate::ownership_store::NodeLeaseWire =
        serde_json::from_str(r#"{"node":"member","expires_ms":1}"#).unwrap();
    assert_eq!(old.disk_incarnation, None);
    assert!(serde_json::to_value(&old)
        .unwrap()
        .get("disk_incarnation")
        .is_none());
    let new: crate::ownership_store::NodeLeaseWire = serde_json::from_str(
        r#"{"node":"member","expires_ms":1,"disk_incarnation":"0123456789abcdef"}"#,
    )
    .unwrap();
    let rewritten: crate::ownership_store::NodeLeaseWire =
        serde_json::from_value(serde_json::to_value(&new).unwrap()).unwrap();
    assert_eq!(
        rewritten.disk_incarnation.as_deref(),
        Some("0123456789abcdef")
    );
}
