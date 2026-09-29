use super::*;
use crate::bucket::StorageBackend;
use object_store::memory::InMemory;
use std::sync::Arc;

fn bucket(prefix: &str) -> Bucket {
    let store = Arc::new(InMemory::new());
    Bucket::with_stores(
        store.clone(),
        store,
        StorageBackend::S3,
        "test".into(),
        format!("{prefix}/"),
    )
}
fn object(id: &str) -> Object {
    Object {
        class: "Cart".into(),
        id: id.into(),
    }
}
fn database() -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = rusqlite::Connection::open(&path).unwrap();
    db.execute_batch("CREATE TABLE items (id INTEGER PRIMARY KEY, title TEXT); INSERT INTO items VALUES(1,'hello'); CREATE TABLE _cf_KV (scope TEXT,k TEXT,v TEXT); INSERT INTO _cf_KV VALUES('Cart:one','key','value'); CREATE TABLE _cf_ALARM (scope TEXT PRIMARY KEY,at_ms INTEGER); INSERT INTO _cf_ALARM VALUES('Cart:one',42); CREATE TABLE _cf_WAKE (epoch TEXT); INSERT INTO _cf_WAKE VALUES('0000000000000064'); CREATE TABLE _litestream_seq (seq INTEGER); INSERT INTO _litestream_seq VALUES(100);").unwrap();
    drop(db);
    std::fs::read(path).unwrap()
}
async fn source_image(source: &Bucket, id: &str) {
    source
        .put(
            &bootstrap_key(&object(id).scope().unwrap()).replace("/e0/", "/e7/"),
            encode_sqlite(&database()).unwrap(),
        )
        .await
        .unwrap();
}
async fn restored(target: &Bucket, id: &str) -> (tempfile::TempDir, rusqlite::Connection) {
    let config = ObjectStoreConfig {
        path: format!("{}cells/Cart:{id}/ltx/e0", target.prefix),
        ..Default::default()
    };
    let client = ObjectStoreClient::with_store(config, target.store.clone());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    replica::restore(&client, &path, TXID(0)).await.unwrap();
    let db = rusqlite::Connection::open(path).unwrap();
    (dir, db)
}

#[test]
fn multiple_objects_roundtrip_with_clear_and_preserve() {
    crate::asyncrt::test_block_on(async {
        for policy in [Alarms::Clear, Alarms::Preserve] {
            let source = bucket("source");
            let target = bucket("target");
            source
                .put("nodes/production", b"secret lease".to_vec())
                .await
                .unwrap();
            for id in ["one", "two"] {
                source_image(&source, id).await;
            }
            ensure_unopened(&target, "reservation-1").await.unwrap();
            let mut entries = Vec::new();
            for id in ["one", "two"] {
                entries.push(
                    capture(&source, &target, "reservation-1", &object(id), policy)
                        .await
                        .unwrap(),
                );
            }
            let json = serde_json::to_value(&entries).unwrap();
            assert!(json[0]["snapshotID"].is_string());
            assert_eq!(serde_json::from_value::<Vec<Entry>>(json).unwrap(), entries);
            for entry in &entries {
                import(&target, "reservation-1", entry).await.unwrap();
                import(&target, "reservation-1", entry).await.unwrap();
                let (_dir, db) = restored(&target, &entry.object.id).await;
                assert_eq!(
                    db.query_row("SELECT title FROM items", [], |r| r.get::<_, String>(0))
                        .unwrap(),
                    "hello"
                );
                assert_eq!(
                    db.query_row("SELECT v FROM _cf_KV", [], |r| r.get::<_, String>(0))
                        .unwrap(),
                    "value"
                );
                assert_eq!(
                    db.query_row("SELECT count(*) FROM _cf_ALARM", [], |r| r.get::<_, u32>(0))
                        .unwrap(),
                    u32::from(policy == Alarms::Preserve)
                );
                assert_eq!(db.query_row("SELECT count(*) FROM sqlite_schema WHERE name IN ('_cf_WAKE','_litestream_seq')",[],|r|r.get::<_,u32>(0)).unwrap(),0);
            }
            assert!(target.get("nodes/production").await.unwrap().is_none());
            assert!(ensure_unopened(&target, "reservation-1").await.is_err());
        }
    });
}
#[test]
fn import_rejects_tampering_and_different_operation() {
    crate::asyncrt::test_block_on(async {
        let source = bucket("source");
        let target = bucket("target");
        source_image(&source, "one").await;
        let entry = capture(
            &source,
            &target,
            "reservation-1",
            &object("one"),
            Alarms::Clear,
        )
        .await
        .unwrap();
        assert!(capture(
            &source,
            &target,
            "reservation-1",
            &object("one"),
            Alarms::Clear
        )
        .await
        .is_err());
        assert!(import(&target, "reservation-2", &entry).await.is_err());
        target
            .put(&entry.snapshot_id, b"corrupt".to_vec())
            .await
            .unwrap();
        assert!(import(&target, "reservation-1", &entry).await.is_err());
        assert!(target
            .get(&bootstrap_key("Cart:one"))
            .await
            .unwrap()
            .is_none());
    });
}
#[test]
fn empty_source_and_used_destination_fail_closed() {
    crate::asyncrt::test_block_on(async {
        let source = bucket("source");
        let target = bucket("target");
        assert!(capture(
            &source,
            &target,
            "reservation-1",
            &object("missing"),
            Alarms::Clear
        )
        .await
        .is_err());
        target
            .put("current.json", b"deployed".to_vec())
            .await
            .unwrap();
        assert!(ensure_unopened(&target, "reservation-1").await.is_err());
    });
}
#[test]
fn identities_and_duplicates() {
    assert!(validate_selection(&[]).is_err());
    assert!(validate_selection(&[object("same"), object("same")]).is_err());
    assert!(object("../../production").scope().is_err());
    assert!(object(&"x".repeat(256)).scope().is_err());
    assert!(validate_selection(&[object("one"), object("two")]).is_ok());
}

#[test]
fn first_ownership_restores_seed_before_serving() {
    crate::asyncrt::test_block_on(async {
        let store = Arc::new(InMemory::new());
        let target = Bucket::with_stores(
            store.clone(),
            store.clone(),
            StorageBackend::S3,
            "test".into(),
            String::new(),
        );
        let source = bucket("source");
        source_image(&source, "one").await;
        let entry = capture(
            &source,
            &target,
            "reservation-1",
            &object("one"),
            Alarms::Clear,
        )
        .await
        .unwrap();
        import(&target, "reservation-1", &entry).await.unwrap();
        let dir = tempfile::tempdir().unwrap();
        let repl = crate::ltx_repl::LtxRepl::start_with_store_for_test(dir.path(), store);
        let activated = repl
            .activate(crate::replication::ActivationOptions {
                cell: "Cart:one",
                epoch: 1,
                fresh: true,
                took_over: false,
                resume_local: false,
                prior: None,
            })
            .await
            .unwrap();
        assert!(activated.restored);
        let db = rusqlite::Connection::open(&activated.path).unwrap();
        assert_eq!(
            db.query_row("SELECT title FROM items", [], |r| r.get::<_, String>(0))
                .unwrap(),
            "hello"
        );
        drop(db);
        repl.close_in_place("Cart:one", 1).await.unwrap();
    });
}
