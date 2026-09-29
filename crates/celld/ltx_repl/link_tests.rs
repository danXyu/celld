// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! What each kind of activation reports as its change-export link.

use super::*;
use crate::replication::{ActivationLink, ActivationMode, ActivationOptions};
use object_store::memory::InMemory;
use std::time::Duration;

const CELL: &str = "Cart:links";

fn options(epoch: u64, fresh: bool, took_over: bool) -> ActivationOptions<'static> {
    ActivationOptions {
        cell: CELL,
        epoch,
        fresh,
        took_over,
        resume_local: false,
        prior: None,
    }
}

/// Commit `rows` inserts to the activation's database and make them durable.
async fn write(repl: &LtxRepl, epoch: u64, path: &Path, rows: u32) {
    let db = rusqlite::Connection::open(path).unwrap();
    db.execute_batch("CREATE TABLE IF NOT EXISTS t (v INTEGER)")
        .unwrap();
    for v in 0..rows {
        db.execute("INSERT INTO t VALUES (?1)", [v]).unwrap();
    }
    drop(db);
    assert!(matches!(
        repl.sync_wait(CELL, epoch, Duration::from_secs(10)).await,
        SyncWait::Durable
    ));
}

#[tokio::test]
async fn every_activation_names_its_predecessor() {
    let dir = tempfile::tempdir().unwrap();
    let repl = LtxRepl::start_with_store_for_test(dir.path(), Arc::new(InMemory::new()));

    // A new cell has nothing before it.
    let first = repl.activate(options(1, true, false)).await.unwrap();
    assert_eq!(
        first.link,
        ActivationLink {
            mode: ActivationMode::Fresh,
            start_txid: 1,
            prev_epoch: None,
            prev_txid: None,
        }
    );
    write(&repl, 1, &first.path, 3).await;
    // An orderly release leaves the previous epoch's local snapshot. Its
    // LTX metadata is gone, so without an exporter to pay for the listing
    // the link names the epoch alone.
    repl.release(CELL, 1).await.unwrap();
    let second = repl.activate(options(2, false, false)).await.unwrap();
    assert_eq!(second.link.mode, ActivationMode::Clone);
    assert_eq!(second.link.prev_epoch, Some(1));
    assert_eq!(second.link.prev_txid, None);
    write(&repl, 2, &second.path, 2).await;
    let covered = repl.contiguous_covered_txid(CELL, 2).await.unwrap();
    assert!(covered > 0);
    assert_eq!(
        repl.previous_epoch_txid(CELL, 2).await,
        Some(covered),
        "the local-snapshot path reads the same position when exporting"
    );

    // Another node takes over: a remote clone of epoch 2's chain.
    repl.close_in_place(CELL, 2).await.unwrap();
    let third = repl.activate(options(3, false, true)).await.unwrap();
    assert_eq!(third.link.mode, ActivationMode::Clone);
    assert_eq!(third.link.prev_epoch, Some(2));
    assert_eq!(third.link.prev_txid, Some(covered));
    write(&repl, 3, &third.path, 2).await;
    let covered = repl.contiguous_covered_txid(CELL, 3).await.unwrap();

    // A paged restore continues the chain's TXIDs after its marker.
    repl.paged_restore.store(true, Ordering::Relaxed);
    repl.paged_fleet.store(true, Ordering::Relaxed);
    repl.paged_min_bytes.store(0, Ordering::Relaxed);
    repl.close_in_place(CELL, 3).await.unwrap();
    let fourth = repl.activate(options(4, false, true)).await.unwrap();
    assert!(fourth.vfs.is_some(), "the restore paged");
    assert_eq!(
        fourth.link,
        ActivationLink {
            mode: ActivationMode::Paged,
            // The marker takes the TXID after the cut.
            start_txid: covered + 2,
            prev_epoch: Some(3),
            prev_txid: Some(covered),
        }
    );
    repl.close_in_place(CELL, 4).await.unwrap();

    // A clean reload reopens the same epoch where it stopped.
    let reload = repl
        .activate(ActivationOptions {
            resume_local: true,
            ..options(4, false, false)
        })
        .await
        .unwrap();
    assert_eq!(
        reload.link,
        ActivationLink {
            mode: ActivationMode::Resume,
            // The paged epoch's marker was its last TXID.
            start_txid: covered + 2,
            prev_epoch: Some(4),
            prev_txid: Some(covered + 1),
        }
    );
    repl.close_in_place(CELL, 4).await.unwrap();
}
