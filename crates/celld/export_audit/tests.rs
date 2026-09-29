use std::collections::BTreeMap;
use std::sync::Arc;

use celld_export_format::{
    Body, Consumer, Envelope, Gap, Op, Origin, Position, Record, RowChange, RowsBody, SnapshotBody,
    SnapshotEndBody, SnapshotScope, StreamId, TableGen, TableRows, Value, WatermarkBody,
};
use celld_ltx::client::object_store::{ObjectStoreClient, ObjectStoreConfig};
use celld_ltx::client::ReplicaClient;
use celld_ltx::{ltx, TXID};
use object_store::memory::InMemory;
use rusqlite::Connection;

use super::cli::{erase_targets, Selection};
use super::inventory::{BucketHead, Inventory, Landed, Loss, Span};
use super::reconcile::{reconcile, Options, Reconciled};
use super::tombstone::{self, scope_of, Tombstone};
use super::verify::{self, DiffKind, Outcome};
use super::*;
use crate::bucket::StorageBackend;

const HOUR: i64 = 60 * 60 * 1000;
const CELL: &str = "Cart:one";

fn bucket() -> Bucket {
    let store = Arc::new(InMemory::new());
    Bucket::with_stores(
        store.clone(),
        store,
        StorageBackend::S3,
        "test".into(),
        "fleet/".into(),
    )
}

fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}

fn stream(cell: &str, facet: Option<&str>) -> StreamId {
    StreamId {
        script: "app".into(),
        class: cell.split(':').next().unwrap().into(),
        cell: cell.into(),
        facet: facet.map(str::to_string),
        incarnation: 1,
    }
}

fn root() -> StreamId {
    stream(CELL, None)
}

fn at(epoch: u64, txid: u64, commit: u64) -> Position {
    Position::new(epoch, txid, commit)
}

fn envelope(s: &StreamId, p: Position, node: &str, origin: Origin) -> Envelope {
    Envelope {
        stream: s.clone(),
        cell_name: None,
        position: p,
        committed_at: 0,
        node: node.into(),
        origin,
        fragment: 1,
        fragments: 1,
    }
}

fn table(name: &str, rows: &[(i64, &str)]) -> TableRows {
    TableRows {
        table: name.into(),
        generation: 1,
        columns: vec!["id".into(), "v".into()],
        key_columns: vec!["id".into()],
        rows: rows
            .iter()
            .map(|(id, v)| {
                RowChange(
                    Op::Insert,
                    vec![Value::Integer(*id)],
                    vec![Value::Integer(*id), Value::Text((*v).into())],
                )
            })
            .collect(),
    }
}

fn rows(s: &StreamId, p: Position, name: &str, data: &[(i64, &str)]) -> Record {
    Record {
        envelope: envelope(s, p, "node-a", Origin::Live),
        body: Body::Rows(RowsBody {
            data: table(name, data),
        }),
    }
}

/// The first watermark of `through`'s epoch, certifying `records` records
/// in `commits` commits.
fn watermark(s: &StreamId, through: Position, commits: u64, records: u64) -> Record {
    Record {
        envelope: envelope(s, through, "node-a", Origin::Live),
        body: Body::Watermark(WatermarkBody {
            from: None,
            through,
            commits,
            records,
        }),
    }
}

fn snapshot(s: &StreamId, p: Position, name: &str, data: &[(i64, &str)]) -> Vec<Record> {
    vec![
        Record {
            envelope: envelope(s, p, "repair", Origin::Repair),
            body: Body::Snapshot(SnapshotBody {
                snapshot_id: "s1".into(),
                data: table(name, data),
            }),
        },
        Record {
            envelope: envelope(s, p, "repair", Origin::Repair),
            body: Body::SnapshotEnd(SnapshotEndBody {
                snapshot_id: "s1".into(),
                scope: SnapshotScope::Stream,
                tables: vec![TableGen {
                    table: name.into(),
                    generation: 1,
                }],
                records: 1,
            }),
        },
    ]
}

fn summary(id: StreamId, certified: &[(u64, u64)]) -> StreamSummary {
    StreamSummary {
        certified: certified.iter().map(|(e, t)| (*e, at(*e, *t, 1))).collect(),
        nodes: certified
            .iter()
            .map(|(e, _)| (*e, ["node-a".to_string()].into()))
            .collect(),
        ..StreamSummary::new(id)
    }
}

fn head(scope: &str, spans: &[(u64, u64, u64)]) -> BucketHead {
    let last = spans.last().unwrap();
    BucketHead {
        scope: scope.into(),
        epoch: last.0,
        txid: last.2,
        spans: spans
            .iter()
            .map(|(epoch, lo, hi)| Span {
                epoch: *epoch,
                lo: *lo,
                hi: *hi,
            })
            .collect(),
        modified_ms: 0,
        landed: spans
            .iter()
            .map(|(epoch, lo, hi)| Landed {
                epoch: *epoch,
                min: *lo,
                max: *hi,
                ms: 0,
            })
            .collect(),
    }
}

fn heads(list: Vec<BucketHead>) -> BTreeMap<String, BucketHead> {
    list.into_iter().map(|h| (h.scope.clone(), h)).collect()
}

fn run(
    heads: &BTreeMap<String, BucketHead>,
    losses: &[Loss],
    streams: &[StreamSummary],
    tombstones: &[Tombstone],
) -> Reconciled {
    reconcile(
        heads,
        &BTreeMap::new(),
        losses,
        streams,
        &[],
        tombstones,
        |class| class != "__Queue",
        Options {
            now_ms: 10 * HOUR,
            settle_ms: HOUR,
        },
    )
}

fn kinds(r: &Reconciled) -> Vec<(FindingKind, String)> {
    r.findings
        .iter()
        .map(|f| (f.kind, f.scope.clone()))
        .collect()
}

// ── inventory ────────────────────────────────────────────────────────────────

fn ltx_key(scope: &str, epoch: u64, level: u32, min: u64, max: u64) -> String {
    format!(
        "cells/{scope}/ltx/e{epoch}/ltx/{level}/{}",
        ltx::format_filename(TXID(min), TXID(max))
    )
}

#[test]
fn the_head_follows_the_chain_across_a_paged_epoch_and_skips_a_fenced_one() {
    let mut inventory = Inventory::new();
    // Epoch 3 opens with a snapshot; epoch 5 continues it from txid 7. Epoch
    // 4 is a fenced owner's late snapshot that ends elsewhere, and a fold
    // that landed in epoch 3 after the cut is clipped.
    for (min, max) in [(1, 1), (2, 5), (6, 6), (7, 8)] {
        inventory.add(&ltx_key(CELL, 3, 0, min, max), 10);
    }
    inventory.add(&ltx_key(CELL, 4, 9, 1, 4), 20);
    for (min, max) in [(7, 7), (8, 9)] {
        inventory.add(&ltx_key(CELL, 5, 0, min, max), 30);
    }
    inventory.add("cells/Cart:one/meta.json", 99);
    inventory.add("log/node-a/g1.e7.loss.json", 5);
    inventory.add("log/node-a/g1.bundle-x.loss.json", 5);
    let head = block_on(inventory.head(CELL)).unwrap();
    assert_eq!((head.epoch, head.txid), (5, 9));
    assert_eq!(
        head.spans,
        vec![
            Span {
                epoch: 3,
                lo: 1,
                hi: 6
            },
            Span {
                epoch: 5,
                lo: 7,
                hi: 9
            },
        ]
    );
    // Only LTX objects count; the ignored meta.json does not.
    assert_eq!(head.modified_ms, 30);
    assert_eq!(
        inventory.losses(),
        &[Loss {
            session: "node-a/g1".into(),
            epoch: 7,
            modified_ms: 5,
        }]
    );
    assert_eq!(inventory.losses()[0].node(), "node-a");
}

#[test]
fn the_head_is_the_newest_restorable_cut_not_the_largest_name() {
    let mut inventory = Inventory::new();
    // 5..6 cannot apply after 1..3: the head is 3.
    inventory.add(&ltx_key(CELL, 1, 0, 1, 3), 1);
    inventory.add(&ltx_key(CELL, 1, 0, 5, 6), 1);
    let facet = format!("{CELL}/facets/{}", "a".repeat(32));
    inventory.add(&ltx_key(&facet, 2, 0, 1, 2), 1);
    let heads = block_on(inventory.heads());
    assert_eq!(heads[CELL].txid, 3);
    assert!(heads[&facet].is_facet());
    assert_eq!(heads[&facet].root(), CELL);
}

#[test]
fn a_scope_with_no_snapshot_to_start_from_has_no_head() {
    let mut inventory = Inventory::new();
    inventory.add(&ltx_key(CELL, 2, 0, 4, 6), 1);
    assert!(block_on(inventory.head(CELL)).is_none());
}

// ── reconcile ────────────────────────────────────────────────────────────────

#[test]
fn a_consumer_certified_to_the_head_has_nothing_to_find() {
    let r = run(
        &heads(vec![head(CELL, &[(3, 1, 6), (5, 7, 9)])]),
        &[],
        &[summary(root(), &[(3, 6), (5, 9)])],
        &[],
    );
    assert!(r.findings.is_empty(), "{:?}", r.findings);
    assert_eq!(r.checked, 1);
}

#[test]
fn a_consumer_behind_the_head_is_a_gap_once_it_settles() {
    let h = heads(vec![head(CELL, &[(3, 1, 6), (5, 7, 9)])]);
    let streams = [summary(root(), &[(3, 6), (5, 8)])];
    let r = run(&h, &[], &streams, &[]);
    assert_eq!(kinds(&r), vec![(FindingKind::Gap, CELL.to_string())]);
    let f = &r.findings[0];
    assert_eq!(f.epochs, vec![5]);
    assert_eq!(f.from, Some(at(5, 8, 1)));
    assert_eq!(f.head, Some(at(5, 9, u64::MAX)));
    assert!(matches!(&r.records[0].body, Body::Gap(g) if g.to == at(5, 9, u64::MAX)));
    assert_eq!(r.records[0].envelope.origin, Origin::Repair);

    // A change that landed a minute ago may still be on its way to the
    // consumer.
    let mut fresh = h.clone();
    for l in &mut fresh.get_mut(CELL).unwrap().landed {
        l.ms = 10 * HOUR - 60_000;
    }
    let r = run(&fresh, &[], &streams, &[]);
    assert!(r.findings.is_empty());
    assert_eq!(r.unsettled, 1);
}

#[test]
fn a_closed_epoch_the_consumer_never_finished_is_a_gap_too() {
    let r = run(
        &heads(vec![head(CELL, &[(3, 1, 6), (5, 7, 9)])]),
        &[],
        &[summary(root(), &[(3, 4), (5, 9)])],
        &[],
    );
    assert_eq!(r.findings.len(), 1);
    assert_eq!(r.findings[0].epochs, vec![3]);
}

#[test]
fn a_stream_wide_snapshot_at_the_head_covers_every_gap() {
    let mut s = summary(root(), &[]);
    s.snapshot_at = Some(at(5, 9, 0));
    let r = run(
        &heads(vec![head(CELL, &[(3, 1, 6), (5, 7, 9)])]),
        &[],
        &[s],
        &[],
    );
    assert!(r.findings.is_empty());
}

#[test]
fn certified_past_a_closed_epoch_is_lost() {
    let r = run(
        &heads(vec![head(CELL, &[(3, 1, 6), (5, 7, 9)])]),
        &[],
        &[summary(root(), &[(3, 8), (5, 9)])],
        &[],
    );
    assert_eq!(kinds(&r), vec![(FindingKind::Lost, CELL.to_string())]);
    assert_eq!(r.findings[0].from, Some(at(3, 6, u64::MAX)));
    assert_eq!(r.findings[0].certified, Some(at(3, 8, 1)));
}

#[test]
fn certified_in_an_epoch_the_chain_skipped_is_lost() {
    let r = run(
        &heads(vec![head(CELL, &[(3, 1, 6), (5, 7, 9)])]),
        &[],
        &[summary(root(), &[(3, 6), (4, 3), (5, 9)])],
        &[],
    );
    assert_eq!(kinds(&r), vec![(FindingKind::Lost, CELL.to_string())]);
    assert_eq!(r.findings[0].epochs, vec![4]);
}

#[test]
fn past_the_newest_head_is_lost_only_after_its_node_declared_a_loss() {
    let h = heads(vec![head(CELL, &[(5, 1, 9)])]);
    let streams = [summary(root(), &[(5, 12)])];
    // The bucket lags the fleet: not a finding by itself.
    assert!(run(&h, &[], &streams, &[]).findings.is_empty());
    let other = Loss {
        session: "node-b/g1".into(),
        epoch: 1,
        modified_ms: 0,
    };
    assert!(run(&h, &[other], &streams, &[]).findings.is_empty());
    let ours = Loss {
        session: "node-a/g1".into(),
        epoch: 1,
        modified_ms: 0,
    };
    let r = run(&h, &[ours], &streams, &[]);
    assert_eq!(kinds(&r), vec![(FindingKind::Lost, CELL.to_string())]);
    // The gap reaches past what the consumer holds, so only a snapshot past
    // the lost changes clears it.
    assert!(matches!(&r.records[0].body, Body::Gap(g) if g.to == at(5, 12, 1)));
}

#[test]
fn an_unknown_cell_is_found_and_skipped_classes_and_tombstones_are_not() {
    let facet = format!("{CELL}/facets/{}", "b".repeat(32));
    let h = heads(vec![
        head(CELL, &[(1, 1, 3)]),
        head(&facet, &[(2, 1, 2)]),
        head("__Queue:q", &[(1, 1, 3)]),
        head("Cart:erased", &[(1, 1, 3)]),
    ]);
    let erased = Tombstone {
        script: "app".into(),
        class: "Cart".into(),
        cell: "Cart:erased".into(),
        facet: None,
        incarnation: None,
        erased_at_ms: 0,
        reason: None,
        cleared_at_ms: None,
    };
    let r = run(&h, &[], &[], &[erased]);
    assert_eq!(
        kinds(&r),
        vec![
            (FindingKind::UnknownStream, CELL.to_string()),
            (FindingKind::UnknownStream, facet.clone()),
        ]
    );
    assert_eq!(r.findings[1].stream.cell, CELL);
    assert_eq!(
        r.findings[1].stream.facet.as_deref(),
        Some(&facet[CELL.len() + 1..])
    );
    assert!(r.records.is_empty());
    assert_eq!((r.not_exported, r.tombstoned), (1, 1));
}

#[test]
fn the_stream_is_the_newest_incarnation_at_or_below_the_head() {
    let mut old = summary(root(), &[(1, 3)]);
    old.id.incarnation = 1;
    let mut new = summary(root(), &[(4, 2)]);
    new.id.incarnation = 4;
    let r = run(
        &heads(vec![head(CELL, &[(4, 1, 2)])]),
        &[],
        &[old, new],
        &[],
    );
    assert!(r.findings.is_empty(), "{:?}", r.findings);
}

#[test]
fn a_facet_missing_from_the_bucket_gets_its_deleted_record() {
    let facet = stream(CELL, Some("child"));
    let r = run(
        &heads(vec![head(CELL, &[(1, 1, 3)])]),
        &[],
        &[
            summary(root(), &[(1, 3)]),
            summary(facet.clone(), &[(1, 2)]),
        ],
        &[],
    );
    assert_eq!(
        kinds(&r),
        vec![(FindingKind::MissingDeleted, scope_of(CELL, Some("child")))]
    );
    let deleted = &r.records[0];
    assert_eq!(deleted.envelope.stream, root());
    assert_eq!(deleted.envelope.position, at(1, 3, 1));

    // The consumer applies it: the facet stream is gone.
    let mut consumer = Consumer::new();
    consumer
        .ingest_all([
            rows(&root(), at(1, 1, 1), "t", &[(1, "a")]),
            rows(&facet, at(1, 1, 1), "t", &[(1, "f")]),
            deleted.clone(),
        ])
        .unwrap();
    assert!(consumer.stream(&facet).is_none());
    assert!(consumer.stream(&root()).is_some());
}

#[test]
fn a_gap_record_shows_as_a_gap_leaves_certification_alone_and_repair_clears_it() {
    let s = root();
    let live = vec![
        rows(&s, at(5, 8, 1), "t", &[(1, "a")]),
        watermark(&s, at(5, 8, 1), 1, 1),
    ];
    let mut consumer = Consumer::new();
    consumer.ingest_all(live.clone()).unwrap();
    let state = consumer.stream(&s).unwrap();
    let mut sum = summary(s.clone(), &[]);
    sum.certified = state.certified.clone();
    let r = run(&heads(vec![head(CELL, &[(5, 1, 9)])]), &[], &[sum], &[]);
    assert_eq!(r.records.len(), 1);

    let mut consumer = Consumer::new();
    consumer.ingest_all(live.clone()).unwrap();
    consumer.ingest_all(r.records.clone()).unwrap();
    // Twice: a later run writes the same record, and it is a duplicate.
    consumer.ingest_all(r.records.clone()).unwrap();
    let state = consumer.stream(&s).unwrap();
    assert_eq!(state.certified[&5], at(5, 8, 1));
    assert!(matches!(state.gaps.as_slice(), [Gap::Reported { .. }]));

    consumer
        .ingest_all(snapshot(&s, at(5, 9, u64::MAX), "t", &[(1, "b")]))
        .unwrap();
    assert!(consumer.stream(&s).unwrap().gaps.is_empty());
}

#[test]
fn short_recoveries_are_reported() {
    let recovered = [
        RecoveredSession {
            session: "node-a/g1".into(),
            expected: 3,
            held: 2,
            loss: false,
        },
        RecoveredSession {
            session: "node-b/g1".into(),
            expected: 1,
            held: 1,
            loss: false,
        },
    ];
    let r = reconcile(
        &BTreeMap::new(),
        &BTreeMap::new(),
        &[],
        &[],
        &recovered,
        &[],
        |_| true,
        Options {
            now_ms: 0,
            settle_ms: 0,
        },
    );
    assert_eq!(r.recovered_short, vec![recovered[0].clone()]);
}

#[test]
fn a_cell_that_keeps_writing_cannot_defer_an_old_gap() {
    // Epoch 1 closed at 5 hours ago with the consumer certified only
    // through 3; epoch 2 is fully certified and was written a minute ago.
    let mut h = head(CELL, &[(1, 1, 5), (2, 6, 9)]);
    h.modified_ms = 10 * HOUR - 60_000;
    h.landed[1].ms = 10 * HOUR - 60_000;
    let r = run(
        &heads(vec![h.clone()]),
        &[],
        &[summary(root(), &[(1, 3), (2, 9)])],
        &[],
    );
    assert_eq!(kinds(&r), vec![(FindingKind::Gap, CELL.to_string())]);
    assert_eq!(r.findings[0].epochs, vec![1]);

    // A lag that only concerns the change that just landed is not yet one.
    let r = run(
        &heads(vec![h]),
        &[],
        &[summary(root(), &[(1, 5), (2, 8)])],
        &[],
    );
    assert!(r.findings.is_empty(), "{:?}", r.findings);
    assert_eq!(r.unsettled, 1);
}

#[test]
fn a_facet_whose_objects_do_not_restore_is_not_read_as_deleted() {
    let facet = stream(CELL, Some("child"));
    let facet_scope = scope_of(CELL, Some("child"));
    let mut inventory = Inventory::new();
    inventory.add(&ltx_key(CELL, 1, 0, 1, 3), 0);
    // The facet's object exists, but without the snapshot to start from.
    inventory.add(&ltx_key(&facet_scope, 2, 0, 2, 2), 0);
    assert!(inventory.contains(&facet_scope));
    let heads = block_on(inventory.heads());
    assert!(!heads.contains_key(&facet_scope));
    let broken = inventory.broken(&heads);
    assert_eq!(broken.keys().collect::<Vec<_>>(), vec![&facet_scope]);

    let r = reconcile(
        &heads,
        &broken,
        &[],
        &[
            summary(root(), &[(1, 3)]),
            summary(facet.clone(), &[(2, 2)]),
        ],
        &[],
        &[],
        |_| true,
        Options {
            now_ms: 10 * HOUR,
            settle_ms: HOUR,
        },
    );
    assert_eq!(kinds(&r), vec![(FindingKind::Unrestorable, facet_scope)]);
    assert_eq!(r.findings[0].stream, facet);
    assert!(r.records.is_empty(), "{:?}", r.records);
}

// ── the bucket consumer, end to end ─────────────────────────────────────────

#[test]
fn reconcile_over_the_bucket_writes_records_the_next_run_reads_back() {
    block_on(async {
        let bucket = bucket();
        let s = root();
        emit(
            &bucket,
            vec![
                rows(&s, at(1, 2, 1), "t", &[(1, "a")]),
                watermark(&s, at(1, 2, 1), 1, 1),
                Record {
                    envelope: envelope(
                        &stream(CELL, None),
                        at(1, 3, u64::MAX),
                        "node-b",
                        Origin::Live,
                    ),
                    body: Body::Recovered(celld_export_format::RecoveredBody {
                        session: "node-a/g1".into(),
                        head: at(1, 3, u64::MAX),
                        loss: false,
                        cells: 2,
                    }),
                },
            ],
        )
        .await
        .unwrap();
        let mut inventory = Inventory::new();
        inventory.add(&ltx_key(CELL, 1, 0, 1, 3), 0);
        let heads = inventory.heads().await;

        let consumer = BucketConsumer::load(bucket.clone()).await.unwrap();
        let streams = consumer.streams().await.unwrap();
        assert_eq!(streams.len(), 1);
        assert_eq!(streams[0].certified[&1], at(1, 2, 1));
        assert_eq!(streams[0].nodes[&1], ["node-a".to_string()].into());
        let recovered = consumer.recovered().await.unwrap();
        assert_eq!((recovered[0].expected, recovered[0].held), (2, 1));

        let r = reconcile(
            &heads,
            &inventory.broken(&heads),
            &[],
            &streams,
            &recovered,
            &[],
            |_| true,
            Options {
                now_ms: 10 * HOUR,
                settle_ms: HOUR,
            },
        );
        assert_eq!(kinds(&r), vec![(FindingKind::Gap, CELL.to_string())]);
        assert_eq!(r.recovered_short.len(), 1);
        emit(&bucket, r.records).await.unwrap();
        consumer.record_findings(&r.findings).await.unwrap();
        assert_eq!(bucket.list(REPORTS_PREFIX).await.unwrap().len(), 1);

        let again = BucketConsumer::load(bucket.clone()).await.unwrap();
        let state = again.state_at(&s, at(9, 0, 0)).await.unwrap().unwrap();
        assert!(state.gaps.iter().any(|g| matches!(
            g,
            Gap::Reported { to, .. } if *to == at(1, 3, u64::MAX)
        )));
        // The reconciler's own record does not break certification.
        assert_eq!(again.streams().await.unwrap()[0].certified[&1], at(1, 2, 1));
    });
}

// ── erase ────────────────────────────────────────────────────────────────────

fn tombstone_for(s: &StreamId, incarnation: Option<u64>) -> Tombstone {
    Tombstone {
        script: s.script.clone(),
        class: s.class.clone(),
        cell: s.cell.clone(),
        facet: s.facet.clone(),
        incarnation,
        erased_at_ms: 1,
        reason: Some("gdpr".into()),
        cleared_at_ms: None,
    }
}

#[test]
fn a_tombstone_without_an_incarnation_erases_every_incarnation() {
    let s = root();
    let mut other = s.clone();
    other.incarnation = 9;
    let all = tombstone_for(&s, None);
    assert!(all.matches(&s) && all.matches(&other));
    let one = tombstone_for(&s, Some(1));
    assert!(one.matches(&s) && !one.matches(&other));
    // Script, cell and facet must be equal, as in EXPORT_TOMBSTONES.
    assert!(!all.matches(&stream(CELL, Some("child"))));
    assert!(!all.matches(&StreamId {
        script: "other".into(),
        ..s.clone()
    }));
    let cleared = Tombstone {
        cleared_at_ms: Some(2),
        ..all.clone()
    };
    assert!(!cleared.matches(&s));
    assert!(all.covers_scope(CELL));
    let facet = tombstone_for(&stream(CELL, Some("a/b")), None);
    assert!(facet.covers_scope(&scope_of(CELL, Some("a/b"))));
    assert!(!facet.covers_scope(CELL));
}

#[test]
fn tombstone_keys_are_one_path_segment_per_part() {
    let t = tombstone_for(&stream(CELL, Some("a/b c")), None);
    assert_eq!(
        t.key(),
        "export/tombstones/Cart:one/app/f.a%2Fb%20c/all.json"
    );
    let t = Tombstone {
        script: String::new(),
        incarnation: Some(7),
        facet: None,
        ..t
    };
    assert_eq!(t.key(), "export/tombstones/Cart:one/-/root/7.json");
}

#[test]
fn erasing_a_root_erases_every_facet_the_consumer_holds() {
    let held = [
        root(),
        stream(CELL, Some("child")),
        StreamId {
            script: "other".into(),
            ..root()
        },
    ];
    let targets = erase_targets(CELL, "Cart", Selection::default(), &held, 5).unwrap();
    let named: Vec<(String, Option<String>)> = targets
        .iter()
        .map(|t| (t.script.clone(), t.facet.clone()))
        .collect();
    assert_eq!(
        named,
        vec![
            ("app".into(), None),
            ("app".into(), Some("child".into())),
            ("other".into(), None),
        ]
    );
    assert!(targets.iter().all(|t| t.incarnation.is_none()));

    let only = erase_targets(
        CELL,
        "Cart",
        Selection {
            script: Some("app"),
            facet: Some("child"),
            incarnation: Some(3),
            reason: None,
        },
        &held,
        5,
    )
    .unwrap();
    assert_eq!(only.len(), 1);
    assert_eq!(only[0].incarnation, Some(3));
    assert!(erase_targets(CELL, "Cart", Selection::default(), &[], 5).is_err());
}

#[test]
fn an_erased_stream_is_dropped_by_the_bucket_consumer_until_cleared() {
    block_on(async {
        let bucket = bucket();
        let s = root();
        let kept = stream("Cart:two", None);
        emit(
            &bucket,
            vec![
                rows(&s, at(1, 1, 1), "t", &[(1, "secret")]),
                rows(&kept, at(1, 1, 1), "t", &[(1, "fine")]),
            ],
        )
        .await
        .unwrap();
        let t = tombstone_for(&s, None);
        tombstone::put(&bucket, &t).await.unwrap();
        assert!(is_tombstoned(&bucket, &s).await.unwrap());
        assert!(!is_tombstoned(&bucket, &kept).await.unwrap());
        assert!(tombstone::is_scope_tombstoned(&bucket, CELL, CELL)
            .await
            .unwrap());

        let consumer = BucketConsumer::load(bucket.clone()).await.unwrap();
        let ids: Vec<StreamId> = consumer
            .streams()
            .await
            .unwrap()
            .into_iter()
            .map(|s| s.id)
            .collect();
        assert_eq!(ids, vec![kept.clone()]);

        tombstone::put(
            &bucket,
            &Tombstone {
                cleared_at_ms: Some(2),
                ..t
            },
        )
        .await
        .unwrap();
        assert!(!is_tombstoned(&bucket, &s).await.unwrap());
        assert_eq!(tombstone::load(&bucket).await.unwrap().len(), 1);
        let consumer = BucketConsumer::load(bucket).await.unwrap();
        assert_eq!(consumer.streams().await.unwrap().len(), 2);
    });
}

// ── verify ───────────────────────────────────────────────────────────────────

/// A database built by `sql`, as one whole-image LTX file over `min..=max`.
fn image(sql: &str, min: u64, max: u64) -> Vec<u8> {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db");
    let db = Connection::open(&path).unwrap();
    db.execute_batch("PRAGMA journal_mode=WAL;").unwrap();
    db.execute_batch(sql).unwrap();
    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    drop(db);
    let bytes = std::fs::read(&path).unwrap();
    let page_size = u32::from(u16::from_be_bytes([bytes[16], bytes[17]]));
    let pages: Vec<_> = bytes
        .chunks_exact(page_size as usize)
        .enumerate()
        .map(|(i, p)| (i as u32 + 1, p.to_vec()))
        .collect();
    let checksum = pages.iter().fold(celld_ltx::CHECKSUM_FLAG, |sum, (n, p)| {
        sum ^ (ltx::checksum_page(*n, p) & !celld_ltx::CHECKSUM_FLAG)
    });
    let header = ltx::Header {
        version: ltx::VERSION,
        page_size,
        commit: pages.len() as u32,
        min_txid: TXID(min),
        max_txid: TXID(max),
        ..Default::default()
    };
    ltx::encode_file(&header, &pages, checksum).unwrap()
}

async fn put_image(bucket: &Bucket, scope: &str, epoch: u64, sql: &str, max: u64) {
    let config = ObjectStoreConfig {
        path: format!("{}cells/{scope}/ltx/e{epoch}", bucket.prefix),
        ..Default::default()
    };
    ObjectStoreClient::with_store(config, bucket.store.clone())
        .write_ltx_file(0, TXID(1), TXID(max), &image(sql, 1, max))
        .await
        .unwrap();
}

const CELL_SQL: &str = "
    CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT);
    INSERT INTO t VALUES (1, 'a'), (2, 'b');
    CREATE TABLE _cf_KV (key TEXT PRIMARY KEY, value BLOB);
    INSERT INTO _cf_KV VALUES ('k', x'00');
    CREATE TABLE __queue_messages (id INTEGER PRIMARY KEY);
    CREATE TABLE hidden (id INTEGER PRIMARY KEY);
    INSERT INTO hidden VALUES (1);
";

async fn verify_with(records: Vec<Record>) -> verify::Verdict {
    let bucket = bucket();
    put_image(&bucket, CELL, 1, CELL_SQL, 4).await;
    let consumer = BucketConsumer::from_records(bucket.clone(), records, &[]).unwrap();
    let streams = consumer.streams().await.unwrap();
    let s = streams.iter().find(|s| s.id == root()).unwrap();
    verify::verify(&bucket, &consumer, s, |class, table| {
        class == "Cart" && table == "hidden"
    })
    .await
    .unwrap()
}

#[test]
fn verify_matches_a_consumer_that_holds_the_cell() {
    block_on(async {
        let s = root();
        let v = verify_with(vec![
            rows(&s, at(1, 4, 1), "t", &[(1, "a"), (2, "b")]),
            watermark(&s, at(1, 4, 1), 1, 1),
            // Past the restored head: ignored, not drift.
            rows(&s, at(1, 5, 1), "t", &[(3, "later")]),
        ])
        .await;
        assert_eq!(v.outcome, Outcome::Match { tables: 1, rows: 2 });
        assert_eq!((v.epoch, v.txid), (1, 4));
        let skipped: Vec<&str> = v.skipped.iter().map(|(t, _)| t.as_str()).collect();
        assert_eq!(skipped, vec!["_cf_KV", "hidden"]);
    });
}

#[test]
fn verify_reports_drift_row_by_row() {
    block_on(async {
        let s = root();
        let v = verify_with(vec![
            rows(&s, at(1, 4, 1), "t", &[(1, "changed"), (3, "extra")]),
            rows(&s, at(1, 4, 1), "gone", &[(1, "x")]),
            watermark(&s, at(1, 4, 1), 1, 2),
        ])
        .await;
        let Outcome::Drift { total, diffs, .. } = &v.outcome else {
            panic!("{v:?}");
        };
        assert_eq!(*total, 4);
        let seen: Vec<(DiffKind, &str, Vec<Value>)> = diffs
            .iter()
            .map(|d| (d.kind.clone(), d.table.as_str(), d.key.clone()))
            .collect();
        assert_eq!(
            seen,
            vec![
                (DiffKind::Changed, "t", vec![Value::Integer(1)]),
                (DiffKind::Missing, "t", vec![Value::Integer(2)]),
                (DiffKind::Extra, "t", vec![Value::Integer(3)]),
                (DiffKind::Extra, "gone", vec![]),
            ]
        );
    });
}

#[test]
fn verify_waits_for_the_consumer_to_certify_the_head() {
    block_on(async {
        let s = root();
        let v = verify_with(vec![
            rows(&s, at(1, 3, 1), "t", &[(1, "a")]),
            watermark(&s, at(1, 3, 1), 1, 1),
        ])
        .await;
        assert_eq!(
            v.outcome,
            Outcome::Behind {
                certified: Some(at(1, 3, 1))
            }
        );
        // A repair snapshot at the head makes it comparable.
        let mut records = vec![
            rows(&s, at(1, 3, 1), "t", &[(1, "a")]),
            watermark(&s, at(1, 3, 1), 1, 1),
        ];
        records.extend(snapshot(&s, at(1, 4, u64::MAX), "t", &[(1, "a"), (2, "b")]));
        let v = verify_with(records).await;
        assert!(matches!(v.outcome, Outcome::Match { .. }), "{v:?}");
    });
}

#[test]
fn a_sample_never_picks_a_stream_twice_or_a_deleted_one() {
    let mut deleted = summary(stream("Cart:gone", None), &[(1, 1)]);
    deleted.deleted_at = Some(at(1, 1, 1));
    let streams: Vec<StreamSummary> = (0..20)
        .map(|i| summary(stream(&format!("Cart:{i}"), None), &[(1, 1)]))
        .chain([deleted])
        .collect();
    let picked = verify::sample(&streams, 5, 42);
    assert_eq!(picked.len(), 5);
    let ids: std::collections::BTreeSet<_> = picked.iter().map(|s| s.id.clone()).collect();
    assert_eq!(ids.len(), 5);
    assert!(picked.iter().all(|s| s.deleted_at.is_none()));
    assert_eq!(verify::sample(&streams, 50, 1).len(), 20);
}

#[test]
fn a_rowid_table_is_keyed_by_its_rowid() {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE r (v TEXT); INSERT INTO r(rowid, v) VALUES (7, 'x');")
        .unwrap();
    let mut state = celld_export_format::StreamState::default();
    let mut t = celld_export_format::TableState {
        columns: vec!["v".into()],
        key_columns: vec![celld_export_format::ROWID_KEY_COLUMN.into()],
        ..Default::default()
    };
    t.rows
        .insert(vec![Value::Integer(7)], vec![Value::Text("x".into())]);
    state.tables.insert(
        TableGen {
            table: "r".into(),
            generation: 1,
        },
        t,
    );
    let (outcome, _) = verify::compare(&db, &state, |_| false).unwrap();
    assert_eq!(outcome, Outcome::Match { tables: 1, rows: 1 });
}

#[test]
fn the_snowflake_binds_follow_the_statements() {
    let finding = Finding {
        stream: root(),
        kind: FindingKind::Gap,
        scope: CELL.into(),
        head: Some(at(5, 9, u64::MAX)),
        from: Some(at(5, 8, 1)),
        certified: Some(at(5, 8, 1)),
        epochs: vec![5],
        detail: "d".into(),
    };
    let binds = snowflake::finding_binds(&finding);
    assert_eq!(snowflake::INSERT_FINDING.matches('?').count(), binds.len());
    assert_eq!(binds[3], serde_json::json!(""));
    assert_eq!(binds[5], serde_json::json!("gap"));
    let t = tombstone_for(&root(), None);
    assert_eq!(
        snowflake::INSERT_TOMBSTONE.matches('?').count(),
        snowflake::tombstone_binds(&t).len()
    );
    assert_eq!(
        snowflake::CLEAR_TOMBSTONE.matches('?').count(),
        snowflake::clear_binds(&t).len()
    );
    assert_eq!(
        snowflake::position_key(1, 2, 3),
        "00000000000000000001.00000000000000000002.00000000000000000003"
    );
}

#[test]
fn bucket_cache_reuses_objects_replaces_changes_and_expires_deleted_history() {
    block_on(async {
        let bucket = bucket();
        let path = tempfile::NamedTempFile::new().unwrap();
        let key = emit(
            &bucket,
            snapshot(&root(), at(1, 1, 1), "items", &[(1, "old")]),
        )
        .await
        .unwrap()
        .unwrap();
        let first = BucketConsumer::load_cached(bucket.clone(), Some(path.path()), "fleet-a", None)
            .await
            .unwrap();
        first.cache.db.lock().unwrap().execute_batch("CREATE TABLE imports (n INTEGER); INSERT INTO imports VALUES (0);
            CREATE TRIGGER count_import AFTER INSERT ON objects BEGIN UPDATE imports SET n=n+1; END;").unwrap();
        drop(first);
        let same = BucketConsumer::load_cached(bucket.clone(), Some(path.path()), "fleet-a", None)
            .await
            .unwrap();
        assert_eq!(
            same.cache
                .db
                .lock()
                .unwrap()
                .query_row("SELECT n FROM imports", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        assert_eq!(same.streams().await.unwrap().len(), 1);
        drop(same);
        bucket
            .put(
                &key,
                crate::export_sink::encode_records(snapshot(
                    &root(),
                    at(1, 2, 1),
                    "items",
                    &[(1, "new")],
                ))
                .unwrap(),
            )
            .await
            .unwrap();
        let changed =
            BucketConsumer::load_cached(bucket.clone(), Some(path.path()), "fleet-a", None)
                .await
                .unwrap();
        assert_eq!(
            changed
                .cache
                .db
                .lock()
                .unwrap()
                .query_row("SELECT n FROM imports", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            1
        );
        let state = changed
            .state_at(&root(), at(1, 2, 1))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            state.table("items").unwrap().rows[&vec![Value::Integer(1)]][1],
            Value::Text("new".into())
        );
        drop(changed);
        assert!(
            BucketConsumer::load_cached(bucket.clone(), Some(path.path()), "fleet-b", None)
                .await
                .is_err()
        );
        bucket.delete(&key).await.unwrap();
        let deleted = BucketConsumer::load_cached(bucket, Some(path.path()), "fleet-a", None)
            .await
            .unwrap();
        assert!(deleted.streams().await.unwrap().is_empty());
    });
}

#[test]
fn scoped_bucket_audit_evaluates_only_the_selected_cell() {
    block_on(async {
        let bucket = bucket();
        let other = stream("Cart:other", None);
        let records = [
            snapshot(&root(), at(1, 1, 1), "items", &[(1, "one")]),
            snapshot(&other, at(1, 1, 1), "items", &[(2, "two")]),
        ]
        .concat();
        emit(&bucket, records).await.unwrap();
        let consumer = BucketConsumer::load_cached(bucket, None, "test", Some(CELL.into()))
            .await
            .unwrap();
        // Poison the other cell's cached payload. A scoped audit must never decode it.
        consumer
            .cache
            .db
            .lock()
            .unwrap()
            .execute("UPDATE records SET data=x'00' WHERE cell=?1", [&other.cell])
            .unwrap();
        let summaries = consumer.streams().await.unwrap();
        assert_eq!(summaries.len(), 1);
        assert_eq!(summaries[0].id, root());
        assert!(consumer
            .state_at(&root(), at(1, 1, 1))
            .await
            .unwrap()
            .is_some());
    });
}

#[test]
fn persistent_audit_cache_applies_and_clears_tombstones() {
    block_on(async {
        let bucket = bucket();
        let path = tempfile::NamedTempFile::new().unwrap();
        emit(
            &bucket,
            snapshot(&root(), at(1, 1, 1), "items", &[(1, "secret")]),
        )
        .await
        .unwrap();
        let consumer =
            BucketConsumer::load_cached(bucket.clone(), Some(path.path()), "fleet", None)
                .await
                .unwrap();
        assert_eq!(consumer.streams().await.unwrap().len(), 1);
        drop(consumer);
        let mut erased = tombstone_for(&root(), None);
        tombstone::put(&bucket, &erased).await.unwrap();
        let consumer =
            BucketConsumer::load_cached(bucket.clone(), Some(path.path()), "fleet", None)
                .await
                .unwrap();
        assert!(consumer.streams().await.unwrap().is_empty());
        assert_eq!(
            consumer
                .cache
                .db
                .lock()
                .unwrap()
                .query_row("SELECT count(*) FROM records", [], |r| r.get::<_, i64>(0))
                .unwrap(),
            0
        );
        drop(consumer);
        erased.cleared_at_ms = Some(erased.erased_at_ms + 1);
        tombstone::put(&bucket, &erased).await.unwrap();
        let consumer = BucketConsumer::load_cached(bucket, Some(path.path()), "fleet", None)
            .await
            .unwrap();
        assert_eq!(consumer.streams().await.unwrap().len(), 1);
    });
}

#[test]
fn bucket_audit_enforces_an_operator_history_budget() {
    block_on(async {
        let records = snapshot(&root(), at(1, 1, 1), "items", &[(1, "data")]);
        let consumer = BucketConsumer::from_records(bucket(), records, &[])
            .unwrap()
            .with_history_limit(1)
            .unwrap();
        let error = consumer.streams().await.unwrap_err().to_string();
        assert!(error.contains("exceeds the 1-byte audit limit"), "{error}");
        let consumer = consumer.with_history_limit(65536).unwrap();
        assert_eq!(consumer.streams().await.unwrap().len(), 1);
    });
}
