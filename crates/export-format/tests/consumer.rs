mod common;

use celld_export_format::*;
use common::*;

fn apply(records: Vec<Record>) -> StreamState {
    let mut c = Consumer::new();
    c.ingest_all(records).unwrap();
    c.stream(&stream()).unwrap_or_default()
}

fn one(id: i64, s: &str) -> (i64, String) {
    (id, s.to_owned())
}

#[test]
fn rows_apply_in_position_order_whatever_the_arrival_order() {
    let records = vec![
        rows(pos(3, 3), "t", 1, vec![del(2, "b")]),
        rows(pos(1, 1), "t", 1, vec![put(1, "a"), put(2, "b")]),
        rows(pos(2, 2), "t", 1, vec![upd(1, "a2")]),
        // A duplicate delivery.
        rows(pos(1, 1), "t", 1, vec![put(1, "a"), put(2, "b")]),
    ];
    assert_eq!(rows_of(&apply(records.clone()), "t"), vec![one(1, "a2")]);
    let mut reversed = records;
    reversed.reverse();
    assert_eq!(rows_of(&apply(reversed), "t"), vec![one(1, "a2")]);
}

#[test]
fn a_snapshot_supersedes_its_scope_including_rows_it_lacks() {
    let state = apply(vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a"), put(2, "phantom")]),
        rows(pos(1, 1), "u", 1, vec![put(9, "gone")]),
        snapshot(pos(5, 4), "s1", "t", 1, vec![put(1, "a-repaired")]),
        snapshot_end(pos(5, 4), "s1", SnapshotScope::Stream, &[("t", 1)], 1),
        rows(pos(6, 5), "t", 1, vec![put(3, "after")]),
    ]);
    assert_eq!(
        rows_of(&state, "t"),
        vec![one(1, "a-repaired"), one(3, "after")]
    );
    // Table `u` is not in the stream-wide snapshot, so it is emptied.
    assert!(state.table("u").is_none());
}

#[test]
fn an_incomplete_snapshot_changes_nothing() {
    let state = apply(vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a"), put(2, "b")]),
        snapshot(pos(5, 4), "s1", "t", 1, vec![put(1, "a")]),
        snapshot_end(pos(5, 4), "s1", SnapshotScope::Stream, &[("t", 1)], 2),
    ]);
    assert_eq!(rows_of(&state, "t"), vec![one(1, "a"), one(2, "b")]);
}

#[test]
fn a_table_scoped_snapshot_leaves_other_tables_alone() {
    let state = apply(vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a")]),
        rows(pos(1, 1), "u", 1, vec![put(9, "kept")]),
        snapshot(pos(2, 2), "ddl", "t", 1, vec![put(1, "b")]),
        snapshot_end(pos(2, 2), "ddl", SnapshotScope::Tables, &[("t", 1)], 1),
    ]);
    assert_eq!(rows_of(&state, "t"), vec![one(1, "b")]);
    assert_eq!(rows_of(&state, "u"), vec![one(9, "kept")]);
}

#[test]
fn repair_wins_over_live_at_an_equal_position() {
    let state = apply(vec![
        rows(pos(5, 4), "t", 1, vec![put(1, "live")]),
        snapshot(pos(5, 4), "s1", "t", 1, vec![put(1, "repair")]),
        snapshot_end(pos(5, 4), "s1", SnapshotScope::Stream, &[("t", 1)], 1),
    ]);
    assert_eq!(rows_of(&state, "t"), vec![one(1, "repair")]);
}

#[test]
fn deleted_removes_the_stream_at_or_below_its_position() {
    let state = apply(vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a")]),
        live(
            pos(2, 2),
            Body::Deleted(DeletedBody {
                facet: None,
                incarnation: None,
                subtree: false,
            }),
        ),
        rows(pos(3, 3), "t", 1, vec![put(2, "b")]),
    ]);
    assert_eq!(state.deleted_at, Some(pos(2, 2)));
    assert_eq!(rows_of(&state, "t"), vec![one(2, "b")]);
}

#[test]
fn a_facet_deleted_from_the_root_removes_that_incarnation_and_its_subtree() {
    let old = facet("rooms/7", 11);
    let child = facet("rooms/7/thread", 12);
    let new = facet("rooms/7", 13);
    let sibling = facet("rooms/70", 14);
    let body = || {
        Body::Rows(RowsBody {
            data: table_rows("t", 1, vec![put(1, "x")]),
        })
    };
    let mut c = Consumer::new();
    for s in [&old, &child, &new, &sibling] {
        c.ingest(record(s, pos(1, 1), Origin::Live, body()))
            .unwrap();
    }
    c.ingest(live(
        pos(9, 9),
        Body::Deleted(DeletedBody {
            facet: Some("rooms/7".into()),
            incarnation: Some(11),
            subtree: true,
        }),
    ))
    .unwrap();
    // A record of the old incarnation that arrives late stays gone.
    c.ingest(record(&old, pos(2, 2), Origin::Live, body()))
        .unwrap();
    let state = c.state();
    assert!(!state.contains_key(&old));
    assert!(!state.contains_key(&child));
    assert!(state.contains_key(&new));
    assert!(state.contains_key(&sibling));
    assert!(state.contains_key(&stream()));
}

#[test]
fn closed_generations_have_no_rows() {
    let dropped = SchemaBody {
        dropped: true,
        ..schema("t", 1)
    };
    let renamed = SchemaBody {
        renamed_from: Some("u".into()),
        ..schema("v", 1)
    };
    let state = apply(vec![
        live(pos(1, 1), Body::Schema(schema("t", 1))),
        rows(pos(1, 1), "t", 1, vec![put(1, "old")]),
        live(pos(2, 2), Body::Schema(dropped)),
        // Recreated under the same name: a new generation, no resurrection.
        live(pos(3, 3), Body::Schema(schema("t", 2))),
        rows(pos(3, 3), "t", 2, vec![put(2, "new")]),
        // `w` is altered: generation 2 supersedes 1.
        rows(pos(1, 1), "w", 1, vec![put(1, "w1")]),
        live(pos(4, 4), Body::Schema(schema("w", 2))),
        snapshot(pos(4, 4), "ddl-w", "w", 2, vec![put(1, "w1")]),
        snapshot_end(pos(4, 4), "ddl-w", SnapshotScope::Tables, &[("w", 2)], 1),
        // `u` renamed to `v`.
        rows(pos(1, 1), "u", 1, vec![put(5, "u")]),
        live(pos(5, 5), Body::Schema(renamed)),
    ]);
    assert_eq!(rows_of(&state, "t"), vec![one(2, "new")]);
    assert_eq!(state.tables.keys().filter(|tg| tg.table == "t").count(), 1);
    assert_eq!(rows_of(&state, "w"), vec![one(1, "w1")]);
    assert!(state.tables.contains_key(&TableGen {
        table: "w".into(),
        generation: 2
    }));
    assert!(state.table("u").is_none());
}

#[test]
fn bulk_is_uncertain_until_a_snapshot_covers_it() {
    let tg = TableGen {
        table: "t".into(),
        generation: 1,
    };
    let bulk = || {
        live(
            pos(2, 2),
            Body::Bulk(BulkBody {
                tables: vec![tg.clone()],
            }),
        )
    };
    let mut records = vec![rows(pos(1, 1), "t", 1, vec![put(1, "a")]), bulk()];
    assert!(apply(records.clone()).uncertain.contains(&tg));
    records.push(snapshot(pos(3, 3), "s", "t", 1, vec![put(1, "big")]));
    records.push(snapshot_end(
        pos(3, 3),
        "s",
        SnapshotScope::Stream,
        &[("t", 1)],
        1,
    ));
    let state = apply(records);
    assert!(state.uncertain.is_empty());
    assert_eq!(rows_of(&state, "t"), vec![one(1, "big")]);
}

fn watermark(from: Option<Position>, through: Position, commits: u64, records: u64) -> Record {
    live(
        through,
        Body::Watermark(WatermarkBody {
            from,
            through,
            commits,
            records,
        }),
    )
}

#[test]
fn watermarks_certify_only_matching_contiguous_ranges() {
    let base = vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a")]),
        rows(pos(1, 1), "u", 1, vec![put(1, "a")]),
        rows(pos(2, 2), "t", 1, vec![put(2, "b")]),
        watermark(None, pos(2, 2), 2, 3),
        rows(pos(4, 3), "t", 1, vec![put(3, "c")]),
        watermark(Some(pos(2, 2)), pos(4, 3), 1, 1),
    ];
    assert_eq!(apply(base.clone()).certified_head(), Some(pos(4, 3)));

    // Missing the commit at txid 2: the first watermark does not match, so
    // nothing is certified, not even the second range.
    let missing: Vec<Record> = base
        .iter()
        .filter(|r| r.position() != pos(2, 2) || matches!(r.body, Body::Watermark(_)))
        .cloned()
        .collect();
    assert_eq!(apply(missing).certified_head(), None);

    // Missing the last commit: only the first range is certified.
    let tail: Vec<Record> = base
        .iter()
        .filter(|r| r.position() != pos(4, 3) || matches!(r.body, Body::Watermark(_)))
        .cloned()
        .collect();
    assert_eq!(apply(tail).certified_head(), Some(pos(2, 2)));
}

#[test]
fn links_and_recovered_heads_beyond_certification_are_gaps_until_repaired() {
    let next_epoch = Position::new(2, 1, 1);
    let mut records = vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a")]),
        watermark(None, pos(1, 1), 1, 1),
        // The predecessor reached txid 3, but only txid 1 was certified.
        live(
            next_epoch,
            Body::Link(LinkBody {
                start_txid: 1,
                prev_epoch: Some(1),
                prev_txid: Some(3),
                mode: LinkMode::Paged,
            }),
        ),
        live(
            pos(3, 3),
            Body::Recovered(RecoveredBody {
                session: "s".into(),
                head: pos(3, 3),
                loss: false,
                cells: 1,
            }),
        ),
        live(
            pos(2, 2),
            Body::Gap(GapBody {
                from: pos(1, 1),
                to: pos(3, 3),
                reason: "queue overflow".into(),
            }),
        ),
    ];
    let state = apply(records.clone());
    assert_eq!(state.gaps.len(), 3, "{:?}", state.gaps);
    assert!(state
        .gaps
        .iter()
        .any(|g| matches!(g, Gap::Link { prev_txid: 3, .. })));

    // A link that stays within certification is no gap.
    let ok = apply(vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a")]),
        watermark(None, pos(1, 1), 1, 1),
        live(
            next_epoch,
            Body::Link(LinkBody {
                start_txid: 1,
                prev_epoch: Some(1),
                prev_txid: Some(1),
                mode: LinkMode::Clone,
            }),
        ),
    ]);
    assert!(ok.gaps.is_empty());

    // A stream-wide repair at or past the gaps closes them.
    records.push(snapshot(pos(3, 3), "r", "t", 1, vec![put(1, "a")]));
    records.push(snapshot_end(
        pos(3, 3),
        "r",
        SnapshotScope::Stream,
        &[("t", 1)],
        1,
    ));
    assert!(apply(records).gaps.is_empty());
}

#[test]
fn fragments_apply_only_once_complete() {
    let changes: Vec<RowChange> = (0..100).map(|i| put(i, &"z".repeat(40))).collect();
    let Split::Fragments(parts) = split(rows(pos(1, 1), "t", 1, changes), 1024) else {
        panic!()
    };
    let mut c = Consumer::new();
    c.ingest_all(parts[1..].iter().cloned()).unwrap();
    assert_eq!(c.incomplete(), 1);
    assert!(c.stream(&stream()).is_none());
    c.ingest(parts[0].clone()).unwrap();
    assert_eq!(c.incomplete(), 0);
    assert_eq!(rows_of(&c.stream(&stream()).unwrap(), "t").len(), 100);
}

#[test]
fn bulk_markers_for_two_tables_of_one_commit_both_count() {
    let big = |t: &str| rows(pos(2, 2), t, 1, vec![put(1, &"q".repeat(2000))]);
    let mut records = vec![
        live(pos(1, 1), Body::Schema(schema("a", 1))),
        live(pos(1, 1), Body::Schema(schema("b", 1))),
    ];
    for t in ["a", "b"] {
        let Split::Bulk(b) = split(big(t), 1000) else {
            panic!("expected bulk")
        };
        records.push(*b);
    }
    assert_ne!(DedupKey::of(&records[2]), DedupKey::of(&records[3]));
    for order in [records.clone(), records.into_iter().rev().collect()] {
        assert_eq!(apply(order).uncertain.len(), 2);
    }
}

#[test]
fn a_table_seen_only_in_a_bulk_marker_is_uncertain() {
    let tg = TableGen {
        table: "t".into(),
        generation: 1,
    };
    let bulk = live(
        pos(2, 2),
        Body::Bulk(BulkBody {
            tables: vec![tg.clone()],
        }),
    );
    assert!(apply(vec![bulk.clone()]).uncertain.contains(&tg));

    // A later snapshot still clears it, and a closed generation is not
    // uncertain.
    let repaired = apply(vec![
        bulk.clone(),
        snapshot_end(pos(3, 3), "s", SnapshotScope::Stream, &[("t", 1)], 0),
    ]);
    assert!(repaired.uncertain.is_empty());
    let dropped = apply(vec![
        bulk,
        live(
            pos(4, 4),
            Body::Schema(SchemaBody {
                dropped: true,
                ..schema("t", 1)
            }),
        ),
    ]);
    assert!(dropped.uncertain.is_empty());
}

/// A `recovered` record as dead-node recovery emits it: no script and
/// incarnation 0, since recovery knows neither.
fn recovered(head: Position, loss: bool) -> Record {
    let unscripted = StreamId {
        script: String::new(),
        incarnation: 0,
        ..stream()
    };
    let mut r = record(
        &unscripted,
        head,
        Origin::Live,
        Body::Recovered(RecoveredBody {
            session: "node-a/g1".into(),
            head,
            loss,
            cells: 1,
        }),
    );
    r.envelope.node = "node-b".into();
    r
}

#[test]
fn a_recovered_record_applies_to_its_cells_stream_without_breaking_certification() {
    let head = pos(3, u64::MAX);
    let mut c = Consumer::new();
    c.ingest_all(vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a")]),
        rows(pos(3, 2), "t", 1, vec![put(2, "b")]),
        watermark(None, pos(3, 2), 2, 2),
        recovered(head, false),
    ])
    .unwrap();
    let state = c.state();
    assert_eq!(
        state.len(),
        1,
        "the scriptless stream is adopted: {state:?}"
    );
    let s = &state[&stream()];
    // Recovery reached txid 3 and so did certification: no gap, and the
    // recovered record is not counted against the dead node's watermark.
    assert_eq!(s.certified_head(), Some(pos(3, 2)));
    assert!(s.gaps.is_empty(), "{:?}", s.gaps);

    // Recovery past certification is a gap on the cell's own stream.
    let mut c = Consumer::new();
    c.ingest_all(vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a")]),
        watermark(None, pos(1, 1), 1, 1),
        recovered(head, false),
    ])
    .unwrap();
    let s = c.stream(&stream()).unwrap();
    assert_eq!(
        s.gaps,
        vec![Gap::Recovered {
            head,
            certified: Some(pos(1, 1)),
            loss: false,
        }]
    );
}

#[test]
fn a_recovered_record_for_a_stream_never_seen_is_its_own_gap() {
    let mut c = Consumer::new();
    c.ingest(recovered(pos(3, u64::MAX), false)).unwrap();
    let state = c.state();
    assert_eq!(state.len(), 1);
    let (id, s) = state.iter().next().unwrap();
    assert!(id.script.is_empty());
    assert!(matches!(
        s.gaps[..],
        [Gap::Recovered {
            certified: None,
            ..
        }]
    ));
}

#[test]
fn a_recovered_record_picks_the_incarnation_its_epoch_belongs_to() {
    let older = StreamId {
        incarnation: 1,
        ..stream()
    };
    let newer = StreamId {
        incarnation: 5,
        ..stream()
    };
    let head = Position::new(3, 9, u64::MAX);
    let mut c = Consumer::new();
    c.ingest_all(vec![
        record(
            &older,
            Position::new(1, 1, 1),
            Origin::Live,
            Body::Rows(RowsBody {
                data: table_rows("t", 1, vec![put(1, "a")]),
            }),
        ),
        record(
            &newer,
            Position::new(5, 1, 1),
            Origin::Live,
            Body::Rows(RowsBody {
                data: table_rows("t", 1, vec![put(1, "a")]),
            }),
        ),
        recovered(head, false),
    ])
    .unwrap();
    assert_eq!(c.stream(&older).unwrap().gaps.len(), 1);
    assert!(c.stream(&newer).unwrap().gaps.is_empty());
}

#[test]
fn a_loss_below_certification_is_a_gap_until_a_later_snapshot() {
    // The dead node exported through txid 4; recovery could only reach 3.
    let head = pos(3, u64::MAX);
    let mut records = vec![
        rows(pos(1, 1), "t", 1, vec![put(1, "a")]),
        rows(pos(4, 2), "t", 1, vec![put(2, "lost")]),
        watermark(None, pos(4, 2), 2, 2),
        recovered(head, true),
    ];
    let s = apply(records.clone());
    assert_eq!(
        s.gaps,
        vec![Gap::Recovered {
            head,
            certified: Some(pos(4, 2)),
            loss: true,
        }]
    );
    // Without the loss the same heads are no gap.
    records[3] = recovered(head, false);
    assert!(apply(records.clone()).gaps.is_empty());

    // A repair at the recovered head does not supersede txid 4; one in the
    // cell's next epoch does, and removes the lost row.
    records[3] = recovered(head, true);
    records.push(snapshot(head, "at-head", "t", 1, vec![put(1, "a")]));
    records.push(snapshot_end(
        head,
        "at-head",
        SnapshotScope::Stream,
        &[("t", 1)],
        1,
    ));
    assert_eq!(apply(records.clone()).gaps.len(), 1);
    let next = Position::new(2, 1, 0);
    records.push(snapshot(next, "next", "t", 1, vec![put(1, "a")]));
    records.push(snapshot_end(
        next,
        "next",
        SnapshotScope::Stream,
        &[("t", 1)],
        1,
    ));
    let s = apply(records);
    assert!(s.gaps.is_empty(), "{:?}", s.gaps);
    assert_eq!(rows_of(&s, "t"), vec![one(1, "a")]);
}

#[test]
fn only_the_unadopted_recovered_records_stay_scriptless() {
    // Recovery reported epochs 3 and 5; only incarnation 5 is known, and it
    // is certified through its recovered head.
    let known = StreamId {
        incarnation: 5,
        ..stream()
    };
    let at = |epoch: u64, txid: u64| Position::new(epoch, txid, u64::MAX);
    let mut c = Consumer::new();
    c.ingest_all(vec![
        record(
            &known,
            Position::new(5, 2, 1),
            Origin::Live,
            Body::Rows(RowsBody {
                data: table_rows("t", 1, vec![put(1, "a")]),
            }),
        ),
        record(
            &known,
            Position::new(5, 2, 1),
            Origin::Live,
            Body::Watermark(WatermarkBody {
                from: None,
                through: Position::new(5, 2, 1),
                commits: 1,
                records: 1,
            }),
        ),
        recovered(at(3, 7), false),
        recovered(at(5, 2), false),
    ])
    .unwrap();
    let state = c.state();
    assert!(state[&known].gaps.is_empty(), "{:?}", state[&known].gaps);
    let scriptless: Vec<&StreamState> = state
        .iter()
        .filter(|(s, _)| s.script.is_empty())
        .map(|(_, st)| st)
        .collect();
    assert_eq!(scriptless.len(), 1);
    assert!(
        matches!(scriptless[0].gaps[..], [Gap::Recovered { head, .. }] if head == at(3, 7)),
        "{:?}",
        scriptless[0].gaps
    );
}
