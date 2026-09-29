mod common;

use celld_export_format::*;
use common::*;
use serde_json::json;

#[test]
fn a_rows_record_is_one_flat_object() {
    let r = rows(pos(7, 3), "messages", 2, vec![put(1, "hi"), del(2, "bye")]);
    let v: serde_json::Value = serde_json::from_slice(&r.to_json()).unwrap();
    assert_eq!(
        v,
        json!({
            "kind": "rows",
            "script": "app", "class": "Chat", "cell": "c0ffee", "cell_name": "room-1",
            "facet": null, "incarnation": 1,
            "epoch": 1, "txid": 7, "commit": 3,
            "committed_at": 1_790_000_000_000i64,
            "node": "node-a", "origin": "live",
            "fragment": 1, "fragments": 1,
            "table": "messages", "generation": 2,
            "columns": ["id", "body"], "key_columns": ["id"],
            "rows": [["I", [1], [1, "hi"]], ["D", [2], [2, "bye"]]],
        })
    );
}

#[test]
fn every_kind_round_trips() {
    let tg = TableGen {
        table: "t".into(),
        generation: 1,
    };
    let bodies = vec![
        Body::Rows(RowsBody {
            data: table_rows("t", 1, vec![put(1, "a"), upd(1, "b")]),
        }),
        Body::Snapshot(SnapshotBody {
            snapshot_id: "s1".into(),
            data: table_rows("t", 1, vec![put(1, "a")]),
        }),
        Body::SnapshotEnd(SnapshotEndBody {
            snapshot_id: "s1".into(),
            scope: SnapshotScope::Stream,
            tables: vec![tg.clone()],
            records: 1,
        }),
        Body::Schema(SchemaBody {
            renamed_from: Some("old".into()),
            ..schema("t", 1)
        }),
        Body::Schema(SchemaBody {
            dropped: true,
            ..schema("t", 1)
        }),
        Body::Link(LinkBody {
            start_txid: 1,
            prev_epoch: Some(3),
            prev_txid: Some(40),
            mode: LinkMode::Clone,
        }),
        Body::Link(LinkBody {
            start_txid: 1,
            prev_epoch: None,
            prev_txid: None,
            mode: LinkMode::Fresh,
        }),
        Body::Recovered(RecoveredBody {
            session: "sess-9".into(),
            head: pos(12, 4),
        }),
        Body::Deleted(DeletedBody {
            facet: None,
            incarnation: None,
            subtree: false,
        }),
        Body::Deleted(DeletedBody {
            facet: Some("a/b".into()),
            incarnation: Some(99),
            subtree: true,
        }),
        Body::Watermark(WatermarkBody {
            from: Some(pos(3, 1)),
            through: pos(9, 7),
            commits: 4,
            records: 6,
        }),
        Body::Bulk(BulkBody { tables: vec![tg] }),
        Body::Gap(GapBody {
            from: pos(1, 1),
            to: pos(5, 2),
            reason: "attribution".into(),
        }),
    ];
    let mut kinds = std::collections::BTreeSet::new();
    for body in bodies {
        kinds.insert(body.kind());
        let r = live(pos(9, 7), body);
        let back = Record::from_json(&r.to_json()).unwrap();
        assert_eq!(back, r);
        let v: serde_json::Value = serde_json::from_slice(&r.to_json()).unwrap();
        let kind = serde_json::to_value(r.kind()).unwrap();
        assert_eq!(v["kind"], kind);
    }
    assert_eq!(kinds.len(), Kind::ALL.len());
}

#[test]
fn a_facet_stream_names_its_path() {
    let r = record(
        &facet("rooms/7", 0xdead_beef),
        pos(1, 1),
        Origin::Live,
        Body::Bulk(BulkBody { tables: vec![] }),
    );
    let v: serde_json::Value = serde_json::from_slice(&r.to_json()).unwrap();
    assert_eq!(v["facet"], "rooms/7");
    assert_eq!(v["incarnation"], 0xdead_beefu64);
}

#[test]
fn unknown_fields_are_ignored_and_bad_fragments_refused() {
    let r = rows(pos(1, 1), "t", 1, vec![put(1, "a")]);
    let mut v: serde_json::Value = serde_json::from_slice(&r.to_json()).unwrap();
    v["added_later"] = json!(true);
    assert_eq!(
        Record::from_json(&serde_json::to_vec(&v).unwrap()).unwrap(),
        r
    );
    v["fragment"] = json!(3);
    v["fragments"] = json!(2);
    assert!(Record::from_json(&serde_json::to_vec(&v).unwrap()).is_err());
    assert!(Record::from_json(br#"{"kind":"nope"}"#).is_err());
}

#[test]
fn a_small_record_is_one_fragment() {
    let r = rows(pos(1, 1), "t", 1, vec![put(1, "a")]);
    assert_eq!(split(r.clone(), 1 << 20), Split::Fragments(vec![r]));
}

#[test]
fn a_large_record_splits_by_rows_and_reassembles() {
    let changes: Vec<RowChange> = (0..200).map(|i| put(i, &"x".repeat(50))).collect();
    let r = rows(pos(4, 2), "t", 1, changes);
    let limit = 1024;
    let Split::Fragments(parts) = split(r.clone(), limit) else {
        panic!("expected fragments")
    };
    assert!(parts.len() > 1);
    let k = parts.len() as u32;
    for (i, p) in parts.iter().enumerate() {
        assert!(
            p.to_json().len() <= limit,
            "fragment {} is {} bytes",
            i + 1,
            p.to_json().len()
        );
        assert_eq!(p.envelope.fragment, i as u32 + 1);
        assert_eq!(p.envelope.fragments, k);
        assert_eq!(p.position(), r.position());
    }
    let mut asm = Reassembler::new();
    let mut out = None;
    // Reverse order, with a duplicate.
    for p in parts.iter().rev().chain(parts.first()) {
        if let Some(whole) = asm.push(p.clone()).unwrap() {
            assert!(out.is_none());
            out = Some(whole);
        }
    }
    assert_eq!(out.unwrap(), r);
}

#[test]
fn a_row_too_large_alone_becomes_bulk() {
    let r = rows(
        pos(4, 2),
        "t",
        3,
        vec![put(1, "small"), put(2, &"y".repeat(4000))],
    );
    let Split::Bulk(b) = split(r.clone(), 1024) else {
        panic!("expected bulk")
    };
    assert_eq!(b.position(), r.position());
    assert_eq!(
        b.body,
        Body::Bulk(BulkBody {
            tables: vec![TableGen {
                table: "t".into(),
                generation: 3
            }]
        })
    );
    assert!(b.to_json().len() <= 1024);
}

#[test]
fn other_kinds_are_never_split() {
    let r = live(
        pos(1, 1),
        Body::Gap(GapBody {
            from: pos(1, 1),
            to: pos(2, 1),
            reason: "z".repeat(5000),
        }),
    );
    assert_eq!(split(r.clone(), 100), Split::Fragments(vec![r]));
}

#[test]
fn mismatched_fragments_are_refused() {
    let changes: Vec<RowChange> = (0..50).map(|i| put(i, &"x".repeat(50))).collect();
    let Split::Fragments(parts) = split(rows(pos(1, 1), "t", 1, changes), 1024) else {
        panic!()
    };
    let mut asm = Reassembler::new();
    asm.push(parts[0].clone()).unwrap();
    let mut odd = parts[1].clone();
    odd.envelope.node = "node-b".into();
    assert_eq!(asm.push(odd), Err(ReassembleError::Inconsistent));
}

#[test]
fn dedup_keys_separate_what_must_not_merge() {
    let a = rows(pos(1, 1), "t", 1, vec![put(1, "a")]);
    let b = rows(pos(1, 1), "t", 2, vec![put(1, "a")]);
    assert_ne!(DedupKey::of(&a), DedupKey::of(&b));
    assert_eq!(DedupKey::of(&a), DedupKey::of(&a.clone()));
    let s1 = snapshot(pos(1, 1), "s1", "t", 1, vec![put(1, "a")]);
    let s2 = snapshot(pos(1, 1), "s2", "t", 1, vec![put(1, "a")]);
    assert_ne!(DedupKey::of(&s1), DedupKey::of(&s2));
    let keys = row_keys(&a);
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0].key, vec![Value::Integer(1)]);
    assert_eq!(keys[0].table, "t");
}
