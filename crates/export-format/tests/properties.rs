//! Property tests: random transactions against random tables, a test
//! producer that emits what the exporter would, delivery that duplicates,
//! reorders, and fragments, and the reference consumer converging to the
//! model database.

mod common;

use std::collections::BTreeMap;

use celld_export_format::*;
use common::{record, stream};
use proptest::prelude::*;

type Table = BTreeMap<Vec<Value>, Vec<Value>>;
type Db = BTreeMap<String, Table>;

const TABLES: [&str; 3] = ["a", "b", "c"];

fn value() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<i64>().prop_map(Value::Integer),
        prop_oneof![
            any::<f64>().prop_filter("SQLite stores no NaN", |f| !f.is_nan()),
            Just(f64::INFINITY),
            Just(f64::NEG_INFINITY),
            Just(-0.0),
        ]
        .prop_map(Value::Real),
        ".{0,12}".prop_map(Value::Text),
        prop::collection::vec(any::<u8>(), 0..16).prop_map(Value::Blob),
    ]
}

fn key() -> impl Strategy<Value = Value> {
    prop_oneof![
        (0i64..12).prop_map(Value::Integer),
        "[a-d]{1,2}".prop_map(Value::Text),
    ]
}

#[derive(Clone, Debug)]
enum Write {
    Put {
        table: usize,
        key: Value,
        body: Value,
    },
    Delete {
        table: usize,
        key: Value,
    },
}

fn write() -> impl Strategy<Value = Write> {
    prop_oneof![
        3 => (0..TABLES.len(), key(), value()).prop_map(|(table, key, body)| Write::Put { table, key, body }),
        1 => (0..TABLES.len(), key()).prop_map(|(table, key)| Write::Delete { table, key }),
    ]
}

/// A commit, and whether a new LTX capture starts before it.
fn history() -> impl Strategy<Value = Vec<(bool, Vec<Write>)>> {
    prop::collection::vec((any::<bool>(), prop::collection::vec(write(), 0..6)), 1..25)
}

fn row(key: &Value, body: &Value) -> Vec<Value> {
    vec![key.clone(), body.clone()]
}

fn table_rows(table: &str, rows: Vec<RowChange>) -> TableRows {
    TableRows {
        table: table.into(),
        generation: 1,
        columns: vec!["k".into(), "v".into()],
        key_columns: vec!["k".into()],
        rows,
    }
}

/// Run the history on a model database and emit the records the exporter
/// would: one net `rows` record per table per commit, and a watermark after
/// each capture boundary chosen by `watermark_every`.
fn produce(
    history: &[(bool, Vec<Write>)],
    watermark_every: usize,
) -> (Db, Vec<Record>, Option<Position>) {
    let mut db: Db = TABLES
        .iter()
        .map(|t| (t.to_string(), Table::new()))
        .collect();
    let mut out = Vec::new();
    let (mut txid, mut commit) = (1u64, 0u64);
    let mut last_mark: Option<Position> = None;
    let mut pending: (u64, u64) = (0, 0);
    for (i, (new_capture, writes)) in history.iter().enumerate() {
        if *new_capture {
            txid += 1;
        }
        commit += 1;
        let at = Position::new(1, txid, commit);
        let before = db.clone();
        for w in writes {
            match w {
                Write::Put { table, key, body } => {
                    db.get_mut(TABLES[*table])
                        .unwrap()
                        .insert(vec![key.clone()], row(key, body));
                }
                Write::Delete { table, key } => {
                    db.get_mut(TABLES[*table])
                        .unwrap()
                        .remove(&vec![key.clone()]);
                }
            }
        }
        let mut records_here = 0;
        for t in TABLES {
            let (old, new) = (&before[t], &db[t]);
            let mut changes = Vec::new();
            for k in old
                .keys()
                .chain(new.keys())
                .collect::<std::collections::BTreeSet<_>>()
            {
                match (old.get(k), new.get(k)) {
                    (None, Some(r)) => changes.push(RowChange(Op::Insert, k.clone(), r.clone())),
                    (Some(a), Some(b)) if a != b => {
                        changes.push(RowChange(Op::Update, k.clone(), b.clone()))
                    }
                    (Some(a), None) => changes.push(RowChange(Op::Delete, k.clone(), a.clone())),
                    _ => {}
                }
            }
            if !changes.is_empty() {
                out.push(record(
                    &stream(),
                    at,
                    Origin::Live,
                    Body::Rows(RowsBody {
                        data: table_rows(t, changes),
                    }),
                ));
                records_here += 1;
            }
        }
        if records_here > 0 {
            pending.0 += 1;
            pending.1 += records_here;
        }
        if (i + 1) % watermark_every == 0 {
            out.push(record(
                &stream(),
                at,
                Origin::Live,
                Body::Watermark(WatermarkBody {
                    from: last_mark,
                    through: at,
                    commits: pending.0,
                    records: pending.1,
                }),
            ));
            last_mark = Some(at);
            pending = (0, 0);
        }
    }
    (db, out, last_mark)
}

/// Fragment every record, duplicate some fragments, and shuffle.
fn deliver(records: Vec<Record>, max_bytes: usize, seed: u64) -> Vec<Record> {
    let mut out = Vec::new();
    for r in records {
        match split(r, max_bytes) {
            Split::Fragments(parts) => out.extend(parts),
            Split::Bulk(_) => panic!("test rows are small enough to fit"),
        }
    }
    let mut s = seed | 1;
    let mut next = move || {
        s ^= s << 13;
        s ^= s >> 7;
        s ^= s << 17;
        s
    };
    let dups: Vec<Record> = out.iter().filter(|_| next() % 4 == 0).cloned().collect();
    out.extend(dups);
    let mut keyed: Vec<(u64, Record)> = out.into_iter().map(|r| (next(), r)).collect();
    keyed.sort_by_key(|(k, _)| *k);
    keyed.into_iter().map(|(_, r)| r).collect()
}

fn consumer_db(state: &StreamState) -> Db {
    TABLES
        .iter()
        .map(|t| {
            (
                t.to_string(),
                state.table(t).map(|s| s.rows.clone()).unwrap_or_default(),
            )
        })
        .collect()
}

fn snapshot_of(db: &Db, at: Position, id: &str) -> Vec<Record> {
    let mut out: Vec<Record> = db
        .iter()
        .map(|(t, rows)| {
            let changes = rows
                .iter()
                .map(|(k, r)| RowChange(Op::Insert, k.clone(), r.clone()))
                .collect();
            record(
                &stream(),
                at,
                Origin::Repair,
                Body::Snapshot(SnapshotBody {
                    snapshot_id: id.into(),
                    data: table_rows(t, changes),
                }),
            )
        })
        .collect();
    let n = out.len() as u64;
    out.push(record(
        &stream(),
        at,
        Origin::Repair,
        Body::SnapshotEnd(SnapshotEndBody {
            snapshot_id: id.into(),
            scope: SnapshotScope::Stream,
            tables: TABLES
                .iter()
                .map(|t| TableGen {
                    table: t.to_string(),
                    generation: 1,
                })
                .collect(),
            records: n,
        }),
    ));
    out
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(256))]

    #[test]
    fn values_round_trip(v in prop::collection::vec(value(), 0..8)) {
        let json = serde_json::to_vec(&v).unwrap();
        let back: Vec<Value> = serde_json::from_slice(&json).unwrap();
        prop_assert_eq!(back, v);
    }

    #[test]
    fn records_round_trip(history in history(), every in 1usize..5) {
        let (_, records, _) = produce(&history, every);
        for r in records {
            prop_assert_eq!(Record::from_json(&r.to_json()).unwrap(), r);
        }
    }

    #[test]
    fn the_consumer_converges_under_any_delivery(
        history in history(),
        every in 1usize..5,
        max_bytes in 400usize..3000,
        seed in any::<u64>(),
    ) {
        let (db, records, last_mark) = produce(&history, every);
        let mut c = Consumer::new();
        c.ingest_all(deliver(records, max_bytes, seed)).unwrap();
        prop_assert_eq!(c.incomplete(), 0);
        let state = c.stream(&stream()).unwrap_or_default();
        prop_assert_eq!(consumer_db(&state), db);
        prop_assert_eq!(state.certified_head(), last_mark);
        prop_assert!(state.gaps.is_empty());
    }

    #[test]
    fn repair_restores_the_state_after_any_loss(
        history in history(),
        seed in any::<u64>(),
        keep in 0u32..4,
    ) {
        let (db, records, _) = produce(&history, 3);
        let head = records.iter().map(Record::position).max().unwrap_or_default();
        // Lose some records: drop those whose hash lands on `keep`.
        let mut s = seed | 1;
        let survivors: Vec<Record> = records
            .into_iter()
            .filter(|_| {
                s = s.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
                (s >> 33) % 4 != u64::from(keep)
            })
            .collect();
        let mut c = Consumer::new();
        c.ingest_all(survivors).unwrap();
        c.ingest_all(snapshot_of(&db, head, "repair-1")).unwrap();
        let state = c.stream(&stream()).unwrap_or_default();
        prop_assert_eq!(consumer_db(&state), db);
    }
}
