//! Synthetic export records for the SQL tests in `sqltest/`.
//!
//! Prints one JSON document: for each scenario, the stage rows to load, the
//! tombstones to write first, the rendered Dynamic Tables, and what the
//! reference consumer derives from the same records, which the SQL must
//! match. Hand-written scenarios cover each precedence rule; random ones,
//! from a fixed seed, deliver realistic histories duplicated, fragmented,
//! and with losses.
//!
//! `cargo run -q -p celld-export-snowflake --example scenarios [RANDOM_COUNT]`

use std::collections::{BTreeMap, BTreeSet};
use std::io::Write as _;

use celld_export_format::*;
use celld_export_snowflake::{DynamicTable, StageRow};
use serde_json::{json, Value as Json};

// ---------------------------------------------------------------- builder

struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        // xorshift64*
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }
    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }
    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

/// Records a scenario produced (`whole`, what the node's sink was given) and
/// what reached the warehouse (`delivered`: duplicated, fragmented, lossy).
#[derive(Default)]
struct Scenario {
    name: String,
    whole: Vec<Record>,
    delivered: Vec<Record>,
    tombstones: Vec<(StreamId, Option<u64>)>,
}

fn stream(cell: &str) -> StreamId {
    StreamId {
        script: "app".into(),
        class: "Room".into(),
        cell: cell.into(),
        facet: None,
        incarnation: 1,
    }
}

fn facet(cell: &str, path: &str, incarnation: u64) -> StreamId {
    StreamId {
        facet: Some(path.into()),
        incarnation,
        ..stream(cell)
    }
}

fn pos(epoch: u64, txid: u64, commit: u64) -> Position {
    Position::new(epoch, txid, commit)
}

fn record(stream: &StreamId, at: Position, origin: Origin, body: Body) -> Record {
    Record {
        envelope: Envelope {
            stream: stream.clone(),
            cell_name: Some(format!("name-{}", stream.cell)),
            position: at,
            committed_at: 1_790_000_000_000 + (at.txid * 1000 + at.commit) as i64,
            node: "node-a".into(),
            origin,
            fragment: 1,
            fragments: 1,
        },
        body,
    }
}

fn col(name: &str, ty: &str, pk: u32) -> ColumnDef {
    ColumnDef {
        name: name.into(),
        decl_type: ty.into(),
        pk,
        not_null: false,
        generated: false,
    }
}

fn schema(table: &str, generation: u64, columns: Vec<ColumnDef>) -> Body {
    Body::Schema(SchemaBody {
        table: table.into(),
        generation,
        sql: format!("CREATE TABLE {table} (...)"),
        columns,
        dropped: false,
        renamed_from: None,
        unsupported: false,
    })
}

fn table_rows(
    table: &str,
    generation: u64,
    columns: &[&str],
    key_columns: &[&str],
    rows: Vec<RowChange>,
) -> TableRows {
    TableRows {
        table: table.into(),
        generation,
        columns: columns.iter().map(|c| c.to_string()).collect(),
        key_columns: key_columns.iter().map(|c| c.to_string()).collect(),
        rows,
    }
}

fn rows(data: TableRows) -> Body {
    Body::Rows(RowsBody { data })
}

fn snapshot(id: &str, data: TableRows) -> Body {
    Body::Snapshot(SnapshotBody {
        snapshot_id: id.into(),
        data,
    })
}

fn snapshot_end(id: &str, scope: SnapshotScope, tables: Vec<(&str, u64)>, records: u64) -> Body {
    Body::SnapshotEnd(SnapshotEndBody {
        snapshot_id: id.into(),
        scope,
        tables: tables
            .into_iter()
            .map(|(t, g)| TableGen {
                table: t.into(),
                generation: g,
            })
            .collect(),
        records,
    })
}

fn i(v: i64) -> Value {
    Value::Integer(v)
}
fn t(v: &str) -> Value {
    Value::Text(v.into())
}
fn ins(key: Vec<Value>, row: Vec<Value>) -> RowChange {
    RowChange(Op::Insert, key, row)
}
fn upd(key: Vec<Value>, row: Vec<Value>) -> RowChange {
    RowChange(Op::Update, key, row)
}
fn del(key: Vec<Value>, row: Vec<Value>) -> RowChange {
    RowChange(Op::Delete, key, row)
}

impl Scenario {
    fn new(name: &str) -> Self {
        Self {
            name: name.into(),
            ..Self::default()
        }
    }

    fn emit(&mut self, r: Record) {
        self.whole.push(r.clone());
        self.delivered.push(r);
    }

    fn emit_twice(&mut self, r: Record) {
        self.whole.push(r.clone());
        self.delivered.push(r.clone());
        self.delivered.push(r);
    }

    /// Produced but never delivered.
    fn lose(&mut self, r: Record) {
        self.whole.push(r);
    }

    /// Delivered in fragments of at most `max_bytes`; `skip` names a
    /// fragment (from one) that never arrives.
    fn emit_split(&mut self, r: Record, max_bytes: usize, skip: Option<u32>) -> u32 {
        self.whole.push(r.clone());
        match split(r, max_bytes) {
            Split::Fragments(fs) => {
                let n = fs.len() as u32;
                self.delivered
                    .extend(fs.into_iter().filter(|f| Some(f.envelope.fragment) != skip));
                n
            }
            Split::Bulk(b) => {
                self.delivered.push(*b);
                1
            }
        }
    }

    /// A watermark over `(from, through]` counting what the node produced
    /// for `stream`; it certifies only if all of that was delivered.
    fn watermark(&mut self, s: &StreamId, from: Option<Position>, through: Position) {
        let lo = from.unwrap_or(pos(through.epoch, 0, 0));
        let in_range: Vec<Position> = self
            .whole
            .iter()
            .filter(|r| &r.envelope.stream == s)
            .filter(|r| r.envelope.origin != Origin::Repair)
            .filter(|r| !matches!(r.body, Body::Watermark(_)))
            .map(|r| r.position())
            .filter(|p| *p <= through && (*p > lo || (from.is_none() && *p == lo)))
            .collect();
        let commits: BTreeSet<Position> = in_range.iter().copied().collect();
        let body = Body::Watermark(WatermarkBody {
            from,
            through,
            commits: commits.len() as u64,
            records: in_range.len() as u64,
        });
        let r = record(s, through, Origin::Live, body);
        self.delivered.push(r.clone());
        self.whole.push(r);
    }
}

// ---------------------------------------------------------------- scenarios

const ITEMS: [&str; 5] = ["id", "name", "price", "data", "extra"];

fn items_schema() -> Body {
    schema(
        "items",
        1,
        vec![
            col("id", "INTEGER", 1),
            col("name", "TEXT", 0),
            col("price", "REAL", 0),
            col("data", "BLOB", 0),
            col("extra", "", 0),
        ],
    )
}

fn items(rows: Vec<RowChange>) -> Body {
    self::rows(table_rows("items", 1, &ITEMS, &["id"], rows))
}

fn item(id: i64, name: &str, price: Value, data: Value, extra: Value) -> Vec<Value> {
    vec![i(id), t(name), price, data, extra]
}

/// Keyed and rowid tables, every value encoding, updates and deletes.
fn basic() -> Scenario {
    let mut s = Scenario::new("basic");
    let a = stream("r1");
    s.emit(record(&a, pos(1, 1, 1), Origin::Live, items_schema()));
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        items(vec![
            ins(
                vec![i(1)],
                item(
                    1,
                    "apple",
                    Value::Real(1.5),
                    Value::Blob(vec![0, 1, 255]),
                    Value::Null,
                ),
            ),
            ins(
                vec![i(2)],
                item(
                    2,
                    "it's \"quoted\" \\ ünï",
                    Value::Real(f64::INFINITY),
                    Value::Null,
                    i(7),
                ),
            ),
            ins(
                vec![i(3)],
                item(
                    3,
                    "gone",
                    Value::Real(f64::NEG_INFINITY),
                    Value::Null,
                    t("x"),
                ),
            ),
        ]),
    ));
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        schema("log", 1, vec![col("msg", "TEXT", 0)]),
    ));
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        rows(table_rows(
            "log",
            1,
            &["msg"],
            &[ROWID_KEY_COLUMN],
            vec![
                ins(vec![i(1)], vec![t("first")]),
                ins(vec![i(2)], vec![t("second")]),
            ],
        )),
    ));
    s.emit(record(
        &a,
        pos(1, 2, 2),
        Origin::Live,
        items(vec![
            upd(
                vec![i(1)],
                item(
                    1,
                    "apple",
                    Value::Real(-0.25),
                    Value::Blob(vec![]),
                    Value::Real(2.0),
                ),
            ),
            del(
                vec![i(3)],
                item(
                    3,
                    "gone",
                    Value::Real(f64::NEG_INFINITY),
                    Value::Null,
                    t("x"),
                ),
            ),
        ]),
    ));
    // A rowid change on a rowid table: a delete and an insert.
    s.emit(record(
        &a,
        pos(1, 2, 3),
        Origin::Live,
        rows(table_rows(
            "log",
            1,
            &["msg"],
            &[ROWID_KEY_COLUMN],
            vec![
                del(vec![i(2)], vec![t("second")]),
                ins(vec![i(9)], vec![t("second")]),
            ],
        )),
    ));
    s.watermark(&a, None, pos(1, 2, 2));
    s.watermark(&a, Some(pos(1, 2, 2)), pos(1, 2, 3));
    s
}

/// Duplicates, fragments delivered more than once, and a record with a
/// missing fragment, which must not apply.
fn fragments() -> Scenario {
    let mut s = Scenario::new("fragments");
    let a = stream("r1");
    s.emit(record(&a, pos(1, 1, 1), Origin::Live, items_schema()));
    let big: Vec<RowChange> = (1..=12)
        .map(|n| {
            ins(
                vec![i(n)],
                item(
                    n,
                    &format!("row number {n}"),
                    Value::Real(n as f64),
                    Value::Null,
                    Value::Null,
                ),
            )
        })
        .collect();
    let n = s.emit_split(
        record(&a, pos(1, 1, 1), Origin::Live, items(big)),
        520,
        None,
    );
    assert!(n > 2, "the record must split");
    // Every fragment again.
    let again: Vec<Record> = s.delivered[1..].to_vec();
    s.delivered.extend(again);
    let later: Vec<RowChange> = (20..=30)
        .map(|n| {
            ins(
                vec![i(n)],
                item(
                    n,
                    &format!("never whole {n}"),
                    Value::Null,
                    Value::Null,
                    Value::Null,
                ),
            )
        })
        .collect();
    s.emit_split(
        record(&a, pos(1, 2, 2), Origin::Live, items(later)),
        520,
        Some(2),
    );
    s.emit_twice(record(
        &a,
        pos(1, 3, 3),
        Origin::Live,
        items(vec![upd(
            vec![i(4)],
            item(4, "updated", Value::Null, Value::Null, Value::Null),
        )]),
    ));
    // The missing fragment keeps this from certifying.
    s.watermark(&a, None, pos(1, 3, 3));
    s
}

/// A complete stream snapshot supersedes rows at or below it, including rows
/// it lacks; live rows above it apply; an incomplete snapshot is ignored; a
/// table-scoped snapshot opens a new generation.
fn snapshots() -> Scenario {
    let mut s = Scenario::new("snapshots");
    let a = stream("r1");
    s.emit(record(&a, pos(1, 1, 1), Origin::Live, items_schema()));
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        items(
            (1..=4)
                .map(|n| {
                    ins(
                        vec![i(n)],
                        item(n, "live", Value::Null, Value::Null, Value::Null),
                    )
                })
                .collect(),
        ),
    ));
    s.emit(record(
        &a,
        pos(1, 3, 2),
        Origin::Live,
        items(vec![
            upd(
                vec![i(1)],
                item(1, "at the cut", Value::Null, Value::Null, Value::Null),
            ),
            ins(
                vec![i(9)],
                item(9, "at the cut", Value::Null, Value::Null, Value::Null),
            ),
        ]),
    ));
    // Repair at (1,3,2): rows 1 and 2 only; 3 and 4 are deleted by it, and
    // the live update at the same position loses to the repair.
    let at = pos(1, 3, 2);
    s.emit(record(&a, at, Origin::Repair, items_schema()));
    s.emit(record(
        &a,
        at,
        Origin::Repair,
        snapshot(
            "rep-1",
            table_rows(
                "items",
                1,
                &ITEMS,
                &["id"],
                vec![
                    ins(
                        vec![i(1)],
                        item(1, "repaired", Value::Null, Value::Null, Value::Null),
                    ),
                    ins(
                        vec![i(2)],
                        item(2, "repaired", Value::Null, Value::Null, Value::Null),
                    ),
                ],
            ),
        ),
    ));
    s.emit(record(
        &a,
        at,
        Origin::Repair,
        snapshot_end("rep-1", SnapshotScope::Stream, vec![("items", 1)], 1),
    ));
    // Above the cut.
    s.emit(record(
        &a,
        pos(1, 4, 3),
        Origin::Live,
        items(vec![ins(
            vec![i(5)],
            item(5, "after", Value::Null, Value::Null, Value::Null),
        )]),
    ));
    // Incomplete: claims two records, delivers one.
    s.emit(record(
        &a,
        pos(1, 5, 4),
        Origin::Repair,
        snapshot("rep-2", table_rows("items", 1, &ITEMS, &["id"], vec![])),
    ));
    s.emit(record(
        &a,
        pos(1, 5, 4),
        Origin::Repair,
        snapshot_end("rep-2", SnapshotScope::Stream, vec![("items", 1)], 2),
    ));
    // A DDL: generation 2 of `notes` with its inline table snapshot.
    s.emit(record(
        &a,
        pos(1, 2, 1),
        Origin::Live,
        schema("notes", 1, vec![col("k", "TEXT", 1), col("v", "", 0)]),
    ));
    s.emit(record(
        &a,
        pos(1, 2, 1),
        Origin::Live,
        rows(table_rows(
            "notes",
            1,
            &["k", "v"],
            &["k"],
            vec![ins(vec![t("a")], vec![t("a"), i(1)])],
        )),
    ));
    let ddl = pos(1, 6, 5);
    s.emit(record(
        &a,
        ddl,
        Origin::Live,
        schema(
            "notes",
            2,
            vec![col("k", "TEXT", 1), col("v", "", 0), col("w", "INTEGER", 0)],
        ),
    ));
    s.emit(record(
        &a,
        ddl,
        Origin::Snapshot,
        snapshot(
            "ddl-5",
            table_rows(
                "notes",
                2,
                &["k", "v", "w"],
                &["k"],
                vec![ins(vec![t("a")], vec![t("a"), i(1), Value::Null])],
            ),
        ),
    ));
    s.emit(record(
        &a,
        ddl,
        Origin::Snapshot,
        snapshot_end("ddl-5", SnapshotScope::Tables, vec![("notes", 2)], 1),
    ));
    s.emit(record(
        &a,
        pos(1, 7, 6),
        Origin::Live,
        rows(table_rows(
            "notes",
            2,
            &["k", "v", "w"],
            &["k"],
            vec![ins(vec![t("b")], vec![t("b"), i(2), i(3)])],
        )),
    ));
    s
}

/// Closed generations: dropped, renamed away, superseded.
fn generations() -> Scenario {
    let mut s = Scenario::new("generations");
    let a = stream("r1");
    let one = |k: &str| {
        rows(table_rows(
            "t",
            1,
            &["k"],
            &["k"],
            vec![ins(vec![t(k)], vec![t(k)])],
        ))
    };
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        schema("t", 1, vec![col("k", "TEXT", 1)]),
    ));
    s.emit(record(&a, pos(1, 1, 1), Origin::Live, one("x")));
    let mut dropped = schema("t", 1, vec![col("k", "TEXT", 1)]);
    if let Body::Schema(b) = &mut dropped {
        b.dropped = true;
    }
    s.emit(record(&a, pos(1, 2, 2), Origin::Live, dropped));
    // u renamed to v.
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        schema("u", 1, vec![col("k", "TEXT", 1)]),
    ));
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        rows(table_rows(
            "u",
            1,
            &["k"],
            &["k"],
            vec![ins(vec![t("y")], vec![t("y")])],
        )),
    ));
    let mut renamed = schema("v", 1, vec![col("k", "TEXT", 1)]);
    if let Body::Schema(b) = &mut renamed {
        b.renamed_from = Some("u".into());
    }
    s.emit(record(&a, pos(1, 3, 3), Origin::Live, renamed));
    s.emit(record(
        &a,
        pos(1, 3, 3),
        Origin::Snapshot,
        snapshot(
            "ren",
            table_rows(
                "v",
                1,
                &["k"],
                &["k"],
                vec![ins(vec![t("y")], vec![t("y")])],
            ),
        ),
    ));
    s.emit(record(
        &a,
        pos(1, 3, 3),
        Origin::Snapshot,
        snapshot_end("ren", SnapshotScope::Tables, vec![("v", 1)], 1),
    ));
    // z created and renamed away at one position.
    s.emit(record(
        &a,
        pos(1, 6, 6),
        Origin::Live,
        schema("z", 1, vec![col("k", "TEXT", 1)]),
    ));
    s.emit(record(
        &a,
        pos(1, 6, 6),
        Origin::Live,
        rows(table_rows(
            "z",
            1,
            &["k"],
            &["k"],
            vec![ins(vec![t("q")], vec![t("q")])],
        )),
    ));
    let mut renamed = schema("zz", 1, vec![col("k", "TEXT", 1)]);
    if let Body::Schema(b) = &mut renamed {
        b.renamed_from = Some("z".into());
    }
    s.emit(record(&a, pos(1, 6, 6), Origin::Live, renamed));
    // w generation 1 superseded by generation 2, which has no rows yet.
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        schema("w", 1, vec![col("k", "INTEGER", 1)]),
    ));
    s.emit(record(
        &a,
        pos(1, 1, 1),
        Origin::Live,
        rows(table_rows(
            "w",
            1,
            &["k"],
            &["k"],
            vec![ins(vec![i(1)], vec![i(1)])],
        )),
    ));
    s.emit(record(
        &a,
        pos(1, 4, 4),
        Origin::Live,
        schema("w", 2, vec![col("k", "INTEGER", 1)]),
    ));
    s.emit(record(
        &a,
        pos(1, 5, 5),
        Origin::Live,
        rows(table_rows(
            "w",
            2,
            &["k"],
            &["k"],
            vec![ins(vec![i(2)], vec![i(2)])],
        )),
    ));
    s
}

/// A stream `deleted` at a position, and a facet subtree deleted from the
/// root's stream.
fn deletions() -> Scenario {
    let mut s = Scenario::new("deletions");
    let put = |s: &mut Scenario, st: &StreamId, at: Position, id: i64| {
        s.emit(record(st, at, Origin::Live, items_schema()));
        s.emit(record(
            st,
            at,
            Origin::Live,
            items(vec![ins(
                vec![i(id)],
                item(id, "x", Value::Null, Value::Null, Value::Null),
            )]),
        ));
    };
    let root = stream("r1");
    put(&mut s, &root, pos(1, 1, 1), 1);
    s.emit(record(
        &root,
        pos(1, 2, 2),
        Origin::Live,
        Body::Deleted(DeletedBody {
            facet: None,
            incarnation: None,
            subtree: false,
        }),
    ));
    put(&mut s, &root, pos(1, 3, 3), 2);
    let f = facet("r1", "f", 70);
    let fb = facet("r1", "f/b", 71);
    let fbc = facet("r1", "f/b/c", 72);
    let fother = facet("r1", "f", 69);
    let fsibling = facet("r1", "fx", 73);
    let elsewhere = facet("r2", "f", 70);
    for (n, st) in [&f, &fb, &fbc, &fother, &fsibling, &elsewhere]
        .into_iter()
        .enumerate()
    {
        put(&mut s, st, pos(1, 1, 1), 10 + n as i64);
    }
    s.emit(record(
        &root,
        pos(1, 4, 4),
        Origin::Live,
        Body::Deleted(DeletedBody {
            facet: Some("f".into()),
            incarnation: Some(70),
            subtree: true,
        }),
    ));
    s
}

/// Watermark chains across epochs, a count mismatch that stops a chain, gap,
/// link, and recovered records, one of them covered by a snapshot, and bulk.
fn certification() -> Scenario {
    let mut s = Scenario::new("certification");
    let a = stream("r1");
    let put = |s: &mut Scenario, at: Position, id: i64| {
        s.emit(record(
            &a,
            at,
            Origin::Live,
            items(vec![ins(
                vec![i(id)],
                item(id, "x", Value::Null, Value::Null, Value::Null),
            )]),
        ));
    };
    s.emit(record(&a, pos(1, 1, 1), Origin::Live, items_schema()));
    put(&mut s, pos(1, 1, 1), 1);
    put(&mut s, pos(1, 1, 2), 2);
    s.watermark(&a, None, pos(1, 1, 2));
    put(&mut s, pos(1, 2, 3), 3);
    s.watermark(&a, Some(pos(1, 1, 2)), pos(1, 2, 3));
    s.lose(record(
        &a,
        pos(1, 3, 4),
        Origin::Live,
        items(vec![ins(
            vec![i(4)],
            item(4, "lost", Value::Null, Value::Null, Value::Null),
        )]),
    ));
    s.watermark(&a, Some(pos(1, 2, 3)), pos(1, 3, 4));
    put(&mut s, pos(1, 4, 5), 5);
    s.watermark(&a, Some(pos(1, 3, 4)), pos(1, 4, 5));
    // Epoch 2 links back past the certified position.
    s.emit(record(
        &a,
        pos(2, 1, 1),
        Origin::Live,
        Body::Link(LinkBody {
            start_txid: 1,
            prev_epoch: Some(1),
            prev_txid: Some(4),
            mode: LinkMode::Paged,
        }),
    ));
    put(&mut s, pos(2, 1, 1), 6);
    s.watermark(&a, None, pos(2, 1, 1));
    s.emit(record(
        &a,
        pos(2, 2, 2),
        Origin::Live,
        Body::Gap(GapBody {
            from: pos(2, 1, 1),
            to: pos(2, 2, 2),
            reason: "queue_overflow".into(),
        }),
    ));
    s.emit(record(
        &a,
        pos(2, 2, 2),
        Origin::Live,
        Body::Recovered(RecoveredBody {
            session: "s-1".into(),
            head: pos(2, 1, 1),
        }),
    ));
    s.emit(record(
        &a,
        pos(2, 3, 3),
        Origin::Live,
        Body::Recovered(RecoveredBody {
            session: "s-2".into(),
            head: pos(2, 3, 3),
        }),
    ));
    s.emit(record(
        &a,
        pos(2, 4, 4),
        Origin::Live,
        Body::Bulk(BulkBody {
            tables: vec![TableGen {
                table: "items".into(),
                generation: 1,
            }],
        }),
    ));
    // A second stream whose link is covered by a stream snapshot.
    let b = stream("r2");
    s.emit(record(
        &b,
        pos(3, 1, 1),
        Origin::Live,
        Body::Link(LinkBody {
            start_txid: 1,
            prev_epoch: Some(2),
            prev_txid: Some(9),
            mode: LinkMode::Clone,
        }),
    ));
    s.emit(record(
        &b,
        pos(3, 1, 1),
        Origin::Live,
        Body::Gap(GapBody {
            from: pos(2, 1, 1),
            to: pos(2, 9, 9),
            reason: "lost".into(),
        }),
    ));
    s.emit(record(
        &b,
        pos(3, 2, 2),
        Origin::Live,
        Body::Bulk(BulkBody {
            tables: vec![TableGen {
                table: "items".into(),
                generation: 1,
            }],
        }),
    ));
    s.emit(record(&b, pos(3, 5, 5), Origin::Repair, items_schema()));
    s.emit(record(
        &b,
        pos(3, 5, 5),
        Origin::Repair,
        snapshot(
            "heal",
            table_rows(
                "items",
                1,
                &ITEMS,
                &["id"],
                vec![ins(
                    vec![i(1)],
                    item(1, "healed", Value::Null, Value::Null, Value::Null),
                )],
            ),
        ),
    ));
    s.emit(record(
        &b,
        pos(3, 5, 5),
        Origin::Repair,
        snapshot_end("heal", SnapshotScope::Stream, vec![("items", 1)], 1),
    ));
    s
}

/// A tombstoned stream is dropped at routing and hidden, whatever
/// incarnation; a cleared tombstone does nothing.
fn tombstones() -> Scenario {
    let mut s = Scenario::new("tombstones");
    for (cell, inc) in [("r1", 1), ("r1", 2), ("r2", 1), ("r3", 1)] {
        let st = StreamId {
            incarnation: inc,
            ..stream(cell)
        };
        s.emit(record(&st, pos(1, 1, 1), Origin::Live, items_schema()));
        s.emit(record(
            &st,
            pos(1, 1, 1),
            Origin::Live,
            items(vec![ins(
                vec![i(1)],
                item(1, cell, Value::Null, Value::Null, Value::Null),
            )]),
        ));
    }
    s.tombstones.push((stream("r1"), None));
    s.tombstones.push((stream("r2"), Some(5)));
    s
}

// ---------------------------------------------------------------- random

type Model = BTreeMap<Vec<Value>, Vec<Value>>;

struct Table {
    name: &'static str,
    generation: u64,
    columns: &'static [&'static str],
    key_columns: &'static [&'static str],
    rows: Model,
}

impl Table {
    fn schema(&self) -> Body {
        let cols = if self.key_columns == [ROWID_KEY_COLUMN] {
            vec![col("v", "TEXT", 0)]
        } else {
            vec![col("id", "INTEGER", 1), col("v", "", 0)]
        };
        schema(self.name, self.generation, cols)
    }
    fn data(&self, rows: Vec<RowChange>) -> TableRows {
        table_rows(
            self.name,
            self.generation,
            self.columns,
            self.key_columns,
            rows,
        )
    }
}

fn random_value(rng: &mut Rng) -> Value {
    match rng.below(7) {
        0 => Value::Null,
        1 | 2 => i(rng.next() as i64 >> rng.below(60)),
        3 => Value::Real(match rng.below(5) {
            0 => f64::INFINITY,
            1 => f64::NEG_INFINITY,
            _ => (rng.below(20_000) as f64 - 10_000.0) / 8.0,
        }),
        4 | 5 => t(&"xyz'\"\\é"
            .chars()
            .take(rng.below(8) as usize)
            .collect::<String>()),
        _ => Value::Blob((0..rng.below(6)).map(|_| rng.below(256) as u8).collect()),
    }
}

fn random(seed: u64) -> Scenario {
    let mut rng = Rng(seed.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1);
    let mut s = Scenario::new(&format!("random_{seed}"));
    let streams = [stream("r1"), stream("r2"), facet("r1", "f", 77)];
    for st in &streams {
        let mut tables = vec![
            Table {
                name: "a",
                generation: 1,
                columns: &["id", "v"],
                key_columns: &["id"],
                rows: Model::new(),
            },
            Table {
                name: "b",
                generation: 1,
                columns: &["v"],
                key_columns: &[ROWID_KEY_COLUMN],
                rows: Model::new(),
            },
        ];
        let (mut epoch, mut txid, mut commit) = (1u64, 1u64, 0u64);
        let mut last_mark: Option<Position> = None;
        let mut last: Option<Position> = None;
        let first = pos(1, 1, 1);
        for tb in &tables {
            s.emit(record(st, first, Origin::Live, tb.schema()));
        }
        for _ in 0..(8 + rng.below(12)) {
            if rng.chance(6) {
                // A new epoch, linked to the last position.
                let prev = last.unwrap_or(first);
                epoch += 1;
                txid = 1;
                commit = 0;
                last_mark = None;
                s.emit(record(
                    st,
                    pos(epoch, 1, 1),
                    Origin::Live,
                    Body::Link(LinkBody {
                        start_txid: 1,
                        prev_epoch: Some(prev.epoch),
                        prev_txid: Some(prev.txid),
                        mode: LinkMode::Paged,
                    }),
                ));
            }
            commit += 1;
            txid += rng.below(2);
            let at = pos(epoch, txid, commit);
            last = Some(at);

            if rng.chance(7) {
                // DDL on one table: a new generation, with its inline snapshot.
                let tb = &mut tables[rng.below(2) as usize];
                tb.generation += 1;
                let dropped = rng.chance(40);
                if dropped {
                    tb.rows.clear();
                }
                s.emit(record(st, at, Origin::Live, tb.schema()));
                let id = format!("ddl-{}-{}", st.cell, commit);
                let snap: Vec<RowChange> = tb
                    .rows
                    .iter()
                    .map(|(k, r)| ins(k.clone(), r.clone()))
                    .collect();
                s.emit(record(
                    st,
                    at,
                    Origin::Snapshot,
                    snapshot(&id, tb.data(snap)),
                ));
                s.emit(record(
                    st,
                    at,
                    Origin::Snapshot,
                    snapshot_end(
                        &id,
                        SnapshotScope::Tables,
                        vec![(tb.name, tb.generation)],
                        1,
                    ),
                ));
                continue;
            }

            let mut changes: [BTreeMap<Vec<Value>, RowChange>; 2] = Default::default();
            for _ in 0..(1 + rng.below(5)) {
                let ti = rng.below(2) as usize;
                let tb = &mut tables[ti];
                let key = vec![i(rng.below(8) as i64)];
                let before = tb.rows.get(&key).cloned();
                let removable = before.clone().filter(|_| rng.chance(35));
                let change = if let Some(before) = removable {
                    tb.rows.remove(&key);
                    // The net change against the commit's starting state.
                    match changes[ti].remove(&key) {
                        Some(RowChange(Op::Insert, ..)) => continue,
                        Some(RowChange(_, _, _)) | None => del(key.clone(), before),
                    }
                } else {
                    let row = if ti == 0 {
                        vec![key[0].clone(), random_value(&mut rng)]
                    } else {
                        vec![random_value(&mut rng)]
                    };
                    tb.rows.insert(key.clone(), row.clone());
                    match (changes[ti].remove(&key), before) {
                        (Some(RowChange(Op::Insert, ..)), _) | (None, None) => {
                            ins(key.clone(), row)
                        }
                        (Some(RowChange(Op::Delete, ..)), _) => upd(key.clone(), row),
                        _ => upd(key.clone(), row),
                    }
                };
                changes[ti].insert(key, change);
            }
            for (ti, ch) in changes.into_iter().enumerate() {
                if ch.is_empty() {
                    continue;
                }
                let tb = &tables[ti];
                let r = record(
                    st,
                    at,
                    Origin::Live,
                    rows(tb.data(ch.into_values().collect())),
                );
                match rng.below(100) {
                    0..=4 => {
                        let tg = TableGen {
                            table: tb.name.into(),
                            generation: tb.generation,
                        };
                        s.emit(record(
                            st,
                            at,
                            Origin::Live,
                            Body::Bulk(BulkBody { tables: vec![tg] }),
                        ));
                    }
                    5..=9 => s.lose(r),
                    10..=19 => s.emit_twice(r),
                    20..=34 => {
                        let skip = rng.chance(20).then_some(2);
                        s.emit_split(r, 420, skip);
                    }
                    _ => s.emit(r),
                }
            }
            if rng.chance(30) {
                s.watermark(st, last_mark, at);
                last_mark = Some(at);
            }
        }
        let head = last.unwrap_or(first);
        if rng.chance(15) {
            s.emit(record(
                st,
                head,
                Origin::Live,
                Body::Gap(GapBody {
                    from: first,
                    to: head,
                    reason: "queue_overflow".into(),
                }),
            ));
        }
        if rng.chance(15) {
            s.emit(record(
                st,
                head,
                Origin::Live,
                Body::Recovered(RecoveredBody {
                    session: "dead".into(),
                    head,
                }),
            ));
        }
        if rng.chance(35) {
            // Repair at the head: every table, the whole stream.
            let id = format!("repair-{}", st.cell);
            for tb in &tables {
                s.emit(record(st, head, Origin::Repair, tb.schema()));
                let snap: Vec<RowChange> = tb
                    .rows
                    .iter()
                    .map(|(k, r)| ins(k.clone(), r.clone()))
                    .collect();
                let r = record(st, head, Origin::Repair, snapshot(&id, tb.data(snap)));
                if rng.chance(30) {
                    s.emit_split(r, 420, None);
                } else {
                    s.emit(r);
                }
            }
            let covered = tables.iter().map(|tb| (tb.name, tb.generation)).collect();
            let end = snapshot_end(&id, SnapshotScope::Stream, covered, tables.len() as u64);
            let end = record(st, head, Origin::Repair, end);
            if rng.chance(10) {
                s.lose(end);
            } else {
                s.emit(end);
            }
        }
        if st.facet.is_none() && rng.chance(10) {
            let at = pos(epoch, txid, commit + 1);
            s.emit(record(
                st,
                at,
                Origin::Live,
                Body::Deleted(DeletedBody {
                    facet: Some("f".into()),
                    incarnation: Some(77),
                    subtree: false,
                }),
            ));
        }
    }
    // Arrival order means nothing; shuffle anyway.
    for n in (1..s.delivered.len()).rev() {
        let j = rng.below(n as u64 + 1) as usize;
        s.delivered.swap(n, j);
    }
    s
}

// ---------------------------------------------------------------- output

fn position_key(p: Position) -> String {
    format!("{:020}.{:020}.{:020}", p.epoch, p.txid, p.commit)
}

fn stream_json(s: &StreamId) -> Json {
    json!({
        "script": s.script,
        "class": s.class,
        "cell": s.cell,
        "facet": s.facet.clone().unwrap_or_default(),
        "incarnation": s.incarnation,
    })
}

fn tombstoned(s: &StreamId, tombstones: &[(StreamId, Option<u64>)]) -> bool {
    tombstones.iter().any(|(t, inc)| {
        t.script == s.script
            && t.class == s.class
            && t.cell == s.cell
            && t.facet == s.facet
            && inc.is_none_or(|i| i == s.incarnation)
    })
}

fn expected(s: &Scenario) -> Json {
    let mut consumer = Consumer::new();
    consumer
        .ingest_all(
            s.delivered
                .iter()
                .filter(|r| !tombstoned(r.stream(), &s.tombstones))
                .cloned(),
        )
        .expect("well-formed fragments");
    let streams: Vec<Json> = consumer
        .state()
        .iter()
        .map(|(id, st)| {
            let tables: Vec<Json> = st
                .tables
                .iter()
                .map(|(tg, t)| {
                    json!({
                        "table": tg.table,
                        "generation": tg.generation,
                        "columns": t.columns,
                        "rows": t.rows.iter().map(|(k, r)| json!({"key": k, "row": r})).collect::<Vec<_>>(),
                    })
                })
                .collect();
            let gaps: Vec<Json> = st
                .gaps
                .iter()
                .map(|g| match g {
                    Gap::Reported { to, .. } => json!(["gap", to.epoch, to.txid]),
                    Gap::Link { prev_epoch, prev_txid, .. } => json!(["link", prev_epoch, prev_txid]),
                    Gap::Recovered { head, .. } => json!(["recovered", head.epoch, head.txid]),
                })
                .collect();
            json!({
                "stream": stream_json(id),
                "deleted_at": st.deleted_at.map(position_key),
                "tables": tables,
                "uncertain": st.uncertain.iter().map(|tg| json!([tg.table, tg.generation])).collect::<Vec<_>>(),
                "certified": st.certified.values().map(|p| position_key(*p)).collect::<Vec<_>>(),
                "gaps": gaps,
            })
        })
        .collect();
    json!({ "streams": streams })
}

fn dynamic_tables(s: &Scenario) -> Vec<Json> {
    let mut schemas: BTreeMap<(String, String, String), Vec<SchemaBody>> = BTreeMap::new();
    for r in &s.whole {
        if let Body::Schema(b) = &r.body {
            let k = (
                r.stream().script.clone(),
                r.stream().class.clone(),
                b.table.clone(),
            );
            schemas.entry(k).or_default().push(b.clone());
        }
    }
    schemas
        .iter()
        .enumerate()
        .filter_map(|(n, ((script, class, table), bodies))| {
            let dt = DynamicTable {
                name: format!("DT_{n}"),
                target_lag: "1 minute".into(),
                warehouse: "EXPORT_WH".into(),
                script: script.clone(),
                class: class.clone(),
                table: table.clone(),
            };
            // A table only ever dropped has no columns to project.
            let sql = dt.render(bodies).ok()?;
            Some(json!({
                "name": dt.name,
                "script": script,
                "class": class,
                "table": table,
                "columns": dt.projection(bodies).iter().map(|c| json!([c.name, format!("{:?}", c.ty)])).collect::<Vec<_>>(),
                "sql": sql,
            }))
        })
        .collect()
}

fn main() {
    let random_count: u64 = std::env::args()
        .nth(1)
        .map(|a| a.parse().expect("RANDOM_COUNT is a number"))
        .unwrap_or(16);
    let mut all = vec![
        basic(),
        fragments(),
        snapshots(),
        generations(),
        deletions(),
        certification(),
        tombstones(),
    ];
    all.extend((1..=random_count).map(random));
    let out: Vec<Json> = all
        .iter()
        .map(|s| {
            json!({
                "name": s.name,
                "stage_rows": s.delivered.iter().map(StageRow::from_record).collect::<Vec<_>>(),
                "tombstones": s.tombstones.iter().map(|(id, inc)| {
                    let mut j = stream_json(id);
                    j["incarnation"] = json!(inc);
                    j
                }).collect::<Vec<_>>(),
                "dynamic_tables": dynamic_tables(s),
                "expected": expected(s),
            })
        })
        .collect();
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, &json!({ "scenarios": out })).expect("stdout");
    stdout.write_all(b"\n").expect("stdout");
}
