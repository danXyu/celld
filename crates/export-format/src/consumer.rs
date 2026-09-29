//! The reference consumer: what any consumer of the export must converge to.
//!
//! It keeps every whole record it has been given and derives state from the
//! set, never from arrival order, because the stream is at-least-once and
//! unordered across partitions. The derivation follows the design's
//! precedence rules and the Dynamic Table definition:
//!
//! - rows of a table generation apply in position order, newest per key;
//! - a complete snapshot at `P` supersedes every record of its scope at or
//!   below `P`, including rows it does not contain; at an equal position the
//!   snapshot wins over `live`;
//! - a closed generation (dropped, renamed away, or followed by a higher
//!   generation of the same name) has no rows;
//! - a `deleted` record removes its stream's state at or below its position,
//!   or, when it names a facet, that facet stream;
//! - a `bulk` table is uncertain until a snapshot at or after it covers it;
//! - watermarks certify a range only when their counts match what is held,
//!   and links, `recovered` records, and `gap` records beyond the certified
//!   position are gaps until a snapshot covers them.
//!
//! Later pieces use it as the test oracle: apply the exported stream, compare
//! with the cell.

use std::collections::{BTreeMap, BTreeSet};

use crate::dedup::DedupKey;
use crate::fragment::{ReassembleError, Reassembler};
use crate::record::{
    Body, DeletedBody, Op, Origin, Position, Record, SnapshotEndBody, SnapshotScope, StreamId,
    TableGen,
};
use crate::value::Value;

/// Applies export records. See the module docs.
#[derive(Default, Debug)]
pub struct Consumer {
    seen: BTreeSet<DedupKey>,
    reassembler: Reassembler,
    records: BTreeMap<StreamId, Vec<Record>>,
}

/// Why the state has a hole the consumer cannot fill from the stream.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Gap {
    /// A `gap` record.
    Reported {
        from: Position,
        to: Position,
        reason: String,
    },
    /// A link whose predecessor lies beyond what was certified in its epoch.
    Link {
        prev_epoch: u64,
        prev_txid: u64,
        certified: Option<Position>,
    },
    /// A `recovered` head beyond what was certified in its epoch.
    Recovered {
        head: Position,
        certified: Option<Position>,
    },
}

/// One table generation's current rows.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TableState {
    pub columns: Vec<String>,
    pub key_columns: Vec<String>,
    /// Key values to the full row, in `columns` order.
    pub rows: BTreeMap<Vec<Value>, Vec<Value>>,
}

/// One stream's derived state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StreamState {
    /// Open table generations that hold rows.
    pub tables: BTreeMap<TableGen, TableState>,
    /// Tables a `bulk` record made unknown and no later snapshot covered.
    pub uncertain: BTreeSet<TableGen>,
    /// Per epoch, the highest position certified by a chain of matching
    /// watermarks.
    pub certified: BTreeMap<u64, Position>,
    /// Holes no snapshot has covered yet.
    pub gaps: Vec<Gap>,
    /// Set when a `deleted` record removed the stream at this position.
    pub deleted_at: Option<Position>,
}

impl StreamState {
    /// The rows of the newest open generation of `table`.
    pub fn table(&self, table: &str) -> Option<&TableState> {
        self.tables
            .iter()
            .rev()
            .find(|(tg, _)| tg.table == table)
            .map(|(_, t)| t)
    }

    /// The highest certified position of any epoch.
    pub fn certified_head(&self) -> Option<Position> {
        self.certified.values().max().copied()
    }
}

impl Consumer {
    pub fn new() -> Self {
        Self::default()
    }

    /// Accept one record or fragment. Duplicates are dropped; a fragment is
    /// held until its record is complete.
    pub fn ingest(&mut self, record: Record) -> Result<(), ReassembleError> {
        if !self.seen.insert(DedupKey::of(&record)) {
            return Ok(());
        }
        if let Some(whole) = self.reassembler.push(record)? {
            self.records
                .entry(whole.envelope.stream.clone())
                .or_default()
                .push(whole);
        }
        Ok(())
    }

    pub fn ingest_all(
        &mut self,
        records: impl IntoIterator<Item = Record>,
    ) -> Result<(), ReassembleError> {
        records.into_iter().try_for_each(|r| self.ingest(r))
    }

    /// Records still missing fragments.
    pub fn incomplete(&self) -> usize {
        self.reassembler.incomplete()
    }

    /// Streams removed by a `deleted` record that names a facet.
    fn facet_deletions(&self) -> BTreeSet<StreamId> {
        let targets: Vec<(&StreamId, &DeletedBody)> = self
            .records
            .iter()
            .flat_map(|(s, rs)| rs.iter().map(move |r| (s, r)))
            .filter_map(|(s, r)| match &r.body {
                Body::Deleted(d) if d.facet.is_some() => Some((s, d)),
                _ => None,
            })
            .collect();
        self.records
            .keys()
            .filter(|s| {
                targets.iter().any(|(root, d)| {
                    let path = d.facet.as_deref().expect("filtered");
                    let at_path = s.facet.as_deref() == Some(path);
                    let under = at_path
                        || (d.subtree
                            && s.is_at_or_under(&root.script, &root.class, &root.cell, Some(path)));
                    let removed = match d.through_incarnation {
                        // Ordered incarnations: a facet recreated after the
                        // delete, at the path or below it, is above the bound.
                        Some(bound) => under && s.incarnation <= bound,
                        None => {
                            (at_path && d.incarnation.is_none_or(|i| i == s.incarnation))
                                || (under && !at_path)
                        }
                    };
                    s.script == root.script
                        && s.class == root.class
                        && s.cell == root.cell
                        && removed
                })
            })
            .cloned()
            .collect()
    }

    /// Derive every live stream's state. Streams a facet `deleted` record
    /// removed are absent.
    pub fn state(&self) -> BTreeMap<StreamId, StreamState> {
        let gone = self.facet_deletions();
        self.records
            .iter()
            .filter(|(s, _)| !gone.contains(*s))
            .map(|(s, rs)| (s.clone(), derive(rs)))
            .collect()
    }

    /// One stream's state; `None` when it was never seen or a facet
    /// `deleted` record removed it.
    pub fn stream(&self, stream: &StreamId) -> Option<StreamState> {
        if self.facet_deletions().contains(stream) {
            return None;
        }
        self.records.get(stream).map(|rs| derive(rs))
    }
}

/// A complete snapshot and what it replaces.
struct Cut<'a> {
    position: Position,
    rank: (Origin, &'a str),
    end: &'a SnapshotEndBody,
}

impl Cut<'_> {
    fn beats(&self, other: &Cut<'_>) -> bool {
        (self.position, self.rank) > (other.position, other.rank)
    }
}

fn derive(all: &[Record]) -> StreamState {
    let mut state = StreamState::default();

    // A `deleted` naming this stream removes everything at or below it.
    let deleted_at = all
        .iter()
        .filter(|r| matches!(&r.body, Body::Deleted(d) if d.facet.is_none()))
        .map(Record::position)
        .max();
    state.deleted_at = deleted_at;
    let records: Vec<&Record> = all
        .iter()
        .filter(|r| deleted_at.is_none_or(|d| r.position() > d))
        .collect();

    // Complete snapshots: a `snapshot_end` whose record count is present.
    let mut snap_counts: BTreeMap<(&str, Position), u64> = BTreeMap::new();
    for r in &records {
        if let Body::Snapshot(b) = &r.body {
            *snap_counts
                .entry((b.snapshot_id.as_str(), r.position()))
                .or_default() += 1;
        }
    }
    let cuts: Vec<Cut<'_>> = records
        .iter()
        .filter_map(|r| match &r.body {
            Body::SnapshotEnd(e)
                if snap_counts
                    .get(&(e.snapshot_id.as_str(), r.position()))
                    .copied()
                    .unwrap_or(0)
                    == e.records =>
            {
                Some(Cut {
                    position: r.position(),
                    rank: (r.envelope.origin, e.snapshot_id.as_str()),
                    end: e,
                })
            }
            _ => None,
        })
        .collect();
    let stream_cut = cuts
        .iter()
        .filter(|c| c.end.scope == SnapshotScope::Stream)
        .fold(None::<&Cut<'_>>, |best, c| match best {
            Some(b) if !c.beats(b) => Some(b),
            _ => Some(c),
        });
    let cut_for = |tg: &TableGen| -> Option<&Cut<'_>> {
        cuts.iter()
            .filter(|c| c.end.scope == SnapshotScope::Tables && c.end.tables.contains(tg))
            .chain(stream_cut)
            .fold(None, |best: Option<&Cut<'_>>, c| match best {
                Some(b) if !c.beats(b) => Some(b),
                _ => Some(c),
            })
    };

    // Table generations and which are closed.
    let mut opened: BTreeMap<TableGen, Position> = BTreeMap::new();
    let mut closed: BTreeSet<TableGen> = BTreeSet::new();
    let mut renames: Vec<(&str, Position)> = Vec::new();
    for r in &records {
        // A `bulk` marker names generations too: a table seen only through
        // one is still uncertain.
        let tgs = match &r.body {
            Body::Schema(s) => {
                let tg = TableGen {
                    table: s.table.clone(),
                    generation: s.generation,
                };
                if s.dropped {
                    closed.insert(tg.clone());
                }
                if let Some(from) = &s.renamed_from {
                    renames.push((from.as_str(), r.position()));
                }
                vec![tg]
            }
            Body::Bulk(b) => b.tables.clone(),
            _ => match r.body.table_rows() {
                Some(d) => vec![d.table_gen()],
                None => continue,
            },
        };
        for tg in tgs {
            let at = opened.entry(tg).or_insert(r.position());
            *at = (*at).min(r.position());
        }
    }
    let newest: BTreeMap<&str, u64> = opened.keys().fold(BTreeMap::new(), |mut m, tg| {
        let g = m.entry(tg.table.as_str()).or_insert(0);
        *g = (*g).max(tg.generation);
        m
    });
    for (tg, at) in &opened {
        let superseded = newest[tg.table.as_str()] > tg.generation;
        let renamed_away = renames
            .iter()
            .any(|(from, p)| *from == tg.table && *p >= *at);
        if superseded || renamed_away {
            closed.insert(tg.clone());
        }
    }

    // Rows per open generation: the winning snapshot, then live rows above it.
    for tg in opened.keys().filter(|tg| !closed.contains(*tg)) {
        let cut = cut_for(tg);
        let mut table = TableState::default();
        if let Some(c) = cut {
            for r in &records {
                if let Body::Snapshot(b) = &r.body {
                    if r.position() == c.position
                        && b.snapshot_id == c.rank.1
                        && r.envelope.origin == c.rank.0
                        && b.data.table_gen() == *tg
                    {
                        table.columns = b.data.columns.clone();
                        table.key_columns = b.data.key_columns.clone();
                        for row in &b.data.rows {
                            table.rows.insert(row.key().to_vec(), row.row().to_vec());
                        }
                    }
                }
            }
        }
        let mut live: Vec<&Record> = records
            .iter()
            .copied()
            .filter(|r| matches!(&r.body, Body::Rows(b) if b.data.table_gen() == *tg))
            .filter(|r| cut.is_none_or(|c| r.position() > c.position))
            .collect();
        live.sort_by_key(|r| r.position());
        for r in live {
            let data = r.body.table_rows().expect("rows");
            table.columns = data.columns.clone();
            table.key_columns = data.key_columns.clone();
            for row in &data.rows {
                match row.op() {
                    Op::Insert | Op::Update => {
                        table.rows.insert(row.key().to_vec(), row.row().to_vec());
                    }
                    Op::Delete => {
                        table.rows.remove(row.key());
                    }
                }
            }
        }
        if !table.rows.is_empty() {
            state.tables.insert(tg.clone(), table);
        }

        let bulk_after_cut = records.iter().any(|r| match &r.body {
            Body::Bulk(b) => b.tables.contains(tg) && cut.is_none_or(|c| r.position() > c.position),
            _ => false,
        });
        if bulk_after_cut {
            state.uncertain.insert(tg.clone());
        }
    }

    state.certified = certify(&records);

    // Gaps a stream-wide snapshot has not covered.
    let covered = |epoch: u64, txid: u64| {
        stream_cut.is_some_and(|c| (c.position.epoch, c.position.txid) >= (epoch, txid))
    };
    let beyond = |epoch: u64, txid: u64| match state.certified.get(&epoch) {
        Some(c) => txid > c.txid,
        None => txid > 0,
    };
    for r in &records {
        let gap = match &r.body {
            Body::Gap(g) => (!covered(g.to.epoch, g.to.txid)).then(|| Gap::Reported {
                from: g.from,
                to: g.to,
                reason: g.reason.clone(),
            }),
            Body::Link(l) => match (l.prev_epoch, l.prev_txid) {
                (Some(e), Some(t)) if beyond(e, t) && !covered(e, t) => Some(Gap::Link {
                    prev_epoch: e,
                    prev_txid: t,
                    certified: state.certified.get(&e).copied(),
                }),
                _ => None,
            },
            Body::Recovered(rec) => {
                let h = rec.head;
                (beyond(h.epoch, h.txid) && !covered(h.epoch, h.txid)).then(|| Gap::Recovered {
                    head: h,
                    certified: state.certified.get(&h.epoch).copied(),
                })
            }
            _ => None,
        };
        state.gaps.extend(gap);
    }
    state
}

/// Per epoch, follow watermarks from the epoch's first (`from` absent) along
/// `from == previous through`, accepting each only while its counts match the
/// records held in its range.
fn certify(records: &[&Record]) -> BTreeMap<u64, Position> {
    // What the node's sink sent: everything but repair output and watermarks.
    let sent: Vec<&Record> = records
        .iter()
        .copied()
        .filter(|r| r.envelope.origin != Origin::Repair)
        .filter(|r| !matches!(r.body, Body::Watermark(_)))
        .collect();
    let mut certified = BTreeMap::new();
    let marks: Vec<(Position, &crate::record::WatermarkBody)> = records
        .iter()
        .filter_map(|r| match &r.body {
            Body::Watermark(w) => Some((r.position(), w)),
            _ => None,
        })
        .collect();
    let epochs: BTreeSet<u64> = marks.iter().map(|(_, w)| w.through.epoch).collect();
    for epoch in epochs {
        let mut at: Option<Position> = None;
        loop {
            let next = marks.iter().map(|(_, w)| *w).find(|w| {
                w.through.epoch == epoch && w.from == at && at.is_none_or(|a| w.through > a) && {
                    let lo = w.from.unwrap_or(Position::new(epoch, 0, 0));
                    let in_range: Vec<&&Record> = sent
                        .iter()
                        .filter(|r| {
                            let p = r.position();
                            p <= w.through && (p > lo || (w.from.is_none() && p == lo))
                        })
                        .collect();
                    let commits: BTreeSet<Position> =
                        in_range.iter().map(|r| r.position()).collect();
                    in_range.len() as u64 == w.records && commits.len() as u64 == w.commits
                }
            });
            match next {
                Some(w) => at = Some(w.through),
                None => break,
            }
        }
        if let Some(p) = at {
            certified.insert(epoch, p);
        }
    }
    certified
}
