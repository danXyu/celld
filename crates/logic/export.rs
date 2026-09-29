// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Change export: labelling each captured commit with the LTX file that holds
//! it, and releasing commits once a proof covers their label (sans-IO).
//!
//! The cell thread hands every exported commit to [`Attribution::commit`]
//! with the WAL point its last frame landed at. The capture loop reports
//! every L0 file it writes to [`Attribution::captured`]. A commit's **label**
//! is the TXID of the first file whose capture contains its last frame; the
//! label must be exact, because a label that is too late lets a delivered
//! position claim state the stream has not sent, and one that is too early
//! releases a change before its capture is durable.
//!
//! # Matching
//!
//! A WAL frame `n` of a generation ends at byte `32 + n·(24 + page_size)`,
//! so a captured WAL byte range is a frame range `(after, through]`
//! ([`frames_through`]). A **partial** capture covers the commits of its own
//! generation whose last frame falls in that range. A **full image** covers
//! every commit of its generation up to its boundary frame, and every commit
//! of an earlier generation. No rule assigns "every pending commit" to
//! anything, because a commit can land after a capture's read and before
//! the capture is reported.
//!
//! Salts carry no order of their own (SQLite re-randomizes them when a
//! connection that has never restarted the WAL writes its first frame), so
//! generations are ordered only by what was observed:
//!
//! - the capture stream and the commit stream each visit generations in WAL
//!   order;
//! - a partial capture from the header of a new generation continues the
//!   replica from the capture before it, so no generation lies between them;
//! - every commit that arrived before a capture was reported is in that
//!   capture's generation or an earlier one;
//! - a generation no capture read lies before the first capture or after the
//!   latest, so one known to precede a captured generation precedes all.
//!
//! The last two rest on the capture loop being the only thing that restarts
//! the WAL (its read lock pins the WAL against every other checkpoint), on
//! it capturing the new generation before anything else after a restart,
//! and on it never restarting between a capture's read and its report. A
//! commit whose generation is not yet placed waits; the next capture places
//! it.
//!
//! A commit that a capture's boundary has passed without covering it (a
//! bug, or a capture this state had to forget), or one that two captures
//! could hold with nothing to say which came first, is **dropped**: it
//! becomes a gap that is released with the proof that covers every capture
//! that could hold it. The exporter never guesses a label.
//!
//! # Ordering contract
//!
//! Commits arrive in commit order and captures in TXID order, and both reach
//! this state through one FIFO; the capture observer reports each file before
//! the capture loop does anything else. [`Attribution::caught_up`] says every
//! commit the cell connection has made so far has arrived; the cell thread
//! calls it at every safe point, after it pushes that safe point's commits.
//! Every capture that arrived before it has therefore seen all of its
//! commits, which is what lets the released position cover a capture whose
//! last frames were the capture loop's own `_litestream_seq` writes rather
//! than an app commit.
//!
//! # Release
//!
//! [`Attribution::proven`] reports the position a settled
//! [`crate::Effect::ExportProven`] proved: `max(durable_txid, shipped_txid)`
//! read after the ticket settles, because a fleet proof advances only the
//! shipped watermark. Every pending commit whose label is at or below it is
//! released in commit order. A commit labelled after a proof that already
//! covers it is released at once: the ownership read that proof passed
//! already binds every later owner to a lineage holding it.
//!
//! The **released position** is the largest TXID such that every commit
//! labelled at or below it has been released (or dropped into a released
//! gap), and every commit that could still be labelled at or below it has
//! arrived.
//!
//! One instance serves one residency of one cell: TXIDs restart per epoch,
//! so the adapter pairs every position with its epoch and starts a fresh
//! instance when the cell activates again.

use std::collections::VecDeque;

/// Bytes in a WAL file header.
pub const WAL_HEADER_BYTES: u64 = 32;

/// Bytes in a WAL frame header, before the page.
pub const WAL_FRAME_HEADER_BYTES: u64 = 24;

/// How many unsettled captures one cell keeps for commits still to arrive.
/// The cell thread's safe points settle captures long before this: it bounds
/// memory when they stop, and a capture forgotten here only turns a late
/// commit into a gap.
pub const MAX_RETAINED_CAPTURES: usize = 1024;

/// The number of whole frames of a WAL generation that end at or before
/// byte `offset`.
pub fn frames_through(offset: u64, page_size: u32) -> u64 {
    offset.saturating_sub(WAL_HEADER_BYTES) / (u64::from(page_size) + WAL_FRAME_HEADER_BYTES)
}

/// One WAL generation, named by the salts in its header.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct WalGeneration {
    pub salt1: u32,
    pub salt2: u32,
}

/// Where a commit's last frame landed: its generation, and the number of
/// frames in that generation through the commit (the WAL hook's count).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WalPoint {
    pub generation: WalGeneration,
    pub frames: u64,
}

/// The WAL bytes one captured file read: `[offset, offset + size)` of the
/// generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CapturedWal {
    pub generation: WalGeneration,
    pub offset: u64,
    pub size: u64,
}

/// One L0 file the capture loop wrote, as the capture observer reports it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Capture {
    /// The file's LTX TXID in this epoch. Strictly increasing.
    pub txid: u64,
    pub page_size: u32,
    /// The file holds every page of the database: every commit through the
    /// end of `wal` is in it, including commits of earlier generations.
    pub full_image: bool,
    /// `None` for a seeded baseline, which covers no frame of this WAL.
    pub wal: Option<CapturedWal>,
}

/// What one release hands to the node buffer, in commit order.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Released<T> {
    Commit {
        /// The TXID of the capture that holds the commit.
        label: u64,
        payload: T,
    },
    /// Commits the exporter dropped. Their labels lie in `(after, through]`,
    /// so the consumer must treat that range of the cell as missing until
    /// repair replaces it.
    Gap {
        after: u64,
        through: u64,
        /// Commits dropped because no capture could be matched to them.
        unmatched: u64,
        /// Commits dropped because the export queue was over budget.
        overflowed: u64,
    },
}

/// How an over-budget export queue sheds, given the shared budget
/// (`CELLD_EXPORT_QUEUE_BYTES`) and the bytes held by the pending list and by
/// the node buffer.
///
/// The node buffer gives up its oldest released records first: they already
/// passed the gate, and dropping them freezes the delivered position without
/// losing a commit's place in the stream. Pending commits are shed only for
/// what the buffer cannot cover.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shed {
    /// Bytes of the oldest released records the node buffer must drop.
    pub buffered: u64,
    /// The size to shed the pending list down to, with
    /// [`Attribution::shed_to`].
    pub pending_limit: u64,
}

pub fn shed(budget: u64, pending: u64, buffered: u64) -> Shed {
    let over = pending.saturating_add(buffered).saturating_sub(budget);
    let from_buffer = over.min(buffered);
    Shed {
        buffered: from_buffer,
        pending_limit: pending - (over - from_buffer),
    }
}

/// A captured file's reach, in frames of one generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Span {
    generation: WalGeneration,
    /// Frames of the generation before the capture's first frame.
    after: u64,
    /// Frames of the generation through the capture's last frame.
    through: u64,
    full_image: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Retained {
    txid: u64,
    span: Option<Span>,
}

#[derive(Debug)]
enum Entry<T> {
    Commit {
        at: WalPoint,
        label: Option<u64>,
        bytes: u64,
        payload: T,
    },
    Gap {
        /// The largest label among the dropped commits once it is known.
        through: Option<u64>,
        /// The last dropped commit whose label is still unknown. Labels are
        /// non-decreasing in commit order, so it alone bounds the gap.
        unlabeled: Option<WalPoint>,
        unmatched: u64,
        overflowed: u64,
    },
}

/// Why a commit could not be labelled.
enum Verdict {
    Label(u64),
    Wait,
    Unmatched,
}

/// The attribution and release state of one cell residency.
#[derive(Debug)]
pub struct Attribution<T> {
    pending: VecDeque<Entry<T>>,
    pending_bytes: u64,
    /// Captures a commit still to arrive may belong to, in TXID order.
    retained: VecDeque<Retained>,
    /// The latest capture that read the WAL.
    head: Option<Span>,
    /// The furthest span dropped by the retention cap before it settled.
    forgotten: Option<Span>,
    order: Order,
    last_commit: Option<WalPoint>,
    /// The largest TXID captured, and the largest whose commits have all
    /// arrived.
    captured_txid: u64,
    settled_txid: u64,
    proven_txid: u64,
    released_position: u64,
    unmatched_total: u64,
    released: Vec<Released<T>>,
}

impl<T> Default for Attribution<T> {
    fn default() -> Self {
        Self {
            pending: VecDeque::new(),
            pending_bytes: 0,
            retained: VecDeque::new(),
            head: None,
            forgotten: None,
            order: Order::default(),
            last_commit: None,
            captured_txid: 0,
            settled_txid: 0,
            proven_txid: 0,
            released_position: 0,
            unmatched_total: 0,
            released: Vec::new(),
        }
    }
}

impl<T> Attribution<T> {
    pub fn new() -> Self {
        Self::default()
    }

    /// A commit the cell thread pulled, in commit order. `bytes` is its
    /// encoded size, counted against the shared export budget.
    pub fn commit(&mut self, at: WalPoint, bytes: u64, payload: T) {
        self.order.committed(at.generation);
        self.last_commit = Some(at);
        self.pending.push_back(Entry::Commit {
            at,
            label: None,
            bytes,
            payload,
        });
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.resolve();
        self.settle_passed(at);
        self.prune_settled();
        self.forget_generations();
        self.release();
    }

    /// A commit at `at` whose payload was shed before it reached this state,
    /// because the export queue was over budget when the cell thread pulled
    /// it. It takes its place in commit order as an overflow gap, labelled
    /// like any other commit, so the gap is released with the proof that
    /// covers it.
    pub fn dropped(&mut self, at: WalPoint) {
        self.order.committed(at.generation);
        self.last_commit = Some(at);
        let merged = match self.pending.back_mut() {
            Some(Entry::Gap {
                through,
                unlabeled,
                overflowed,
                ..
            }) => {
                // This commit is the later one, so its label bounds the gap
                // once known, as in `drop_commit`.
                *through = None;
                *unlabeled = Some(at);
                *overflowed += 1;
                true
            }
            _ => false,
        };
        if !merged {
            self.pending.push_back(Entry::Gap {
                through: None,
                unlabeled: Some(at),
                unmatched: 0,
                overflowed: 1,
            });
        }
        self.resolve();
        self.settle_passed(at);
        self.prune_settled();
        self.forget_generations();
        self.release();
    }

    /// A commit the cell thread pulled whose WAL point could not be read,
    /// because the capture loop restarted or truncated the WAL between the
    /// commit and the WAL hook's read. The capture loop does that only after
    /// it captured and reported every frame, so the capture that holds the
    /// commit has already arrived: the commit is dropped into a gap bounded
    /// by the latest capture, like a commit a capture passed.
    pub fn unplaced(&mut self, bytes: u64, payload: T) {
        let at = self.last_commit.unwrap_or(WalPoint {
            generation: WalGeneration { salt1: 0, salt2: 0 },
            frames: 0,
        });
        self.pending.push_back(Entry::Commit {
            at,
            label: None,
            bytes,
            payload,
        });
        self.pending_bytes = self.pending_bytes.saturating_add(bytes);
        self.unmatched_total += 1;
        self.drop_commit(self.pending.len() - 1, true);
        self.release();
    }

    /// A file the capture loop wrote, in TXID order.
    pub fn captured(&mut self, capture: &Capture) {
        if capture.txid <= self.captured_txid {
            // A replayed or reordered report: its commits are labelled.
            return;
        }
        self.captured_txid = capture.txid;
        let span = capture.wal.map(|wal| Span {
            generation: wal.generation,
            after: frames_through(wal.offset, capture.page_size),
            through: frames_through(wal.offset.saturating_add(wal.size), capture.page_size),
            full_image: capture.full_image,
        });
        if let Some(span) = span {
            // A partial capture from the header of a new generation continues
            // the replica from the previous capture, which is only sound when
            // no generation lies between the two.
            let predecessor = self
                .head
                .filter(|previous| {
                    !span.full_image && span.after == 0 && previous.generation != span.generation
                })
                .map(|previous| previous.generation);
            self.order.captured(
                span.generation,
                predecessor,
                self.last_commit.map(|at| at.generation),
            );
            self.head = Some(span);
        }
        self.retained.push_back(Retained {
            txid: capture.txid,
            span,
        });
        self.resolve();
        if let Some(at) = self.last_commit {
            self.settle_passed(at);
        }
        self.prune_settled();
        while self.retained.len() > MAX_RETAINED_CAPTURES {
            let lost = self.retained.pop_front().expect("over the cap");
            if let Some(span) = lost.span {
                self.forgotten = Some(span);
            }
        }
        self.release();
    }

    /// Every commit the cell connection has made so far has arrived, so every
    /// capture that arrived before this call has seen all of its commits.
    pub fn caught_up(&mut self) {
        self.settled_txid = self.captured_txid;
        self.prune_settled();
        self.release();
    }

    /// A settled export ticket proved every capture through `txid` durable,
    /// and the node still owns the cell.
    pub fn proven(&mut self, txid: u64) {
        self.proven_txid = self.proven_txid.max(txid);
        self.release();
    }

    /// Drop the oldest pending commits until at most `limit` bytes remain.
    /// They become one gap, released with the proof that covers them.
    pub fn shed_to(&mut self, limit: u64) {
        let mut index = 0;
        while self.pending_bytes > limit && index < self.pending.len() {
            // A commit merged into the gap before it leaves the queue, and
            // the next entry takes its index.
            if !(matches!(self.pending[index], Entry::Commit { .. })
                && self.drop_commit(index, false))
            {
                index += 1;
            }
        }
        self.release();
    }

    /// What the gate has let out since the last call, in commit order.
    pub fn take_released(&mut self) -> Vec<Released<T>> {
        std::mem::take(&mut self.released)
    }

    /// The largest TXID such that every commit labelled at or below it has
    /// been released. 0 until the first release.
    pub fn released_position(&self) -> u64 {
        self.released_position
    }

    /// Encoded bytes of the commits still waiting for a proof.
    pub fn pending_bytes(&self) -> u64 {
        self.pending_bytes
    }

    /// Commits and gaps still waiting for a proof.
    pub fn pending_len(&self) -> usize {
        self.pending.len()
    }

    /// Commits dropped because no capture matched them, for the metric.
    pub fn unmatched_total(&self) -> u64 {
        self.unmatched_total
    }

    fn unlabeled_points(&self) -> impl Iterator<Item = WalPoint> + '_ {
        self.pending.iter().filter_map(|entry| match entry {
            Entry::Commit {
                at, label: None, ..
            } => Some(*at),
            Entry::Gap {
                through: None,
                unlabeled,
                ..
            } => *unlabeled,
            _ => None,
        })
    }

    fn earlier(&self, a: WalGeneration, b: WalGeneration) -> bool {
        self.order.earlier(a, b)
    }

    /// Forget the order of generations no commit can still be in: those
    /// before the first waiting commit's (or the latest commit's) that no
    /// retained capture reads. Commits arrive in WAL order, so none will.
    fn forget_generations(&mut self) {
        let Some(floor) = self
            .unlabeled_points()
            .next()
            .or(self.last_commit)
            .map(|at| at.generation)
        else {
            return;
        };
        let mut keep: Vec<WalGeneration> = self
            .retained
            .iter()
            .filter_map(|kept| kept.span.map(|span| span.generation))
            .collect();
        keep.extend(self.head.map(|span| span.generation));
        keep.extend(self.forgotten.map(|span| span.generation));
        self.order.forget_before(floor, &keep);
    }

    fn verdict(&self, at: WalPoint) -> Verdict {
        // An earlier full image whose generation the streams have not ordered
        // against the commit's may still turn out to hold it, so a later
        // capture that holds it cannot be known to be the first.
        let mut unordered_image = false;
        for kept in &self.retained {
            let Some(span) = kept.span else {
                continue;
            };
            if self.covers(span, at) {
                if unordered_image {
                    return Verdict::Unmatched;
                }
                return Verdict::Label(kept.txid);
            }
            if self.passes(span, at) {
                return Verdict::Unmatched;
            }
            unordered_image |= span.full_image
                && span.generation != at.generation
                && !self.earlier(span.generation, at.generation);
        }
        // A capture this state no longer holds reached the commit, so the
        // capture that held it is gone.
        let behind = |span: Option<Span>| {
            span.is_some_and(|span| {
                (span.generation == at.generation && at.frames <= span.through)
                    || self.earlier(at.generation, span.generation)
            })
        };
        if behind(self.head) || behind(self.forgotten) {
            return Verdict::Unmatched;
        }
        Verdict::Wait
    }

    fn covers(&self, span: Span, at: WalPoint) -> bool {
        if span.generation == at.generation {
            return at.frames <= span.through && (span.full_image || at.frames > span.after);
        }
        span.full_image && self.earlier(at.generation, span.generation)
    }

    /// A partial capture that starts past the commit, or that reads a later
    /// generation, sealed everything before it without holding this commit.
    fn passes(&self, span: Span, at: WalPoint) -> bool {
        if span.full_image {
            return false;
        }
        if span.generation == at.generation {
            return at.frames <= span.after;
        }
        self.earlier(at.generation, span.generation)
    }

    /// Label every waiting commit the retained captures can, and drop the
    /// ones a capture passed.
    fn resolve(&mut self) {
        let mut index = 0;
        while index < self.pending.len() {
            let waiting = match &self.pending[index] {
                Entry::Commit {
                    at, label: None, ..
                } => Some((*at, true)),
                Entry::Gap {
                    through: None,
                    unlabeled: Some(at),
                    ..
                } => Some((*at, false)),
                _ => None,
            };
            let Some((at, is_commit)) = waiting else {
                index += 1;
                continue;
            };
            let verdict = self.verdict(at);
            match (&mut self.pending[index], verdict) {
                (_, Verdict::Wait) => {}
                (Entry::Commit { label, .. }, Verdict::Label(txid)) => *label = Some(txid),
                (Entry::Gap { through, .. }, Verdict::Label(txid)) => *through = Some(txid),
                (_, Verdict::Unmatched) if is_commit => {
                    self.unmatched_total += 1;
                    if self.drop_commit(index, true) {
                        continue;
                    }
                }
                (Entry::Gap { through, .. }, Verdict::Unmatched) => {
                    // Whatever capture held it is no later than the head.
                    *through = Some(self.captured_txid);
                }
                (Entry::Commit { .. }, Verdict::Unmatched) => unreachable!("handled above"),
            }
            index += 1;
        }
    }

    /// Turn the commit at `index` into a gap, merged with a gap just before
    /// it. Returns whether it merged, which removes the entry at `index`.
    fn drop_commit(&mut self, index: usize, unmatched: bool) -> bool {
        let Entry::Commit {
            at, label, bytes, ..
        } = self.pending[index]
        else {
            unreachable!("only commits are dropped")
        };
        self.pending_bytes -= bytes;
        // An unmatched commit's capture is already written, so no capture
        // after the latest one can hold it.
        let label = if unmatched {
            Some(self.captured_txid)
        } else {
            label
        };
        let (through, unlabeled) = match label {
            Some(txid) => (Some(txid), None),
            None => (None, Some(at)),
        };
        let merged = index > 0
            && match &mut self.pending[index - 1] {
                Entry::Gap {
                    through: previous,
                    unlabeled: previous_unlabeled,
                    unmatched: previous_unmatched,
                    overflowed: previous_overflowed,
                } => {
                    // The later commit's label bounds the merged gap; an
                    // earlier bound only matters while the later is unknown
                    // and cannot be below it.
                    *previous = match (through, *previous) {
                        (Some(later), Some(earlier)) => Some(later.max(earlier)),
                        (later, _) => later,
                    };
                    *previous_unlabeled = unlabeled;
                    if unmatched {
                        *previous_unmatched += 1;
                    } else {
                        *previous_overflowed += 1;
                    }
                    true
                }
                Entry::Commit { .. } => false,
            };
        if merged {
            self.pending.remove(index);
        } else {
            self.pending[index] = Entry::Gap {
                through,
                unlabeled,
                unmatched: u64::from(unmatched),
                overflowed: u64::from(!unmatched),
            };
        }
        merged
    }

    /// Forget the settled captures no waiting commit can still need. A full
    /// image may still cover a waiting commit once the streams order its
    /// generation, so it stays until they do; every other settled capture
    /// covers nothing that has not arrived.
    fn prune_settled(&mut self) {
        let unlabeled: Vec<WalPoint> = self.unlabeled_points().collect();
        let settled = self.settled_txid;
        let kept: VecDeque<Retained> = self
            .retained
            .iter()
            .filter(|kept| {
                kept.txid > settled
                    || kept.span.is_some_and(|span| {
                        span.full_image
                            && unlabeled.iter().any(|at| {
                                at.generation != span.generation
                                    && !self.earlier(at.generation, span.generation)
                                    && !self.earlier(span.generation, at.generation)
                            })
                    })
            })
            .copied()
            .collect();
        self.retained = kept;
    }

    /// Settle the captures a commit at `at` has passed: every commit they
    /// hold precedes it, so every such commit has arrived.
    fn settle_passed(&mut self, at: WalPoint) {
        while let Some(front) = self.retained.front() {
            let passed = match front.span {
                None => true,
                Some(span) => {
                    (span.generation == at.generation && span.through <= at.frames)
                        || self.earlier(span.generation, at.generation)
                }
            };
            if !passed {
                break;
            }
            self.settled_txid = self.settled_txid.max(front.txid);
            self.retained.pop_front();
        }
    }

    fn release(&mut self) {
        while let Some(front) = self.pending.front() {
            match front {
                Entry::Commit {
                    label: Some(label), ..
                } if *label <= self.proven_txid => {
                    let Some(Entry::Commit {
                        label: Some(label),
                        bytes,
                        payload,
                        ..
                    }) = self.pending.pop_front()
                    else {
                        unreachable!("matched above")
                    };
                    self.pending_bytes -= bytes;
                    self.released.push(Released::Commit { label, payload });
                }
                Entry::Gap {
                    through: Some(through),
                    unmatched,
                    overflowed,
                    ..
                } if *through <= self.proven_txid => {
                    self.released.push(Released::Gap {
                        after: self.released_position,
                        through: *through,
                        unmatched: *unmatched,
                        overflowed: *overflowed,
                    });
                    self.pending.pop_front();
                }
                _ => break,
            }
        }
        // A label still to be assigned comes from a retained capture or a
        // later one, so no position at or past the first of those is final.
        let unassigned = self.retained.front().map_or(self.settled_txid, |kept| {
            self.settled_txid.min(kept.txid.saturating_sub(1))
        });
        let reach = match self.pending.front() {
            None | Some(Entry::Commit { label: None, .. }) => unassigned,
            Some(Entry::Commit {
                label: Some(label), ..
            }) => unassigned.min(label - 1),
            // A gap's dropped commits carry labels anywhere up to its bound.
            Some(Entry::Gap { .. }) => self.released_position,
        };
        self.released_position = self.released_position.max(reach.min(self.proven_txid));
    }
}

/// What the two streams have shown about the order of WAL generations.
///
/// Salts carry no order of their own: SQLite increments `salt1` when a
/// connection restarts the WAL, but a connection that has never restarted it
/// re-randomizes both salts when it writes the first frame after another
/// connection's TRUNCATE. Order therefore comes only from what was observed.
#[derive(Debug, Default)]
struct Order {
    /// Generations captures read, in WAL order.
    captured: Vec<WalGeneration>,
    /// Generations arrived commits were in, in WAL order.
    committed: Vec<WalGeneration>,
    /// `(earlier, later)`: the latest commit to arrive before a capture of
    /// `later` was reported was in `earlier`.
    before: Vec<(WalGeneration, WalGeneration)>,
    /// `(predecessor, successor)`: no generation lies between them.
    adjacent: Vec<(WalGeneration, WalGeneration)>,
}

impl Order {
    fn committed(&mut self, generation: WalGeneration) {
        push_distinct(&mut self.committed, generation);
    }

    /// A capture of `generation`. `predecessor` is set when it continues the
    /// replica from a capture of that generation with nothing between.
    /// `last_commit` is the generation of the latest commit to arrive.
    fn captured(
        &mut self,
        generation: WalGeneration,
        predecessor: Option<WalGeneration>,
        last_commit: Option<WalGeneration>,
    ) {
        push_distinct(&mut self.captured, generation);
        if let Some(predecessor) = predecessor {
            self.adjacent.push((predecessor, generation));
        }
        // Every commit that arrived before this report committed before it,
        // and the capture loop does not restart the WAL between a capture's
        // read and its report: those commits are in this generation or an
        // earlier one.
        if let Some(last) = last_commit.filter(|last| *last != generation) {
            if !self.before.contains(&(last, generation)) {
                self.before.push((last, generation));
            }
        }
        // A backstop only: `forget_before` keeps these small while commits
        // arrive.
        for list in [&mut self.captured, &mut self.committed] {
            if list.len() > MAX_RETAINED_CAPTURES {
                list.remove(0);
            }
        }
        for list in [&mut self.before, &mut self.adjacent] {
            if list.len() > MAX_RETAINED_CAPTURES {
                list.remove(0);
            }
        }
    }

    /// Whether the streams have shown generation `a` before generation `b`.
    fn earlier(&self, a: WalGeneration, b: WalGeneration) -> bool {
        if a == b {
            return false;
        }
        let later = self.after(a);
        if later.contains(&b) {
            return true;
        }
        // The capture loop makes every WAL restart, and captures the new
        // generation before anything else happens, so a generation no capture
        // read lies before the first capture or after the latest. One known to
        // precede a captured generation therefore precedes all of them.
        !self.captured.contains(&a)
            && self.captured.contains(&b)
            && later.iter().any(|later| self.captured.contains(later))
    }

    /// Every generation known to follow `a`: its successors along either
    /// chain and the `before` pairs, and the adjacent predecessor of any of
    /// them other than `a` itself.
    fn after(&self, a: WalGeneration) -> Vec<WalGeneration> {
        let mut later: Vec<WalGeneration> = Vec::new();
        let mut frontier = vec![a];
        while let Some(generation) = frontier.pop() {
            let mut found = |candidate: WalGeneration| {
                if candidate != a && !later.contains(&candidate) {
                    later.push(candidate);
                    frontier.push(candidate);
                }
            };
            for chain in [&self.captured, &self.committed] {
                if let Some(position) = chain.iter().position(|seen| *seen == generation) {
                    chain[position + 1..].iter().for_each(|later| found(*later));
                }
            }
            for (earlier, successor) in &self.before {
                if *earlier == generation {
                    found(*successor);
                }
            }
            if generation != a {
                for (predecessor, successor) in &self.adjacent {
                    if *successor == generation {
                        found(*predecessor);
                    }
                }
            }
        }
        later
    }

    /// Forget every generation known to precede `floor`, except `keep`.
    fn forget_before(&mut self, floor: WalGeneration, keep: &[WalGeneration]) {
        let gone: Vec<WalGeneration> = self
            .captured
            .iter()
            .chain(&self.committed)
            .copied()
            .filter(|generation| !keep.contains(generation) && self.earlier(*generation, floor))
            .collect();
        if gone.is_empty() {
            return;
        }
        self.captured
            .retain(|generation| !gone.contains(generation));
        self.committed
            .retain(|generation| !gone.contains(generation));
        self.before
            .retain(|(a, b)| !gone.contains(a) && !gone.contains(b));
        self.adjacent
            .retain(|(a, b)| !gone.contains(a) && !gone.contains(b));
    }
}

fn push_distinct(chain: &mut Vec<WalGeneration>, generation: WalGeneration) {
    if chain.last() == Some(&generation) {
        return;
    }
    // A generation seen again after another is a new lineage with colliding
    // salts; forget the old place so the chain stays a strict order.
    chain.retain(|seen| *seen != generation);
    chain.push(generation);
}
