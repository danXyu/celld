//! The reconciler (`docs/design/change-export.md#completeness`).
//!
//! It is the bound on how long a loss stays invisible. Links and `recovered`
//! records expose most losses from the stream itself, but a cell can die
//! with its last records unacknowledged and never activate again, recovery
//! does not visit a cell whose writes were already folded, and the
//! `recovered` record itself can be lost. So on a schedule the reconciler
//! compares every cell's head in the bucket ([`super::inventory`]) with the
//! consumer's certified positions and stream set, and turns every
//! difference into a [`Finding`]:
//!
//! - **Gap.** The bucket holds an epoch of the cell past what the consumer
//!   certified in it, and no stream-wide snapshot covers the difference.
//!   Closed epochs are checked to the end the chain gives them, the newest to
//!   the head.
//! - **Lost.** The consumer certified changes the cell no longer has: past
//!   the end the chain gives a closed epoch, in an epoch between the chain's
//!   ends that the chain skipped, or past the head of the newest epoch when a
//!   node that produced it declared a bounded loss.
//! - **Missing `deleted`.** A facet the consumer holds has no objects while
//!   its root does.
//! - **Unknown stream.** The bucket holds an exported cell the consumer has
//!   never seen: backfill it.
//!
//! The bucket runs behind the fleet (tails not yet folded, rows not yet
//! flushed) and the consumer runs behind the bucket sink, so a difference
//! counts only once it has settled: the objects or records it rests on are
//! older than [`Options::settle_ms`]. Tombstoned cells are skipped, and so
//! are classes the export does not cover.
//!
//! Gap and lost findings also become `gap` records, and a missing `deleted`
//! a `deleted` record on the root's stream, all with `origin: repair` and
//! `node: reconciler`. A consumer's certification ignores repair output, so
//! these cannot break a watermark's counts, and a later run that finds the
//! same difference writes the same record, which consumers drop as a
//! duplicate. The repair driver closes them with a snapshot at the head.

use std::collections::BTreeMap;

use celld_export_format::{
    Body, DeletedBody, Envelope, GapBody, Origin, Position, Record, StreamId,
};
use serde::Serialize;

use super::inventory::{class_of, root_of, BucketHead, Loss};
use super::tombstone::{scope_of, Tombstone};
use super::{Finding, FindingKind, RecoveredSession, StreamSummary, AUDIT_NODE};

#[derive(Clone, Copy, Debug)]
pub struct Options {
    /// Now, in unix ms.
    pub now_ms: i64,
    /// How old a difference's evidence must be before it counts.
    pub settle_ms: i64,
}

/// One run's result.
#[derive(Debug, Default, Serialize)]
pub struct Reconciled {
    pub findings: Vec<Finding>,
    /// The `gap` and `deleted` records the findings call for.
    #[serde(skip)]
    pub records: Vec<Record>,
    /// Bucket cells compared with a consumer stream.
    pub checked: usize,
    /// Bucket cells skipped because a tombstone covers them.
    pub tombstoned: usize,
    /// Bucket cells skipped because their class is not exported.
    pub not_exported: usize,
    /// Differences seen but too recent to count yet.
    pub unsettled: usize,
    /// Dead sessions whose `recovered` records the consumer holds fewer of
    /// than recovery sent. The per-cell comparison above already covers the
    /// cells concerned; this says the stream alone would have missed them.
    pub recovered_short: Vec<RecoveredSession>,
}

pub fn reconcile(
    heads: &BTreeMap<String, BucketHead>,
    losses: &[Loss],
    streams: &[StreamSummary],
    recovered: &[RecoveredSession],
    tombstones: &[Tombstone],
    exports_class: impl Fn(&str) -> bool,
    options: Options,
) -> Reconciled {
    let settled = |ms: i64| options.now_ms.saturating_sub(ms) >= options.settle_ms;
    let mut out = Reconciled {
        recovered_short: recovered
            .iter()
            .filter(|s| s.held < s.expected)
            .cloned()
            .collect(),
        ..Default::default()
    };
    let lossy: Vec<&str> = losses.iter().map(Loss::node).collect();

    // Consumer streams by the bucket scope they live under.
    let mut by_scope: BTreeMap<String, Vec<&StreamSummary>> = BTreeMap::new();
    for s in streams {
        by_scope
            .entry(scope_of(&s.id.cell, s.id.facet.as_deref()))
            .or_default()
            .push(s);
    }

    for head in heads.values() {
        if !exports_class(class_of(&head.scope)) {
            out.not_exported += 1;
            continue;
        }
        if tombstones.iter().any(|t| t.covers_scope(&head.scope)) {
            out.tombstoned += 1;
            continue;
        }
        let Some(stream) = pick(by_scope.get(&head.scope), head) else {
            if settled(head.modified_ms) {
                out.findings.push(unknown(head));
            } else {
                out.unsettled += 1;
            }
            continue;
        };
        if stream.deleted_at.is_some() {
            // A root deleted by its own `deleted` record: nothing to certify.
            continue;
        }
        out.checked += 1;
        let (gap, lost) = compare(head, stream, &lossy);
        for finding in [gap, lost].into_iter().flatten() {
            if settled(head.modified_ms) {
                out.records.push(gap_record(stream, &finding));
                out.findings.push(finding);
            } else {
                out.unsettled += 1;
            }
        }
    }

    // Facets the consumer holds with no objects under a root that has some.
    for s in streams {
        let Some(facet) = s.id.facet.as_deref() else {
            continue;
        };
        let scope = scope_of(&s.id.cell, Some(facet));
        if heads.contains_key(&scope) || !heads.contains_key(root_of(&scope)) {
            continue;
        }
        if tombstones.iter().any(|t| t.matches(&s.id)) {
            continue;
        }
        if !settled(s.last_committed_ms) {
            out.unsettled += 1;
            continue;
        }
        let root = streams
            .iter()
            .filter(|r| {
                r.id.facet.is_none()
                    && r.id.cell == s.id.cell
                    && r.id.class == s.id.class
                    && r.id.script == s.id.script
            })
            .max_by_key(|r| r.id.incarnation);
        let finding = Finding {
            stream: s.id.clone(),
            kind: FindingKind::MissingDeleted,
            scope,
            head: None,
            from: None,
            certified: s.certified_head(),
            epochs: Vec::new(),
            detail: format!(
                "facet {facet:?} has no objects in the bucket while its root does; \
                 its deletion was never exported"
            ),
        };
        out.records.push(deleted_record(s, root));
        out.findings.push(finding);
    }
    out
}

/// The consumer stream a bucket scope belongs to. A root's incarnation is
/// its first epoch, so the stream is the newest incarnation at or below the
/// head's epoch, the rule `recovered` records follow too. A facet's
/// incarnation is random; an older incarnation of the same path was removed
/// by its `deleted` record, so the newest by activity is the live one.
fn pick<'a>(
    candidates: Option<&Vec<&'a StreamSummary>>,
    head: &BucketHead,
) -> Option<&'a StreamSummary> {
    let candidates = candidates?;
    if head.is_facet() {
        return candidates
            .iter()
            .copied()
            .max_by_key(|s| s.last_committed_ms);
    }
    candidates
        .iter()
        .copied()
        .filter(|s| s.id.incarnation <= head.epoch)
        .max_by_key(|s| (s.id.incarnation, s.last_committed_ms))
}

/// The gap and lost findings for one cell.
fn compare(
    head: &BucketHead,
    stream: &StreamSummary,
    lossy: &[&str],
) -> (Option<Finding>, Option<Finding>) {
    let first = head.spans.first().map_or(head.epoch, |s| s.epoch);
    let newest = head.spans.last().map_or(head.epoch, |s| s.epoch);
    let mut short = Vec::new();
    let mut ahead = Vec::new();
    // Where the lost changes start: the end the chain gives the epoch.
    let mut ahead_from = BTreeMap::new();
    for span in &head.spans {
        // A root's stream starts at its incarnation; earlier epochs belong to
        // an earlier incarnation of the scope.
        if !head.is_facet() && span.epoch < stream.id.incarnation {
            continue;
        }
        let certified = stream.certified.get(&span.epoch).map(|p| p.txid);
        if certified.unwrap_or(0) < span.hi && !stream.covered(span.epoch, span.hi) {
            short.push(span.epoch);
        }
        if let Some(c) = certified.filter(|c| *c > span.hi) {
            let closed = span.epoch != newest;
            let dropped = stream
                .nodes
                .get(&span.epoch)
                .is_some_and(|nodes| nodes.iter().any(|n| lossy.contains(&n.as_str())));
            if (closed || dropped) && !stream.covered(span.epoch, c) {
                ahead.push(span.epoch);
                ahead_from.insert(span.epoch, span.hi);
            }
        }
    }
    // Certified epochs inside the chain's range that the chain does not
    // follow: a fenced owner's writes that are in no restorable history.
    for (epoch, p) in &stream.certified {
        if *epoch > first
            && *epoch < newest
            && head.span(*epoch).is_none()
            && !stream.covered(*epoch, p.txid)
        {
            ahead.push(*epoch);
        }
    }
    ahead.sort_unstable();
    let head_position = Position::new(head.epoch, head.txid, u64::MAX);
    let certified_at = |epoch: u64| stream.certified.get(&epoch).copied();
    let gap = (!short.is_empty()).then(|| Finding {
        stream: stream.id.clone(),
        kind: FindingKind::Gap,
        scope: head.scope.clone(),
        head: Some(head_position),
        from: Some(certified_at(short[0]).unwrap_or(Position::new(short[0], 0, 0))),
        certified: certified_at(short[0]),
        detail: format!("the bucket holds epoch(s) {short:?} past what the consumer certified"),
        epochs: short,
    });
    let lost = (!ahead.is_empty()).then(|| Finding {
        stream: stream.id.clone(),
        kind: FindingKind::Lost,
        scope: head.scope.clone(),
        head: Some(head_position),
        from: Some(Position::new(
            ahead[0],
            ahead_from.get(&ahead[0]).copied().unwrap_or(0),
            u64::MAX,
        )),
        certified: certified_at(ahead[ahead.len() - 1]),
        detail: format!(
            "the consumer certified changes in epoch(s) {ahead:?} that the cell's \
             history in the bucket does not hold"
        ),
        epochs: ahead,
    });
    (gap, lost)
}

fn unknown(head: &BucketHead) -> Finding {
    let root = root_of(&head.scope);
    let facet = head
        .scope
        .strip_prefix(root)
        .and_then(|rest| rest.strip_prefix('/'))
        .map(str::to_string);
    Finding {
        stream: StreamId {
            script: String::new(),
            class: class_of(&head.scope).to_string(),
            cell: root.to_string(),
            facet,
            incarnation: 0,
        },
        kind: FindingKind::UnknownStream,
        scope: head.scope.clone(),
        head: Some(Position::new(head.epoch, head.txid, u64::MAX)),
        from: None,
        certified: None,
        epochs: head.spans.iter().map(|s| s.epoch).collect(),
        detail: "the bucket holds a cell the consumer has never seen; backfill it".into(),
    }
}

/// `committed_at` is the stream's newest, so a later run that finds the
/// same difference writes an identical record.
fn envelope(stream: &StreamId, position: Position, committed_at: i64) -> Envelope {
    Envelope {
        stream: stream.clone(),
        cell_name: None,
        position,
        committed_at,
        node: AUDIT_NODE.to_string(),
        origin: Origin::Repair,
        fragment: 1,
        fragments: 1,
    }
}

/// A `gap` from what the consumer certified to the bucket head. Its
/// position is the head, so a repair snapshot at the head covers it, and it
/// is stable across runs until the head moves.
fn gap_record(stream: &StreamSummary, finding: &Finding) -> Record {
    let head = finding.head.expect("gap and lost findings carry the head");
    let from = finding.from.expect("gap and lost findings carry a start");
    let to = match finding.kind {
        // Lost changes are covered only by a snapshot past them.
        FindingKind::Lost => stream.certified_head().map_or(head, |c| c.max(head)),
        _ => head,
    };
    let reason = format!("reconciler: {}: {}", finding.kind.as_str(), finding.detail);
    Record {
        envelope: envelope(&stream.id, to, stream.last_committed_ms),
        body: Body::Gap(GapBody { from, to, reason }),
    }
}

/// A `deleted` naming the facet, on its root's stream at the root's
/// certified head. A consumer removes the facet stream whatever the
/// position; the position only orders it within the root's stream.
fn deleted_record(facet: &StreamSummary, root: Option<&StreamSummary>) -> Record {
    let (stream, position) = match root {
        Some(root) => (root.id.clone(), root.certified_head().unwrap_or_default()),
        None => (
            StreamId {
                facet: None,
                incarnation: 0,
                ..facet.id.clone()
            },
            Position::default(),
        ),
    };
    Record {
        envelope: envelope(&stream, position, facet.last_committed_ms),
        body: Body::Deleted(DeletedBody {
            facet: facet.id.facet.clone(),
            incarnation: Some(facet.id.incarnation),
            subtree: false,
        }),
    }
}
