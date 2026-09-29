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
//! - **Unrestorable.** The bucket holds objects for a cell that form no
//!   restorable chain. Such a cell is present, not deleted, so it never
//!   yields a `deleted` record.
//!
//! The bucket runs behind the fleet (tails not yet folded, rows not yet
//! flushed) and the consumer runs behind the bucket sink, so a difference
//! counts only once it has settled: the evidence it rests on is older than
//! [`Options::settle_ms`]. The evidence is the difference's own, never the
//! cell's newest object, so a cell that keeps writing cannot defer an old
//! gap forever: for a gap, when the first change the consumer lacks reached
//! the bucket; for lost changes, when the chain moved past them (the next
//! epoch's first object) or when the loss was declared; for an unknown or
//! unrestorable cell, its first object. Tombstoned cells are skipped, and
//! so are classes the export does not cover.
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

/// `broken` is [`super::inventory::Inventory::broken`]: scopes with objects
/// but no head.
#[allow(clippy::too_many_arguments)]
pub fn reconcile(
    heads: &BTreeMap<String, BucketHead>,
    broken: &BTreeMap<String, i64>,
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
    // Per node, when its earliest loss was declared.
    let mut lossy: BTreeMap<&str, i64> = BTreeMap::new();
    for loss in losses {
        let at = lossy.entry(loss.node()).or_insert(loss.modified_ms);
        *at = (*at).min(loss.modified_ms);
    }
    let present = |scope: &str| heads.contains_key(scope) || broken.contains_key(scope);

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
            if settled(head.first_landed_ms()) {
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
        for (finding, evidence_ms) in compare(head, stream, &lossy) {
            if settled(evidence_ms) {
                out.records.push(gap_record(stream, &finding));
                out.findings.push(finding);
            } else {
                out.unsettled += 1;
            }
        }
    }

    for (scope, first_ms) in broken {
        if !exports_class(class_of(scope)) {
            out.not_exported += 1;
            continue;
        }
        if tombstones.iter().any(|t| t.covers_scope(scope)) {
            out.tombstoned += 1;
            continue;
        }
        if !settled(*first_ms) {
            out.unsettled += 1;
            continue;
        }
        let stream = by_scope
            .get(scope)
            .and_then(|c| c.iter().max_by_key(|s| s.last_committed_ms))
            .map(|s| s.id.clone())
            .unwrap_or_else(|| scope_stream(scope));
        out.findings.push(Finding {
            stream,
            kind: FindingKind::Unrestorable,
            scope: scope.clone(),
            head: None,
            from: None,
            certified: None,
            epochs: Vec::new(),
            detail: "the bucket holds objects for the cell that form no restorable chain".into(),
        });
    }

    // Facets the consumer holds with no objects at all under a root that
    // has some. Objects that merely fail to restore are not an absence.
    for s in streams {
        let Some(facet) = s.id.facet.as_deref() else {
            continue;
        };
        let scope = scope_of(&s.id.cell, Some(facet));
        if present(&scope) || !present(root_of(&scope)) {
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

/// The gap and lost findings for one cell, each with the time of the
/// evidence it rests on.
fn compare(
    head: &BucketHead,
    stream: &StreamSummary,
    lossy: &BTreeMap<&str, i64>,
) -> Vec<(Finding, i64)> {
    let first = head.spans.first().map_or(head.epoch, |s| s.epoch);
    let newest = head.spans.last().map_or(head.epoch, |s| s.epoch);
    // When the chain moved past `epoch`: the next span's first object.
    let moved_on = |epoch: u64| {
        head.spans
            .iter()
            .find(|s| s.epoch > epoch)
            .and_then(|next| head.landed_ms(next.epoch, next.lo))
            .unwrap_or(head.modified_ms)
    };
    let mut short = Vec::new();
    let mut short_ms = i64::MAX;
    let mut ahead = Vec::new();
    let mut ahead_ms = i64::MAX;
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
            // The first change the consumer lacks.
            let missing = certified.map_or(span.lo, |c| (c + 1).max(span.lo));
            let landed = head
                .landed_ms(span.epoch, missing)
                .unwrap_or(head.modified_ms);
            short_ms = short_ms.min(landed);
        }
        if let Some(c) = certified.filter(|c| *c > span.hi) {
            let evidence = if span.epoch != newest {
                Some(moved_on(span.epoch))
            } else {
                stream
                    .nodes
                    .get(&span.epoch)
                    .into_iter()
                    .flatten()
                    .filter_map(|n| lossy.get(n.as_str()).copied())
                    .min()
            };
            if let Some(ms) = evidence.filter(|_| !stream.covered(span.epoch, c)) {
                ahead.push(span.epoch);
                ahead_from.insert(span.epoch, span.hi);
                ahead_ms = ahead_ms.min(ms);
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
            ahead_ms = ahead_ms.min(moved_on(*epoch));
        }
    }
    ahead.sort_unstable();
    let head_position = Position::new(head.epoch, head.txid, u64::MAX);
    let certified_at = |epoch: u64| stream.certified.get(&epoch).copied();
    let mut out = Vec::new();
    if !short.is_empty() {
        out.push((
            Finding {
                stream: stream.id.clone(),
                kind: FindingKind::Gap,
                scope: head.scope.clone(),
                head: Some(head_position),
                from: Some(certified_at(short[0]).unwrap_or(Position::new(short[0], 0, 0))),
                certified: certified_at(short[0]),
                detail: format!(
                    "the bucket holds epoch(s) {short:?} past what the consumer certified"
                ),
                epochs: short,
            },
            short_ms,
        ));
    }
    if !ahead.is_empty() {
        out.push((
            Finding {
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
            },
            ahead_ms,
        ));
    }
    out
}

/// The stream a bucket scope names when the consumer holds none: no script,
/// incarnation 0, and a facet by its bucket path, `facets/<hash>...`.
fn scope_stream(scope: &str) -> StreamId {
    let root = root_of(scope);
    StreamId {
        script: String::new(),
        class: class_of(scope).to_string(),
        cell: root.to_string(),
        facet: scope
            .strip_prefix(root)
            .and_then(|rest| rest.strip_prefix('/'))
            .map(str::to_string),
        incarnation: 0,
    }
}

fn unknown(head: &BucketHead) -> Finding {
    Finding {
        stream: scope_stream(&head.scope),
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
            through_incarnation: None,
        }),
    }
}
