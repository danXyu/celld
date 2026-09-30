// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Performance statistics: counters and latency histograms for the hot paths.
//!
//! These instruments let a benchmark read what a node did, not only how long
//! its client waited: how many bucket requests a warm read made, how long a
//! write waited for its proof and which proof released it, how late the core
//! thread ran its timers. `GET /debug/metrics` on the internal listener
//! returns them all as one JSON snapshot ([`snapshot`]); a harness reads it
//! before and after a phase and subtracts.
//!
//! They are always compiled and always on, so an operator's node and a
//! benchmark's node count the same things. Every instrument is a fixed slot
//! in a static array, so an observation takes no lock and allocates nothing:
//! a counter is one relaxed `fetch_add`, a histogram four. A call site that
//! did not already read the monotonic clock reads it once more.
//!
//! Histograms are log-linear: each power of two splits into 16 buckets, so a
//! value lands in a bucket at most 6.25% wider than itself, from 1 to
//! `u64::MAX` in 976 buckets. The snapshot carries the raw bucket counts, so
//! the difference of two snapshots is itself a histogram and its percentiles
//! are exact to that resolution. [`bucket_lower_bound`] is the layout.
//!
//! Nothing here informs a decision. The core never reads these values.

use serde_json::json;
use serde_json::Map;
use serde_json::Value;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

/// Sub-bucket bits per power of two.
pub const SUB_BUCKET_BITS: u32 = 4;
const SUB_BUCKETS: u64 = 1 << SUB_BUCKET_BITS;
/// Buckets in every histogram: 16 for the values below 16, then 16 for
/// each of the 60 remaining powers of two.
pub const BUCKETS: usize = (64 - SUB_BUCKET_BITS as usize + 1) * SUB_BUCKETS as usize;

/// The bucket a value lands in.
pub fn bucket_index(value: u64) -> usize {
    if value < SUB_BUCKETS {
        return value as usize;
    }
    let exponent = 63 - value.leading_zeros();
    let sub = (value >> (exponent - SUB_BUCKET_BITS)) & (SUB_BUCKETS - 1);
    ((u64::from(exponent - SUB_BUCKET_BITS) + 1) * SUB_BUCKETS + sub) as usize
}

/// The smallest value that lands in bucket `index`.
pub fn bucket_lower_bound(index: usize) -> u64 {
    let index = index as u64;
    if index < SUB_BUCKETS {
        return index;
    }
    let group = index / SUB_BUCKETS;
    let sub = index % SUB_BUCKETS;
    (SUB_BUCKETS + sub) << (group - 1)
}

/// The largest value that lands in bucket `index`.
pub fn bucket_upper_bound(index: usize) -> u64 {
    if index + 1 >= BUCKETS {
        return u64::MAX;
    }
    bucket_lower_bound(index + 1) - 1
}

/// One log-linear histogram.
pub struct Histogram {
    counts: [AtomicU64; BUCKETS],
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

impl Histogram {
    pub const fn new() -> Self {
        Self {
            counts: [const { AtomicU64::new(0) }; BUCKETS],
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    pub fn record(&self, value: u64) {
        self.counts[bucket_index(value)].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(value, Ordering::Relaxed);
        self.max.fetch_max(value, Ordering::Relaxed);
    }

    pub fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// The value at quantile `q` (0 to 1), as the midpoint of its bucket.
    /// Concurrent observations can make the counts and the total disagree
    /// by a few; the answer is then off by at most one bucket.
    pub fn quantile(&self, q: f64) -> u64 {
        let counts: Vec<u64> = self
            .counts
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect();
        quantile_of(&counts, q)
    }

    fn json(&self) -> Value {
        let counts: Vec<u64> = self
            .counts
            .iter()
            .map(|count| count.load(Ordering::Relaxed))
            .collect();
        let buckets: Vec<Value> = counts
            .iter()
            .enumerate()
            .filter(|(_, count)| **count > 0)
            .map(|(index, count)| json!([index, count]))
            .collect();
        json!({
            "count": self.count.load(Ordering::Relaxed),
            "sum": self.sum.load(Ordering::Relaxed),
            "max": self.max.load(Ordering::Relaxed),
            "p50": quantile_of(&counts, 0.50),
            "p90": quantile_of(&counts, 0.90),
            "p99": quantile_of(&counts, 0.99),
            "p999": quantile_of(&counts, 0.999),
            "buckets": buckets,
        })
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// The midpoint of the bucket holding quantile `q` of `counts`, which is
/// indexed by bucket. Zero for an empty histogram.
pub fn quantile_of(counts: &[u64], q: f64) -> u64 {
    let total: u64 = counts.iter().sum();
    if total == 0 {
        return 0;
    }
    let rank = ((q.clamp(0.0, 1.0) * total as f64).ceil() as u64).max(1);
    let mut seen = 0;
    for (index, count) in counts.iter().enumerate() {
        seen += count;
        if seen >= rank {
            let low = bucket_lower_bound(index);
            let high = bucket_upper_bound(index);
            return low + (high - low) / 2;
        }
    }
    bucket_lower_bound(counts.len().saturating_sub(1))
}

/// Declares an instrument enum with a stable label for each variant.
macro_rules! instruments {
    (
        $(#[$meta:meta])*
        pub enum $name:ident {
            $($(#[$variant_meta:meta])* $variant:ident => $label:literal,)*
        }
    ) => {
        $(#[$meta])*
        #[derive(Clone, Copy, Debug, PartialEq, Eq)]
        #[repr(usize)]
        pub enum $name {
            $($(#[$variant_meta])* $variant,)*
        }

        impl $name {
            pub const ALL: &'static [$name] = &[$($name::$variant,)*];

            pub const fn label(self) -> &'static str {
                match self {
                    $($name::$variant => $label,)*
                }
            }
        }
    };
}

instruments! {
    /// A latency or size distribution. A label ends in its unit.
    pub enum Hist {
        /// A stateless Worker fetch, from admission queue to completion.
        StatelessFetch => "request.stateless_us",
        /// A stateless Worker fetch's wait for an isolate.
        StatelessQueue => "request.stateless_queue_us",
        /// The core's route for a cell request that was not already
        /// resident: ownership reads, activation, and restore.
        CellRoute => "request.cell_route_us",
        /// One WebSocket message's route to its cell.
        WebSocketRoute => "websocket.route_us",
        /// One output gate, from submission to the core's answer.
        GateWait => "gate.wait_us",
        /// A durability wait that a follower ensemble released.
        ProofFleet => "durability.proof_fleet_us",
        /// A durability wait that a bucket upload released.
        ProofBucket => "durability.proof_bucket_us",
        /// An activation that started an empty database.
        ActivationFresh => "activation.fresh_us",
        /// An activation that reopened a database already on this node.
        ActivationLocal => "activation.local_us",
        /// An activation that downloaded its database from the bucket.
        ActivationDownload => "activation.download_us",
        /// An activation that opened a paged database, faulting pages in.
        ActivationPaged => "activation.paged_us",
        /// One cell isolate's startup, from placement to ready.
        IsolateStartup => "isolate.startup_us",
        /// One Worker load: a new V8 isolate and the bundle's compile.
        WorkerLoad => "isolate.worker_load_us",
        /// One fleet ship round, from submission to its last append.
        ShipRound => "log.ship_round_us",
        /// Cells whose writes one ship round carried.
        ShipRoundCells => "log.ship_round_cells",
        /// Bytes one ship round carried.
        ShipRoundBytes => "log.ship_round_bytes",
        /// The capture phase of one ship round.
        CaptureTotal => "capture.total_us",
        /// LTX encoding and file writes inside one round's captures.
        CaptureEncode => "capture.encode_us",
        /// Local fsyncs inside one round's captures.
        CaptureFsync => "capture.fsync_us",
        /// One follower batch's file write.
        FollowerWrite => "log.follower_write_us",
        /// One follower batch's file and directory fsyncs.
        FollowerFsync => "log.follower_fsync_us",
        /// One node bundle upload.
        BundleFlush => "log.bundle_flush_us",
        /// One cell handoff between nodes, all phases.
        Handoff => "handoff.total_us",
        /// How late the core thread ran a timer. It grows when the core's
        /// mailbox holds more work than its one thread can do.
        CoreLag => "loop.core_lag_us",
        /// How late the node's main loop ran a timer. The main loop polls
        /// WebSocket pumps and DO, gate, service, and queue calls itself.
        MainLag => "loop.main_lag_us",
    }
}

instruments! {
    /// A monotonic count.
    pub enum Counter {
        /// Every message the core thread handled.
        CoreMessages => "core.messages",
        /// Core `Request` messages: one route decision each.
        CoreRequests => "core.requests",
        /// Core `Output` messages: one output gate each.
        CoreOutputs => "core.outputs",
    }
}

instruments! {
    /// One kind of object-store request.
    pub enum BucketOp {
        Get => "get",
        GetRange => "get_range",
        Head => "head",
        Put => "put",
        /// A put that must not overwrite (`PutMode::Create`).
        PutCreate => "put_create",
        /// A put conditional on a version (`PutMode::Update`).
        PutUpdate => "put_update",
        /// One multipart upload, counted when it starts.
        Multipart => "multipart",
        /// One listing call. A long listing can span several requests.
        List => "list",
        ListDelimited => "list_delimited",
        ListPaginated => "list_paginated",
        Delete => "delete",
        Copy => "copy",
    }
}

instruments! {
    /// The kind of key a request touched, from its first known segment.
    pub enum KeyClass {
        /// `cells/<cell>/own.json`: ownership records.
        CellOwner => "cell_owner",
        /// Everything else under `cells/`: LTX files, facets, snapshots.
        CellData => "cell_data",
        /// `nodes/`: node leases.
        Nodes => "nodes",
        /// `fleet/`: capacity samples and fleet singletons.
        Fleet => "fleet",
        /// `log/<node>/bundle/`: node log bundles.
        LogBundle => "log_bundle",
        /// The rest of `log/`: recovery claims and tails.
        Log => "log",
        Wake => "wake",
        /// `deploy/` and `deploy-blobs/`.
        Deploy => "deploy",
        R2 => "r2",
        Kv => "kv",
        Export => "export",
        Telemetry => "telemetry",
        Drain => "drain",
        Probe => "probe",
        Other => "other",
    }
}

instruments! {
    /// How a request ended.
    pub enum Outcome {
        Ok => "ok",
        NotFound => "not_found",
        /// A failed condition: precondition, already exists, not modified.
        Precondition => "precondition",
        /// A 429, a 503, or an S3 SlowDown.
        Throttled => "throttled",
        Error => "error",
    }
}

const HISTS: usize = Hist::ALL.len();
const COUNTERS: usize = Counter::ALL.len();
const OPS: usize = BucketOp::ALL.len();
const CLASSES: usize = KeyClass::ALL.len();
const OUTCOMES: usize = Outcome::ALL.len();

static HISTOGRAMS: [Histogram; HISTS] = [const { Histogram::new() }; HISTS];
static COUNTS: [AtomicU64; COUNTERS] = [const { AtomicU64::new(0) }; COUNTERS];
static BUCKET_REQUESTS: [[[AtomicU64; OUTCOMES]; CLASSES]; OPS] =
    [const { [const { [const { AtomicU64::new(0) }; OUTCOMES] }; CLASSES] }; OPS];
static BUCKET_BYTES: [[AtomicU64; CLASSES]; OPS] =
    [const { [const { AtomicU64::new(0) }; CLASSES] }; OPS];
static BUCKET_LATENCY: [Histogram; OPS] = [const { Histogram::new() }; OPS];

/// Record one observation.
pub fn record(hist: Hist, value: u64) {
    HISTOGRAMS[hist as usize].record(value);
}

/// Add one to a counter.
pub fn count(counter: Counter) {
    add(counter, 1);
}

pub fn add(counter: Counter, amount: u64) {
    COUNTS[counter as usize].fetch_add(amount, Ordering::Relaxed);
}

pub fn histogram(hist: Hist) -> &'static Histogram {
    &HISTOGRAMS[hist as usize]
}

pub fn counter(counter: Counter) -> u64 {
    COUNTS[counter as usize].load(Ordering::Relaxed)
}

/// Record one finished object-store request.
pub fn bucket_request(
    op: BucketOp,
    class: KeyClass,
    outcome: Outcome,
    bytes: u64,
    latency_us: u64,
) {
    BUCKET_REQUESTS[op as usize][class as usize][outcome as usize].fetch_add(1, Ordering::Relaxed);
    if bytes > 0 {
        BUCKET_BYTES[op as usize][class as usize].fetch_add(bytes, Ordering::Relaxed);
    }
    BUCKET_LATENCY[op as usize].record(latency_us);
}

/// Every object-store request so far with this op, class, and outcome.
pub fn bucket_requests(op: BucketOp, class: KeyClass, outcome: Outcome) -> u64 {
    BUCKET_REQUESTS[op as usize][class as usize][outcome as usize].load(Ordering::Relaxed)
}

/// Every object-store request so far, of any kind.
pub fn bucket_requests_total() -> u64 {
    BUCKET_REQUESTS
        .iter()
        .flatten()
        .flatten()
        .map(|count| count.load(Ordering::Relaxed))
        .sum()
}

/// The class of a bucket key, which may carry a fleet prefix: the first
/// segment that names a known root decides.
pub fn key_class(key: &str) -> KeyClass {
    let mut segments = key.split('/');
    while let Some(segment) = segments.next() {
        let class = match segment {
            "cells" => {
                // `cells/<cell>/own.json`; the cell name is one segment.
                let _cell = segments.next();
                return match segments.next() {
                    Some("own.json") if segments.next().is_none() => KeyClass::CellOwner,
                    _ => KeyClass::CellData,
                };
            }
            "nodes" => KeyClass::Nodes,
            "fleet" => KeyClass::Fleet,
            "log" => {
                let _node = segments.next();
                return match segments.next() {
                    Some("bundle") => KeyClass::LogBundle,
                    _ => KeyClass::Log,
                };
            }
            "wake" => KeyClass::Wake,
            "deploy" | "deploy-blobs" => KeyClass::Deploy,
            "r2" => KeyClass::R2,
            "kv" => KeyClass::Kv,
            "export" => KeyClass::Export,
            "telemetry" => KeyClass::Telemetry,
            "drain" => KeyClass::Drain,
            "probe" => KeyClass::Probe,
            _ => continue,
        };
        return class;
    }
    KeyClass::Other
}

/// The outcome an object-store result reports.
pub fn outcome_of<T>(result: &object_store::Result<T>) -> Outcome {
    match result {
        Ok(_) => Outcome::Ok,
        Err(object_store::Error::NotFound { .. }) => Outcome::NotFound,
        Err(
            object_store::Error::Precondition { .. }
            | object_store::Error::AlreadyExists { .. }
            | object_store::Error::NotModified { .. },
        ) => Outcome::Precondition,
        Err(error) => {
            let message = error.to_string();
            if [
                "429",
                "503",
                "SlowDown",
                "TooManyRequests",
                "Too Many Requests",
            ]
            .iter()
            .any(|needle| message.contains(needle))
            {
                Outcome::Throttled
            } else {
                Outcome::Error
            }
        }
    }
}

/// The snapshot `GET /debug/metrics` serves.
pub fn snapshot() -> Value {
    let counters: Map<String, Value> = Counter::ALL
        .iter()
        .map(|counter| (counter.label().to_string(), json!(self::counter(*counter))))
        .collect();
    let histograms: Map<String, Value> = Hist::ALL
        .iter()
        .map(|hist| (hist.label().to_string(), histogram(*hist).json()))
        .collect();
    let mut requests = Vec::new();
    let mut bytes = Vec::new();
    for op in BucketOp::ALL {
        for class in KeyClass::ALL {
            for outcome in Outcome::ALL {
                let count = bucket_requests(*op, *class, *outcome);
                if count > 0 {
                    requests.push(json!({
                        "op": op.label(),
                        "class": class.label(),
                        "outcome": outcome.label(),
                        "count": count,
                    }));
                }
            }
            let total = BUCKET_BYTES[*op as usize][*class as usize].load(Ordering::Relaxed);
            if total > 0 {
                bytes.push(json!({
                    "op": op.label(),
                    "class": class.label(),
                    "bytes": total,
                }));
            }
        }
    }
    let latency: Map<String, Value> = BucketOp::ALL
        .iter()
        .filter(|op| BUCKET_LATENCY[**op as usize].count() > 0)
        .map(|op| (op.label().to_string(), BUCKET_LATENCY[*op as usize].json()))
        .collect();
    json!({
        "schema": "celld.perf.v1",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_us": crate::asyncrt::mono_us(),
        "sub_bucket_bits": SUB_BUCKET_BITS,
        "counters": counters,
        "histograms": histograms,
        "bucket": {
            "requests": requests,
            "bytes": bytes,
            "latency_us": latency,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_contiguous_and_ordered() {
        let mut previous = None;
        for index in 0..BUCKETS {
            let low = bucket_lower_bound(index);
            assert_eq!(bucket_index(low), index, "lower bound of {index}");
            let high = bucket_upper_bound(index);
            assert_eq!(bucket_index(high), index, "upper bound of {index}");
            if let Some(previous) = previous {
                assert_eq!(low, previous + 1, "gap before {index}");
            }
            previous = Some(high);
        }
        assert_eq!(previous, Some(u64::MAX));
    }

    #[test]
    fn a_bucket_is_at_most_a_sixteenth_of_its_values() {
        for index in SUB_BUCKETS as usize..BUCKETS - 1 {
            let low = bucket_lower_bound(index);
            let width = bucket_upper_bound(index) - low + 1;
            assert!(width * 16 <= low, "bucket {index} is {width} wide at {low}");
        }
    }

    #[test]
    fn quantiles_land_in_the_right_bucket() {
        let histogram = Histogram::new();
        for value in 1..=1000 {
            histogram.record(value);
        }
        let p50 = histogram.quantile(0.5);
        assert!((470..=530).contains(&p50), "p50 {p50}");
        let p99 = histogram.quantile(0.99);
        assert!((930..=1050).contains(&p99), "p99 {p99}");
        assert_eq!(Histogram::new().quantile(0.5), 0);
    }

    #[test]
    fn keys_classify_through_a_fleet_prefix() {
        assert_eq!(key_class("cells/room-1/own.json"), KeyClass::CellOwner);
        assert_eq!(
            key_class("prod/eu/cells/room-1/own.json"),
            KeyClass::CellOwner
        );
        assert_eq!(
            key_class("cells/room-1/ltx/e3/0000/a.ltx"),
            KeyClass::CellData
        );
        assert_eq!(key_class("cells/own.json/own.json/x"), KeyClass::CellData);
        assert_eq!(key_class("nodes/node-a.json"), KeyClass::Nodes);
        assert_eq!(
            key_class("log/node-a/bundle/e1-2.ltxb"),
            KeyClass::LogBundle
        );
        assert_eq!(key_class("log/node-a/claim.json"), KeyClass::Log);
        assert_eq!(
            key_class("deploy-blobs/assets/sha256/ab/cd"),
            KeyClass::Deploy
        );
        assert_eq!(key_class("r2/photos/cat.jpg"), KeyClass::R2);
        assert_eq!(key_class("anything/else"), KeyClass::Other);
    }
}
