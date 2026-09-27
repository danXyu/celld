// Copyright 2026 Deno Land Inc. Apache-2.0 license.

// Metrics are observational and do not affect Actor decisions.
#![allow(clippy::disallowed_methods)]

//! Metrics: node load and fleet-wide per-cell distributions, as OTLP.
//!
//! The third telemetry signal, beside spans and logs, and bound by the same
//! rules. Off is structural: without a collector (`CELLD_OTEL=<url>`) no
//! sampler runs and no CPU account exists, so a cell turn reads no clock it
//! would not otherwise read. The hot path never blocks: a turn adds to an
//! atomic, and a sampler that finds an isolate busy skips it, counted.
//!
//! Every interval the sampler exports one snapshot:
//!
//! - node gauges, read from the same `/state` snapshot an operator reads,
//!   so the two surfaces cannot disagree;
//! - `celld.cell.cpu_time`, one observation per cell that ran any JS in
//!   the interval: that cell's thread CPU, in seconds;
//! - `celld.cell.heap_bytes`, one observation per resident cell: its
//!   isolate's V8 heap divided by the cells sharing the isolate.
//!
//! No data point carries a cell, script, or request attribute. A series is
//! a node, so cardinality grows with the fleet, not with its tenants; the
//! distributions are exponential histograms a backend merges across nodes
//! into fleet-wide percentiles without anyone choosing bucket bounds.

use std::collections::HashMap;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::OnceLock;
use std::time::SystemTime;
use std::time::UNIX_EPOCH;

/// One export in flight, retrying, and one queued behind it. A snapshot that
/// finds both taken is shed and counted, as a full telemetry channel sheds.
const EXPORT_QUEUE: usize = 1;

// ---------------------------------------------------------------------------
// CPU per cell

/// Per-cell thread CPU, accumulated by the turns and drained by the sampler.
///
/// An event resolves its cell's account once, when it starts, and each turn
/// then costs one relaxed `fetch_add`. The map is touched per event, never
/// per turn. An account the map alone holds, and that is empty, belongs to a
/// cell with no event in flight; `take` forgets it, so a cell that left the
/// node leaves nothing behind.
#[derive(Default)]
pub struct CellCpu {
    cells: Mutex<HashMap<String, Arc<AtomicU64>>>,
}

impl CellCpu {
    /// The account for `cell`, created on first use.
    pub fn account(&self, cell: &str) -> Arc<AtomicU64> {
        let mut cells = self.cells.lock().unwrap();
        if let Some(account) = cells.get(cell) {
            return account.clone();
        }
        let account = Arc::new(AtomicU64::new(0));
        cells.insert(cell.to_string(), account.clone());
        account
    }

    /// Drain every account: each nonzero total, in nanoseconds, is one
    /// observation, and every account restarts at zero. A cell with no turn
    /// since the last drain contributes nothing.
    pub fn take(&self) -> Vec<u64> {
        let mut cells = self.cells.lock().unwrap();
        let mut totals = Vec::new();
        cells.retain(|_, account| {
            let total = account.swap(0, Ordering::Relaxed);
            if total > 0 {
                totals.push(total);
            }
            // Under the map lock no event can be resolving this account, so
            // a count of one means no event holds it either and no turn can
            // add to it again. An account still held stays, and a turn that
            // lands after the swap is reported next interval.
            Arc::strong_count(account) > 1
        });
        totals
    }

    #[doc(hidden)]
    pub fn len(&self) -> usize {
        self.cells.lock().unwrap().len()
    }

    #[doc(hidden)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

static CELL_CPU: OnceLock<CellCpu> = OnceLock::new();

/// Turn on per-cell CPU accounting. Called once, by telemetry init, before
/// any cell event starts.
pub(crate) fn enable_cell_cpu() {
    let _ = CELL_CPU.set(CellCpu::default());
}

/// Whether cell events account their CPU. `false` unless metrics are on.
pub(crate) fn cell_cpu_enabled() -> bool {
    CELL_CPU.get().is_some()
}

/// The account a cell event's turns add to; `None` when metrics are off.
pub(crate) fn cell_cpu_account(cell: &str) -> Option<Arc<AtomicU64>> {
    Some(CELL_CPU.get()?.account(cell))
}

// ---------------------------------------------------------------------------
// Heap per cell

/// One cell-pool isolate's V8 heap and the cells sharing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IsolateHeap {
    pub cells: usize,
    /// `total_physical_size + external_memory`.
    pub bytes: u64,
}

/// The heap sample of every cell-pool isolate a pass could read.
#[derive(Debug, Default)]
pub struct CellHeaps {
    pub isolates: Vec<IsolateHeap>,
    /// Isolates with resident cells that a turn held at the sample. Reading
    /// one would mean waiting on user code, so the pass skips it.
    pub skipped: u64,
}

impl CellHeaps {
    /// Sample isolates behind their pool gates without waiting on any.
    ///
    /// Each item is an isolate's resident cell count and the async gate a
    /// turn holds. An isolate with no cells is not a cell's heap and is
    /// passed over; a gate that is taken is counted in `skipped`; `read`
    /// answers `None` for an isolate already freed. Holding the gate is the
    /// permit a turn holds, which is what makes the V8 lock inside `read`
    /// uncontended.
    pub fn sample<'a, T: 'a>(
        &mut self,
        isolates: impl IntoIterator<Item = (usize, &'a tokio::sync::Mutex<T>)>,
        read: impl Fn(&T) -> Option<u64>,
    ) {
        for (cells, gate) in isolates {
            if cells == 0 {
                continue;
            }
            match gate.try_lock() {
                Ok(guard) => {
                    if let Some(bytes) = read(&guard) {
                        self.isolates.push(IsolateHeap { cells, bytes });
                    }
                }
                Err(_) => self.skipped += 1,
            }
        }
    }

    /// One observation per resident cell: its isolate's heap divided evenly
    /// among the cells sharing it.
    ///
    /// An average, because V8 cannot cheaply say which context owns what.
    /// Exact for an isolate holding one cell; for a shared isolate the max
    /// is understated when one heavy cell sits among light ones.
    pub fn per_cell(&self) -> Vec<f64> {
        let mut values = Vec::new();
        for isolate in &self.isolates {
            let share = isolate.bytes as f64 / isolate.cells as f64;
            values.extend(std::iter::repeat_n(share, isolate.cells));
        }
        values
    }
}

// ---------------------------------------------------------------------------
// Node gauges

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum GaugeValue {
    Int(i64),
    Double(f64),
}

#[derive(Clone, Debug, PartialEq)]
pub struct Gauge {
    pub name: &'static str,
    pub description: &'static str,
    pub unit: &'static str,
    pub value: GaugeValue,
}

/// The node gauges in one `/state` snapshot.
///
/// Read from the JSON `/state` serves, not from a second path to the same
/// numbers, so a chart and a `curl` at the same instant agree. A field that
/// is absent or `null` produces no gauge: an unknown is not a zero.
pub fn node_gauges(state: &serde_json::Value) -> Vec<Gauge> {
    use GaugeValue::Double;
    use GaugeValue::Int;

    let top = |key: &str| state.get(key).and_then(serde_json::Value::as_i64);
    let load = |key: &str| {
        state
            .get("node_load")
            .and_then(|load| load.get(key))
            .and_then(serde_json::Value::as_i64)
    };
    let flag = |key: &str| {
        state
            .get("node_load")
            .and_then(|load| load.get(key))
            .and_then(serde_json::Value::as_bool)
            .map(i64::from)
    };

    let mut gauges = Vec::new();
    let mut push = |name, description, unit, value: Option<GaugeValue>| {
        if let Some(value) = value {
            gauges.push(Gauge {
                name,
                description,
                unit,
                value,
            });
        }
    };
    push(
        "celld.node.owned_cells",
        "Cells this node owns.",
        "{cell}",
        top("owned_cells").map(Int),
    );
    push(
        "celld.node.resident_cells",
        "Cells resident in memory.",
        "{cell}",
        load("resident_cells").map(Int),
    );
    push(
        "celld.node.occupied",
        "Residency slots in use.",
        "{cell}",
        top("occupied").map(Int),
    );
    push(
        "celld.node.restoring",
        "Cells restoring or queued to restore.",
        "{cell}",
        top("restoring").map(Int),
    );
    push(
        "celld.node.capacity_waiting",
        "Cells queued behind the residency ceiling.",
        "{cell}",
        top("capacity_waiting").map(Int),
    );
    push(
        "celld.node.activation_waiting",
        "Cells queued behind the activation ceiling.",
        "{cell}",
        top("activation_waiting").map(Int),
    );
    push(
        "celld.node.host_websockets",
        "WebSockets the host holds.",
        "{websocket}",
        load("host_websockets").map(Int),
    );
    push(
        "celld.node.rss_bytes",
        "Process resident set size.",
        "By",
        load("rss_bytes").map(Int),
    );
    push(
        "celld.node.in_use_bytes",
        "Allocator-adjusted memory in use.",
        "By",
        load("in_use_bytes").map(Int),
    );
    push(
        "celld.node.cgroup_working_set_bytes",
        "cgroup memory.current minus inactive file pages.",
        "By",
        load("cgroup_working_set_bytes").map(Int),
    );
    push(
        "celld.node.cpu_utilization",
        "Process CPU as a ratio of one core.",
        "1",
        load("cpu_percent_x100").map(|x100| Double(x100 as f64 / 10_000.0)),
    );
    push(
        "celld.node.open_fds",
        "Open file descriptors.",
        "{fd}",
        load("open_fds").map(Int),
    );
    push(
        "celld.node.fd_limit",
        "The file descriptor limit.",
        "{fd}",
        load("fd_limit").map(Int),
    );
    push(
        "celld.node.pressured",
        "1 while the node is under memory pressure.",
        "1",
        flag("pressured").map(Int),
    );
    push(
        "celld.node.memory_headroom",
        "1 while every memory measurement is under its low watermark.",
        "1",
        flag("memory_headroom").map(Int),
    );
    push(
        "celld.node.draining",
        "1 once the node is draining.",
        "1",
        flag("draining").map(Int),
    );
    push(
        "celld.node.shed_cells",
        "Cells shed since the process started.",
        "{cell}",
        load("shed_cells").map(Int),
    );

    // The cell pools of every generation the node still holds: during a
    // deploy the draining ones house cells that have not swapped yet.
    let deployment = state.get("deployment");
    let pools = deployment
        .and_then(|deployment| deployment.get("isolates"))
        .into_iter()
        .chain(
            deployment
                .and_then(|deployment| deployment.get("draining"))
                .and_then(serde_json::Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|generation| generation.get("isolates")),
        )
        .filter_map(|isolates| isolates.get("cells"))
        .filter_map(serde_json::Value::as_object)
        .flat_map(|pools| pools.values());
    let (mut isolates, mut isolate_cells, mut any) = (0i64, 0i64, false);
    for pool in pools {
        any = true;
        isolates += pool
            .get("live")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
        isolate_cells += pool
            .get("cells")
            .and_then(serde_json::Value::as_i64)
            .unwrap_or(0);
    }
    push(
        "celld.node.isolates",
        "Live cell-pool isolates.",
        "{isolate}",
        (deployment.is_some() || any).then_some(Int(isolates)),
    );
    push(
        "celld.node.isolate_cells",
        "Cells housed across the cell-pool isolates.",
        "{cell}",
        (deployment.is_some() || any).then_some(Int(isolate_cells)),
    );
    gauges
}

// ---------------------------------------------------------------------------
// Exponential histogram

/// The finest resolution the otel SDKs default to, and the bucket budget
/// they default to; a narrower spread keeps the finer scale.
const MAX_SCALE: i32 = 20;
const MIN_SCALE: i32 = -10;
const MAX_BUCKETS: i64 = 160;

/// A delta OTLP exponential histogram over one interval's observations.
///
/// Bucket `i` at scale `s` holds values in `(base^i, base^(i+1)]`, with
/// `base = 2^(2^-s)`. The scale is the finest at which the observed spread
/// fits `MAX_BUCKETS`, which is what an SDK's auto-scaling converges to.
#[derive(Clone, Debug, PartialEq)]
pub struct ExpHistogram {
    pub scale: i32,
    pub zero_count: u64,
    pub offset: i32,
    pub counts: Vec<u64>,
    pub count: u64,
    pub sum: f64,
    pub min: f64,
    pub max: f64,
}

impl ExpHistogram {
    /// `None` for an interval with no observations: no point is better than
    /// a point that claims a min and max of nothing. Negative and non-finite
    /// values are not observations of anything celld measures and are
    /// dropped.
    pub fn from_values(values: &[f64]) -> Option<ExpHistogram> {
        let values: Vec<f64> = values
            .iter()
            .copied()
            .filter(|value| value.is_finite() && *value >= 0.0)
            .collect();
        if values.is_empty() {
            return None;
        }
        let mut zero_count = 0u64;
        let mut indices = Vec::with_capacity(values.len());
        for value in &values {
            if *value == 0.0 {
                zero_count += 1;
            } else {
                indices.push(bucket_index(*value, MAX_SCALE));
            }
        }
        // Downscaling by one halves every index (floor), so the finest scale
        // that fits is found by shifting the extremes.
        let mut shift = 0;
        if let (Some(low), Some(high)) = (indices.iter().min(), indices.iter().max()) {
            while MAX_SCALE - shift > MIN_SCALE
                && (high >> shift) - (low >> shift) + 1 > MAX_BUCKETS
            {
                shift += 1;
            }
        }
        let scale = MAX_SCALE - shift;
        let (offset, counts) = match indices.iter().map(|index| index >> shift).min() {
            Some(low) => {
                let high = indices.iter().map(|index| index >> shift).max().unwrap();
                let mut counts = vec![0u64; (high - low + 1) as usize];
                for index in &indices {
                    counts[((index >> shift) - low) as usize] += 1;
                }
                (low as i32, counts)
            }
            None => (0, Vec::new()),
        };
        Some(ExpHistogram {
            scale,
            zero_count,
            offset,
            counts,
            count: values.len() as u64,
            sum: values.iter().sum(),
            min: values.iter().copied().fold(f64::INFINITY, f64::min),
            max: values.iter().copied().fold(f64::NEG_INFINITY, f64::max),
        })
    }
}

/// The bucket of a positive value: `ceil(log2(v) * 2^scale) - 1`. Exact
/// powers of the base land on their bucket's upper, inclusive bound.
fn bucket_index(value: f64, scale: i32) -> i64 {
    let scaled = value.log2() * 2f64.powi(scale);
    // `log2` of an exact power of two is exact, which is the case the
    // boundary matters most for; elsewhere a last-ulp miss moves a value
    // across a boundary at most one part in a million wide.
    scaled.ceil() as i64 - 1
}

// ---------------------------------------------------------------------------
// Snapshot and export

#[derive(Clone, Debug, PartialEq)]
pub struct Sum {
    pub name: &'static str,
    pub description: &'static str,
    pub unit: &'static str,
    /// The delta over the interval.
    pub value: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Distribution {
    pub name: &'static str,
    pub description: &'static str,
    pub unit: &'static str,
    /// `None` when nothing was observed; the metric is then left out.
    pub histogram: Option<ExpHistogram>,
}

/// One interval's metrics, ready to encode.
#[derive(Clone, Debug, PartialEq)]
pub struct Snapshot {
    /// The start of the delta window: the previous sample.
    pub start_ns: u64,
    pub time_ns: u64,
    pub gauges: Vec<Gauge>,
    pub sums: Vec<Sum>,
    pub distributions: Vec<Distribution>,
}

impl Snapshot {
    pub fn new(
        start_ns: u64,
        time_ns: u64,
        state: Option<&serde_json::Value>,
        cpu_nanos: &[u64],
        heaps: &CellHeaps,
        shed: u64,
    ) -> Snapshot {
        let cpu_seconds: Vec<f64> = cpu_nanos.iter().map(|nanos| *nanos as f64 / 1e9).collect();
        Snapshot {
            start_ns,
            time_ns,
            gauges: state.map(node_gauges).unwrap_or_default(),
            sums: vec![
                Sum {
                    name: "celld.cell.heap_samples_skipped",
                    description: "Cell-pool isolates a heap sample skipped because a turn held them.",
                    unit: "{isolate}",
                    value: heaps.skipped,
                },
                Sum {
                    name: "celld.metrics.shed",
                    description: "Metrics exports dropped because the exporter was still busy.",
                    unit: "{export}",
                    value: shed,
                },
            ],
            distributions: vec![
                Distribution {
                    name: "celld.cell.cpu_time",
                    description: "Thread CPU per cell that ran JavaScript in the interval.",
                    unit: "s",
                    histogram: ExpHistogram::from_values(&cpu_seconds),
                },
                Distribution {
                    name: "celld.cell.heap_bytes",
                    description: "V8 heap per resident cell: its isolate's physical and external bytes divided by the cells sharing it.",
                    unit: "By",
                    histogram: ExpHistogram::from_values(&heaps.per_cell()),
                },
            ],
        }
    }
}

fn now_unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}

/// Sample and export forever, one snapshot per interval. Returns at once
/// when metrics are off. Spawned once, from startup, after the actor and
/// the runtime exist.
pub async fn run(app: crate::actor::AppHandle) {
    let Some(export) = crate::telemetry::metrics_export() else {
        return;
    };
    let cpu = CELL_CPU.get();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<Vec<u8>>(EXPORT_QUEUE);
    tokio::spawn(async move {
        while let Some(bytes) = rx.recv().await {
            // The shared retry budget and backoff: an outage costs this one
            // snapshot, and the queue in front of it sheds the rest.
            crate::telemetry::post_otlp(&export.client, &export.url, &export.headers, bytes.into())
                .await;
        }
    });

    let mut ticks = tokio::time::interval(export.interval);
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticks.tick().await;
    let mut start_ns = now_unix_ns();
    let mut shed = 0u64;
    loop {
        ticks.tick().await;
        let state = serde_json::from_str::<serde_json::Value>(&app.snapshot().await)
            .ok()
            .filter(|state| state.get("error").is_none());
        let cpu_nanos = cpu.map(CellCpu::take).unwrap_or_default();
        let mut heaps = CellHeaps::default();
        if let Some(runtime) = &app.runtime {
            let current = runtime.generation();
            current.sample_cell_heaps(&mut heaps);
            for generation in runtime.draining_generations() {
                generation.sample_cell_heaps(&mut heaps);
            }
        }
        let time_ns = now_unix_ns();
        let snapshot = Snapshot::new(start_ns, time_ns, state.as_ref(), &cpu_nanos, &heaps, shed);
        start_ns = time_ns;
        let bytes =
            crate::otlp::metrics_request(&snapshot, &export.node, &export.region, &export.service);
        // A shed snapshot took its count of earlier sheds with it, so the
        // count carries until a snapshot that reports it is queued.
        if tx.try_send(bytes).is_ok() {
            shed = 0;
        } else {
            shed += 1;
            tracing::warn!("metrics export shed: the previous export is still retrying");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
