// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! The open-loop load generator.
//!
//! A phase schedules requests at a fixed offered rate, evenly spaced or with
//! exponential gaps, and measures each one from the moment it was scheduled
//! to start, not from the moment it was sent. A server that falls behind
//! therefore shows its queueing in the latency, instead of slowing the
//! generator down and hiding it (coordinated omission). When more requests
//! are in flight than the phase allows, the generator sheds the rest and
//! counts them, so a saturated run says so.
//!
//! WebSocket messages carry their schedule time, so a reply, or a broadcast
//! delivery on another socket, can be timed without shared state. A `ping`
//! is answered by the node without its cell, as the literal `pong`; each
//! socket times those in order.

use crate::hist::{Hist, Recorder};
use crate::keys::Keyspace;
use crate::scenario::{Arrival, Load, Target, WsMessage};
use anyhow::Context as _;
use bytes::Bytes;
use fastwebsockets::{Frame, OpCode, Payload};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

static EPOCH: OnceLock<Instant> = OnceLock::new();

/// Microseconds since the harness started; the clock messages carry.
pub fn now_us() -> u64 {
    EPOCH.get_or_init(Instant::now).elapsed().as_micros() as u64
}

fn instant_us(at: Instant) -> u64 {
    at.saturating_duration_since(*EPOCH.get_or_init(Instant::now))
        .as_micros() as u64
}

/// Acknowledged and attempted writes per cell, for the verification sweep.
#[derive(Default)]
pub struct Tracker {
    cells: Mutex<HashMap<String, (u64, u64)>>,
}

impl Tracker {
    pub fn attempt(&self, cell: &str) {
        self.cells
            .lock()
            .unwrap()
            .entry(cell.to_string())
            .or_default()
            .0 += 1;
    }

    pub fn ack(&self, cell: &str) {
        self.cells
            .lock()
            .unwrap()
            .entry(cell.to_string())
            .or_default()
            .1 += 1;
    }

    /// Every tracked cell: (attempted, acknowledged).
    pub fn cells(&self) -> Vec<(String, u64, u64)> {
        let mut cells: Vec<_> = self
            .cells
            .lock()
            .unwrap()
            .iter()
            .map(|(cell, (attempted, acked))| (cell.clone(), *attempted, *acked))
            .collect();
        cells.sort();
        cells
    }
}

/// Where replies for the current phase are recorded.
struct PhaseSinks {
    id: u64,
    labels: Vec<Arc<Recorder>>,
    all: Arc<Recorder>,
    received: AtomicU64,
}

#[derive(Default)]
struct WsState {
    phase: RwLock<Option<Arc<PhaseSinks>>>,
}

impl WsState {
    fn record(&self, phase: u64, label: usize, sent_us: u64) {
        let current = self.phase.read().unwrap();
        if let Some(sinks) = current.as_ref().filter(|sinks| sinks.id == phase) {
            let latency = now_us().saturating_sub(sent_us);
            if let Some(recorder) = sinks.labels.get(label) {
                recorder.record(latency);
            }
            sinks.all.record(latency);
            sinks.received.fetch_add(1, Ordering::Relaxed);
        }
    }
}

struct Socket {
    cell: String,
    tx: mpsc::UnboundedSender<String>,
    /// Pings in flight on this socket: (phase, label, sent).
    pings: Arc<Mutex<VecDeque<(u64, usize, u64)>>>,
}

pub struct Driver {
    http: reqwest::Client,
    publics: Vec<String>,
    pub tracker: Arc<Tracker>,
    sockets: RwLock<Vec<Arc<Socket>>>,
    ws: Arc<WsState>,
    next_phase: AtomicU64,
}

/// One load part, ready to fire.
struct Prepared {
    label: String,
    cumulative: f64,
    kind: PreparedKind,
}

enum PreparedKind {
    Http {
        path: String,
        query: String,
        keys: Option<Keyspace>,
        counts_write: bool,
        node: Option<usize>,
    },
    Ws(WsMessage),
}

#[derive(Debug, Default)]
pub struct PhaseOutcome {
    pub scheduled: u64,
    pub ok: u64,
    pub shed: u64,
    pub errors: BTreeMap<String, u64>,
    /// One answer or error message for each kind in `errors`.
    pub error_samples: BTreeMap<String, String>,
    /// From each request's scheduled start: what a client waits.
    pub latency: Hist,
    /// From the moment each request was sent: what the server took. It
    /// excludes the generator's own lateness, so the two differ only when
    /// the generator could not keep its schedule.
    pub service: Hist,
    pub by_label: BTreeMap<String, Hist>,
    /// The latest the generator ran behind its own schedule.
    pub schedule_lag_max_us: u64,
    /// Seconds the phase spent scheduling.
    pub elapsed_s: f64,
    /// WebSocket replies or deliveries received.
    pub ws_received: u64,
    /// Requests still in flight when the drain deadline passed.
    pub abandoned: u64,
    /// Per second of the phase, by schedule time: successes, errors, and
    /// the slowest success. A takeover or a cutover shows here.
    pub timeline: Vec<serde_json::Value>,
}

/// Per-second tallies for [`PhaseOutcome::timeline`].
struct Timeline {
    start: Instant,
    seconds: Vec<[AtomicU64; 3]>,
}

impl Timeline {
    fn new(start: Instant, duration: Duration) -> Timeline {
        let slots = duration.as_secs() as usize + 2;
        Timeline {
            start,
            seconds: (0..slots)
                .map(|_| [AtomicU64::new(0), AtomicU64::new(0), AtomicU64::new(0)])
                .collect(),
        }
    }

    fn slot(&self, intended: Instant) -> Option<&[AtomicU64; 3]> {
        let second = intended.saturating_duration_since(self.start).as_secs() as usize;
        self.seconds.get(second)
    }

    fn ok(&self, intended: Instant, latency_us: u64) {
        if let Some(slot) = self.slot(intended) {
            slot[0].fetch_add(1, Ordering::Relaxed);
            slot[2].fetch_max(latency_us, Ordering::Relaxed);
        }
    }

    fn error(&self, intended: Instant) {
        if let Some(slot) = self.slot(intended) {
            slot[1].fetch_add(1, Ordering::Relaxed);
        }
    }

    fn to_json(&self) -> Vec<serde_json::Value> {
        self.seconds
            .iter()
            .enumerate()
            .map(|(second, slot)| {
                serde_json::json!({
                    "s": second,
                    "ok": slot[0].load(Ordering::Relaxed),
                    "errors": slot[1].load(Ordering::Relaxed),
                    "max_us": slot[2].load(Ordering::Relaxed),
                })
            })
            .collect()
    }
}

struct Counters {
    ok: AtomicU64,
    inflight: AtomicUsize,
    errors: Mutex<BTreeMap<String, u64>>,
    /// The first answer seen for each kind of error, to say what it was.
    samples: Mutex<BTreeMap<String, String>>,
}

impl Counters {
    fn error(&self, kind: String) {
        *self.errors.lock().unwrap().entry(kind).or_default() += 1;
    }

    fn sample(&self, kind: &str, detail: impl FnOnce() -> String) {
        let mut samples = self.samples.lock().unwrap();
        if !samples.contains_key(kind) {
            let mut detail = detail();
            detail.truncate(500);
            samples.insert(kind.to_string(), detail);
        }
    }
}

fn encode_query(query: &BTreeMap<String, String>) -> String {
    let encode = |text: &str| -> String {
        text.bytes()
            .map(|byte| match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    (byte as char).to_string()
                }
                _ => format!("%{byte:02X}"),
            })
            .collect()
    };
    query
        .iter()
        .map(|(name, value)| format!("{}={}", encode(name), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

struct SpawnExecutor;

impl<F> hyper::rt::Executor<F> for SpawnExecutor
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    fn execute(&self, future: F) {
        tokio::spawn(future);
    }
}

impl Driver {
    pub fn new(publics: Vec<String>) -> anyhow::Result<Driver> {
        let http = reqwest::Client::builder()
            .tcp_nodelay(true)
            .pool_idle_timeout(Duration::from_secs(30))
            .build()?;
        Ok(Driver {
            http,
            publics,
            tracker: Arc::default(),
            sockets: RwLock::default(),
            ws: Arc::default(),
            next_phase: AtomicU64::new(1),
        })
    }

    pub fn socket_count(&self) -> usize {
        self.sockets.read().unwrap().len()
    }

    /// Send one request to every cell, `concurrency` at a time. Returns
    /// (ok, failed).
    pub async fn touch(
        &self,
        path: &str,
        query: &BTreeMap<String, String>,
        keys: &Keyspace,
        counts_write: bool,
        concurrency: usize,
    ) -> (u64, u64) {
        let query = encode_query(query);
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
        let ok = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();
        for (index, cell) in keys.names().enumerate() {
            let permit = semaphore.clone().acquire_owned().await.expect("open");
            let node = &self.publics[index % self.publics.len()];
            let url = url(node, path, &query, Some(&cell));
            let http = self.http.clone();
            let tracker = self.tracker.clone();
            let (ok, failed) = (ok.clone(), failed.clone());
            tasks.push(tokio::spawn(async move {
                if counts_write {
                    tracker.attempt(&cell);
                }
                let result = http.get(url).timeout(Duration::from_secs(60)).send().await;
                match result {
                    Ok(response) if response.status().is_success() => {
                        let _ = response.bytes().await;
                        if counts_write {
                            tracker.ack(&cell);
                        }
                        ok.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        failed.fetch_add(1, Ordering::Relaxed);
                    }
                }
                drop(permit);
            }));
        }
        for task in tasks {
            let _ = task.await;
        }
        (ok.load(Ordering::Relaxed), failed.load(Ordering::Relaxed))
    }

    /// Open `per_cell` sockets on each cell, `rate` per second. Returns
    /// how many failed.
    pub async fn connect(&self, keys: &Keyspace, per_cell: usize, rate: f64) -> u64 {
        let gap = Duration::from_secs_f64(1.0 / rate.max(0.001));
        let failed = Arc::new(AtomicU64::new(0));
        let mut tasks = Vec::new();
        let mut index = 0usize;
        let mut next = Instant::now();
        for cell in keys.names() {
            for _ in 0..per_cell {
                tokio::time::sleep_until(next.into()).await;
                next += gap;
                let public = self.publics[index % self.publics.len()].clone();
                index += 1;
                let cell = cell.clone();
                let ws = self.ws.clone();
                let tracker = self.tracker.clone();
                let failed = failed.clone();
                tasks.push(tokio::spawn(async move {
                    match open_socket(&public, &cell, ws, tracker).await {
                        Ok(socket) => Some(socket),
                        Err(_) => {
                            failed.fetch_add(1, Ordering::Relaxed);
                            None
                        }
                    }
                }));
            }
        }
        for task in tasks {
            if let Ok(Some(socket)) = task.await {
                self.sockets.write().unwrap().push(Arc::new(socket));
            }
        }
        failed.load(Ordering::Relaxed)
    }

    /// Run one phase at `rate` for `duration`.
    #[allow(clippy::too_many_arguments)]
    pub async fn run_phase(
        &self,
        loads: &[Load],
        rate: f64,
        duration: Duration,
        arrival: Arrival,
        max_inflight: usize,
        timeout: Duration,
    ) -> anyhow::Result<PhaseOutcome> {
        let total_weight: f64 = loads.iter().map(|load| load.weight).sum();
        let mut cumulative = 0.0;
        let mut prepared = Vec::new();
        for load in loads {
            cumulative += load.weight / total_weight;
            let kind = match &load.target {
                Target::Http {
                    request,
                    cells,
                    node,
                } => PreparedKind::Http {
                    path: request.path.clone(),
                    query: encode_query(&request.query),
                    keys: cells.as_ref().map(Keyspace::new),
                    counts_write: request.counts_write,
                    node: *node,
                },
                Target::WebSocket { message } => {
                    anyhow::ensure!(
                        self.socket_count() > 0,
                        "a WebSocket load needs a connect step first"
                    );
                    PreparedKind::Ws(*message)
                }
            };
            prepared.push(Prepared {
                label: load.label(),
                cumulative,
                kind,
            });
        }
        let prepared = Arc::new(prepared);
        let labels: Vec<Arc<Recorder>> = prepared.iter().map(|_| Arc::default()).collect();
        let all: Arc<Recorder> = Arc::default();
        let service: Arc<Recorder> = Arc::default();
        let phase_id = self.next_phase.fetch_add(1, Ordering::Relaxed);
        let sinks = Arc::new(PhaseSinks {
            id: phase_id,
            labels: labels.clone(),
            all: all.clone(),
            received: AtomicU64::new(0),
        });
        *self.ws.phase.write().unwrap() = Some(sinks.clone());
        let counters = Arc::new(Counters {
            ok: AtomicU64::new(0),
            inflight: AtomicUsize::new(0),
            errors: Mutex::default(),
            samples: Mutex::default(),
        });
        let sockets: Vec<Arc<Socket>> = self.sockets.read().unwrap().clone();
        let mut rng = StdRng::from_entropy();
        let gap = 1.0 / rate.max(0.001);
        let start = Instant::now() + Duration::from_millis(5);
        let end = start + duration;
        let timeline = Arc::new(Timeline::new(start, duration));
        let mut next = start;
        let mut sequence = 0u64;
        let mut scheduled = 0u64;
        let mut shed = 0u64;
        let mut lag_max = 0u64;
        let mut tasks = tokio::task::JoinSet::new();
        while next < end {
            let now = Instant::now();
            if next > now {
                // A timer can fire a millisecond or two late, which the
                // latency would then count against the server: sleep to
                // just short of the deadline, then yield until it.
                let wait = next - now;
                if wait > Duration::from_millis(2) {
                    tokio::time::sleep_until((next - Duration::from_millis(1)).into()).await;
                } else {
                    tokio::task::yield_now().await;
                }
                continue;
            }
            lag_max = lag_max.max(now.duration_since(next).as_micros() as u64);
            while next <= Instant::now() && next < end {
                let intended = next;
                sequence += 1;
                next = match arrival {
                    Arrival::Uniform => start + Duration::from_secs_f64(sequence as f64 * gap),
                    Arrival::Poisson => {
                        let uniform: f64 = rng.gen_range(f64::EPSILON..1.0);
                        next + Duration::from_secs_f64(-uniform.ln() * gap)
                    }
                };
                scheduled += 1;
                if counters.inflight.load(Ordering::Relaxed) >= max_inflight {
                    shed += 1;
                    continue;
                }
                let pick: f64 = rng.gen();
                let index = prepared
                    .iter()
                    .position(|part| pick <= part.cumulative)
                    .unwrap_or(prepared.len() - 1);
                match &prepared[index].kind {
                    PreparedKind::Http {
                        path,
                        query,
                        keys,
                        counts_write,
                        node,
                    } => {
                        let cell = keys.as_ref().map(|keys| keys.name(keys.sample(&mut rng)));
                        let public = match node {
                            Some(node) => &self.publics[*node % self.publics.len()],
                            None => &self.publics[rng.gen_range(0..self.publics.len())],
                        };
                        let url = url(public, path, query, cell.as_deref());
                        let http = self.http.clone();
                        let counters = counters.clone();
                        let tracker = self.tracker.clone();
                        let recorder = labels[index].clone();
                        let all = all.clone();
                        let service = service.clone();
                        let counts_write = *counts_write;
                        let timeline = timeline.clone();
                        counters.inflight.fetch_add(1, Ordering::Relaxed);
                        tasks.spawn(async move {
                            if counts_write {
                                if let Some(cell) = &cell {
                                    tracker.attempt(cell);
                                }
                            }
                            let sent = Instant::now();
                            let result = http.get(url).timeout(timeout).send().await;
                            let outcome = match result {
                                Ok(response) => {
                                    let status = response.status();
                                    match response.bytes().await {
                                        Ok(_) if status.is_success() => Ok(()),
                                        Ok(body) => {
                                            let kind = format!("http_{}", status.as_u16());
                                            counters.sample(&kind, || {
                                                String::from_utf8_lossy(&body).into_owned()
                                            });
                                            Err(kind)
                                        }
                                        Err(_) => Err("body".to_string()),
                                    }
                                }
                                Err(error) => {
                                    let kind = if error.is_timeout() {
                                        "timeout"
                                    } else if error.is_connect() {
                                        "connect"
                                    } else {
                                        "request"
                                    };
                                    counters.sample(kind, || format!("{error:#}"));
                                    Err(kind.to_string())
                                }
                            };
                            let latency = intended.elapsed().as_micros() as u64;
                            match outcome {
                                Ok(()) => {
                                    recorder.record(latency);
                                    all.record(latency);
                                    service.record(sent.elapsed().as_micros() as u64);
                                    timeline.ok(intended, latency);
                                    counters.ok.fetch_add(1, Ordering::Relaxed);
                                    if counts_write {
                                        if let Some(cell) = &cell {
                                            tracker.ack(cell);
                                        }
                                    }
                                }
                                Err(kind) => {
                                    timeline.error(intended);
                                    counters.error(kind);
                                }
                            }
                            counters.inflight.fetch_sub(1, Ordering::Relaxed);
                        });
                    }
                    PreparedKind::Ws(message) => {
                        let socket = &sockets[rng.gen_range(0..sockets.len())];
                        let sent = instant_us(intended);
                        let text = match message {
                            WsMessage::Echo => format!("e:{phase_id}:{index}:{sent}"),
                            WsMessage::Write => {
                                self.tracker.attempt(&socket.cell);
                                format!("w:{phase_id}:{index}:{sent}")
                            }
                            WsMessage::Broadcast => format!("b:{phase_id}:{index}:{sent}"),
                            WsMessage::Ping => {
                                socket
                                    .pings
                                    .lock()
                                    .unwrap()
                                    .push_back((phase_id, index, sent));
                                "ping".to_string()
                            }
                        };
                        if socket.tx.send(text).is_err() {
                            timeline.error(intended);
                            counters.error("ws_closed".into());
                        }
                    }
                }
            }
            // Reap finished requests so the set stays small.
            while tasks.try_join_next().is_some() {}
        }
        let elapsed_s = start.elapsed().as_secs_f64().min(duration.as_secs_f64());
        // Drain: every request in flight gets its own timeout to finish.
        let drain_deadline = Instant::now() + timeout + Duration::from_secs(1);
        let mut abandoned = 0;
        while !tasks.is_empty() {
            let remaining = drain_deadline.saturating_duration_since(Instant::now());
            match tokio::time::timeout(remaining, tasks.join_next()).await {
                Ok(Some(_)) => {}
                Ok(None) => break,
                Err(_) => {
                    abandoned = tasks.len() as u64;
                    tasks.abort_all();
                    break;
                }
            }
        }
        // WebSocket replies: give them a moment to arrive.
        if prepared
            .iter()
            .any(|part| matches!(part.kind, PreparedKind::Ws(_)))
        {
            tokio::time::sleep(Duration::from_millis(500).min(timeout)).await;
        }
        *self.ws.phase.write().unwrap() = None;
        let ok = counters.ok.load(Ordering::Relaxed) + sinks.received.load(Ordering::Relaxed);
        let by_label = prepared
            .iter()
            .zip(&labels)
            .map(|(part, recorder)| (part.label.clone(), recorder.snapshot()))
            .fold(
                BTreeMap::new(),
                |mut labels: BTreeMap<String, Hist>, (label, hist)| {
                    labels.entry(label).or_default().merge(&hist);
                    labels
                },
            );
        let errors = counters.errors.lock().unwrap().clone();
        let error_samples = counters.samples.lock().unwrap().clone();
        Ok(PhaseOutcome {
            scheduled,
            ok,
            shed,
            errors,
            error_samples,
            latency: all.snapshot(),
            service: service.snapshot(),
            by_label,
            schedule_lag_max_us: lag_max,
            elapsed_s,
            ws_received: sinks.received.load(Ordering::Relaxed),
            abandoned,
            timeline: timeline.to_json(),
        })
    }

    /// Send one request (per cell, if `keys`) and fold the JSON answers:
    /// for each numeric field, its sum and its max.
    pub async fn collect(
        &self,
        path: &str,
        query: &BTreeMap<String, String>,
        keys: Option<&Keyspace>,
        concurrency: usize,
    ) -> serde_json::Value {
        let query = encode_query(query);
        let targets: Vec<Option<String>> = match keys {
            Some(keys) => keys.names().map(Some).collect(),
            None => vec![None],
        };
        let semaphore = Arc::new(tokio::sync::Semaphore::new(concurrency.max(1)));
        let mut tasks = Vec::new();
        for (index, cell) in targets.into_iter().enumerate() {
            let permit = semaphore.clone().acquire_owned().await.expect("open");
            let url = url(
                &self.publics[index % self.publics.len()],
                path,
                &query,
                cell.as_deref(),
            );
            let http = self.http.clone();
            tasks.push(tokio::spawn(async move {
                let answer = async {
                    let response = http
                        .get(url)
                        .timeout(Duration::from_secs(60))
                        .send()
                        .await
                        .ok()?;
                    if !response.status().is_success() {
                        return None;
                    }
                    serde_json::from_str::<serde_json::Value>(&response.text().await.ok()?).ok()
                }
                .await;
                drop(permit);
                answer
            }));
        }
        let mut sums: BTreeMap<String, f64> = BTreeMap::new();
        let mut maxes: BTreeMap<String, f64> = BTreeMap::new();
        let (mut answered, mut failed) = (0u64, 0u64);
        for task in tasks {
            match task.await.ok().flatten() {
                Some(serde_json::Value::Object(fields)) => {
                    answered += 1;
                    for (name, value) in fields {
                        if let Some(value) = value.as_f64() {
                            *sums.entry(name.clone()).or_default() += value;
                            let max = maxes.entry(name).or_insert(f64::MIN);
                            *max = max.max(value);
                        }
                    }
                }
                _ => failed += 1,
            }
        }
        serde_json::json!({"answered": answered, "failed": failed, "sum": sums, "max": maxes})
    }

    /// Ask each tracked cell for its write count.
    pub async fn read_counts(&self, cells: &[String]) -> BTreeMap<String, Result<u64, String>> {
        // Gently: every read may activate its cell, and a node at its
        // residency cap answers a burst of activations with refusals that
        // say nothing about the cell's data. A refused read is retried.
        let semaphore = Arc::new(tokio::sync::Semaphore::new(16));
        let mut tasks = Vec::new();
        for (index, cell) in cells.iter().enumerate() {
            let permit = semaphore.clone().acquire_owned().await.expect("open");
            let url = url(
                &self.publics[index % self.publics.len()],
                "/do/state",
                "",
                Some(cell),
            );
            let http = self.http.clone();
            let cell = cell.clone();
            tasks.push(tokio::spawn(async move {
                let mut result = Err("not tried".to_string());
                for attempt in 0..4 {
                    if attempt > 0 {
                        tokio::time::sleep(Duration::from_millis(500 << attempt)).await;
                    }
                    result = read_count(&http, &url).await;
                    if result.is_ok() {
                        break;
                    }
                }
                drop(permit);
                (cell, result)
            }));
        }
        let mut counts = BTreeMap::new();
        for task in tasks {
            if let Ok((cell, result)) = task.await {
                counts.insert(cell, result);
            }
        }
        counts
    }

    /// Close every socket.
    pub fn disconnect(&self) {
        self.sockets.write().unwrap().clear();
    }
}

/// One cell's write count, from `/do/state`.
async fn read_count(http: &reqwest::Client, url: &str) -> Result<u64, String> {
    let response = http
        .get(url)
        .timeout(Duration::from_secs(60))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    let status = response.status();
    let text = response.text().await.map_err(|error| error.to_string())?;
    if !status.is_success() {
        return Err(format!("{status}: {text}"));
    }
    let body: serde_json::Value =
        serde_json::from_str(&text).map_err(|error| format!("{error}: {text}"))?;
    body["n"]
        .as_u64()
        .ok_or_else(|| format!("no count in {body}"))
}

fn url(public: &str, path: &str, query: &str, cell: Option<&str>) -> String {
    let mut url = format!("{public}{path}");
    let mut separator = '?';
    if !query.is_empty() {
        url.push(separator);
        url.push_str(query);
        separator = '&';
    }
    if let Some(cell) = cell {
        url.push(separator);
        url.push_str("cell=");
        url.push_str(cell);
    }
    url
}

async fn open_socket(
    public: &str,
    cell: &str,
    ws: Arc<WsState>,
    tracker: Arc<Tracker>,
) -> anyhow::Result<Socket> {
    let host = public.trim_start_matches("http://").to_string();
    let stream = tokio::net::TcpStream::connect(&host)
        .await
        .with_context(|| format!("connect {host}"))?;
    stream.set_nodelay(true)?;
    let request = hyper::Request::builder()
        .method("GET")
        .uri(format!("http://{host}/ws?cell={cell}"))
        .header(hyper::header::HOST, &host)
        .header(hyper::header::UPGRADE, "websocket")
        .header(hyper::header::CONNECTION, "upgrade")
        .header(
            "Sec-WebSocket-Key",
            fastwebsockets::handshake::generate_key(),
        )
        .header("Sec-WebSocket-Version", "13")
        .body(http_body_util::Empty::<Bytes>::new())?;
    let (socket, _) = fastwebsockets::handshake::client(&SpawnExecutor, request, stream)
        .await
        .context("WebSocket handshake")?;
    let (mut read, write) = socket.split(tokio::io::split);
    let write = Arc::new(tokio::sync::Mutex::new(write));
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let pings: Arc<Mutex<VecDeque<(u64, usize, u64)>>> = Arc::default();
    {
        let write = write.clone();
        tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                let frame = Frame::text(Payload::Owned(text.into_bytes()));
                if write.lock().await.write_frame(frame).await.is_err() {
                    break;
                }
            }
            let _ = write
                .lock()
                .await
                .write_frame(Frame::close(1000, b""))
                .await;
        });
    }
    {
        let pings = pings.clone();
        let cell = cell.to_string();
        tokio::spawn(async move {
            loop {
                let frame = read
                    .read_frame(&mut |frame| {
                        let write = write.clone();
                        async move { write.lock().await.write_frame(frame).await }
                    })
                    .await;
                let Ok(frame) = frame else {
                    break;
                };
                match frame.opcode {
                    OpCode::Close => break,
                    OpCode::Text | OpCode::Binary => {
                        let text = String::from_utf8_lossy(&frame.payload);
                        if text == "pong" {
                            let ping = pings.lock().unwrap().pop_front();
                            if let Some((phase, label, sent)) = ping {
                                ws.record(phase, label, sent);
                            }
                            continue;
                        }
                        let mut parts = text.splitn(4, ':');
                        let kind = parts.next();
                        let fields: Option<(u64, usize, u64)> = (|| {
                            Some((
                                parts.next()?.parse().ok()?,
                                parts.next()?.parse().ok()?,
                                parts.next()?.parse().ok()?,
                            ))
                        })();
                        if let Some((phase, label, sent)) = fields {
                            if kind == Some("w") {
                                tracker.ack(&cell);
                            }
                            ws.record(phase, label, sent);
                        }
                    }
                    _ => {}
                }
            }
        });
    }
    Ok(Socket {
        cell: cell.to_string(),
        tx,
        pings,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_join_their_parts() {
        assert_eq!(url("http://h", "/noop", "", None), "http://h/noop");
        assert_eq!(
            url("http://h", "/do/write", "bytes=100", Some("c-1")),
            "http://h/do/write?bytes=100&cell=c-1"
        );
        let mut query = BTreeMap::new();
        query.insert("a b".to_string(), "x&y".to_string());
        assert_eq!(encode_query(&query), "a%20b=x%26y");
    }
}
