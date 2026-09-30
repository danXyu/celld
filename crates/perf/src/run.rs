// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Running one scenario: start the nodes, run the setup steps and phases,
//! subtract each node's metrics across each phase, run the checks and the
//! verification sweep, and return the scenario's result object.

use crate::check;
use crate::cluster::{Backend, Cluster, Options};
use crate::hist::Hist;
use crate::keys::Keyspace;
use crate::load::Driver;
use crate::scenario::{Scenario, Step};
use crate::sysstat;
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

pub struct RunOptions {
    pub celld: PathBuf,
    pub backend: Backend,
    pub env: BTreeMap<String, String>,
    pub fixtures: PathBuf,
    pub work: PathBuf,
    pub run_id: String,
    /// Multiply every phase duration by this (`--quick`).
    pub duration_scale: f64,
    pub enforce_timing: bool,
}

/// The outcome of one scenario run.
pub struct ScenarioRun {
    pub result: Value,
    /// A count check or the verification sweep failed, or a timing check
    /// failed under `--enforce-timing`.
    pub failed: bool,
}

pub async fn run(scenario: &Scenario, options: &RunOptions) -> anyhow::Result<ScenarioRun> {
    let mut env = scenario.env.clone();
    env.extend(options.env.clone());
    let mut cluster = Cluster::start(Options {
        celld: options.celld.clone(),
        backend: options.backend.clone(),
        work: options.work.clone(),
        fixture: options.fixtures.join(&scenario.fixture),
        nodes: scenario.nodes,
        env: env.clone(),
        node_env: scenario.node_env.clone(),
        run_id: options.run_id.clone(),
    })
    .await?;
    let outcome = drive(scenario, options, &mut cluster, &env).await;
    let states = end_states(&cluster).await;
    cluster.stop().await;
    let mut run = outcome?;
    run.result["end_state"] = states;
    Ok(run)
}

async fn end_states(cluster: &Cluster) -> Value {
    let mut states = Vec::new();
    for node in &cluster.nodes {
        let state = cluster.state(node).await.unwrap_or(Value::Null);
        states.push(json!({
            "node": node.index,
            "owned_cells": state["owned_cells"],
            "residents": state["residents"].as_array().map(Vec::len),
            "rss_bytes": state["rss_bytes"],
            "shedding": state["shedding"],
        }));
    }
    Value::Array(states)
}

async fn drive(
    scenario: &Scenario,
    options: &RunOptions,
    cluster: &mut Cluster,
    env: &BTreeMap<String, String>,
) -> anyhow::Result<ScenarioRun> {
    let driver = Driver::new(cluster.publics())?;
    let mut setup = Vec::new();
    for step in &scenario.setup {
        let started = Instant::now();
        let detail = run_step(step, cluster, &driver).await?;
        cluster.ensure_alive(&format!("setup step {}", step_name(step)))?;
        setup.push(json!({
            "step": step_name(step),
            "seconds": started.elapsed().as_secs_f64(),
            "detail": detail,
        }));
    }
    let mut phases = Vec::new();
    let mut failed = false;
    let mut timing_failed = false;
    // A phase that cannot finish (a node died, a step failed) ends the
    // scenario, and the phases before it are still reported.
    let aborted: Option<String> = 'phases: {
        for phase in &scenario.phases {
            let mut before = Vec::new();
            for step in &phase.before {
                let started = Instant::now();
                let detail = match run_step(step, cluster, &driver).await {
                    Ok(detail) => detail,
                    Err(error) => break 'phases Some(format!("{error:#}")),
                };
                before.push(json!({
                    "step": step_name(step),
                    "seconds": started.elapsed().as_secs_f64(),
                    "detail": detail,
                }));
            }
            for (name, rate) in phase.expand()? {
                let duration =
                    Duration::from_secs_f64((phase.duration_s * options.duration_scale).max(1.0));
                let before = snapshot(cluster).await;
                let sampler = Sampler::start(cluster);
                let phase_started = Instant::now();
                let run = driver.run_phase(
                    &phase.load,
                    rate,
                    duration,
                    phase.arrival,
                    phase.max_inflight,
                    Duration::from_millis(phase.timeout_ms),
                );
                let during = async {
                    let mut done = Vec::new();
                    let mut timed: Vec<_> = phase.during.iter().collect();
                    timed.sort_by(|a, b| a.at_s.total_cmp(&b.at_s));
                    for step in timed {
                        // Offsets shrink with the phase under `--quick`.
                        let at_s = step.at_s * options.duration_scale;
                        tokio::time::sleep_until(
                            (phase_started + Duration::from_secs_f64(at_s.max(0.0))).into(),
                        )
                        .await;
                        let result = run_step(&step.step, cluster, &driver).await;
                        done.push(json!({
                            "at_s": at_s,
                            "step": step_name(&step.step),
                            "took_s": phase_started.elapsed().as_secs_f64() - at_s,
                            "detail": result.as_ref().ok(),
                            "error": result.as_ref().err().map(|error| format!("{error:#}")),
                        }));
                    }
                    done
                };
                let (outcome, during) = tokio::join!(run, during);
                let outcome = match outcome.and_then(|outcome| {
                    cluster
                        .ensure_alive(&format!("phase {name}"))
                        .map(|()| outcome)
                }) {
                    Ok(outcome) => outcome,
                    Err(error) => break 'phases Some(format!("{error:#}")),
                };
                let samples = sampler.finish().await;
                let after = snapshot(cluster).await;
                if phase.warmup {
                    continue;
                }
                let nodes = node_deltas(cluster, &before, &after, &samples);
                let server = merge_nodes(&nodes);
                let mut result = json!({
                    "name": name,
                    "before": before,
                    "during": during,
                    "timeline": outcome.timeline,
                    "offered_rate": rate,
                    "duration_s": outcome.elapsed_s,
                    "scheduled": outcome.scheduled,
                    "ok": outcome.ok,
                    "achieved_rate": outcome.ok as f64 / outcome.elapsed_s.max(0.001),
                    "shed": outcome.shed,
                    "abandoned": outcome.abandoned,
                    "errors": outcome.errors,
                    "error_samples": outcome.error_samples,
                    "schedule_lag_max_us": outcome.schedule_lag_max_us,
                    "ws_received": outcome.ws_received,
                    "latency_us": outcome.latency.to_json(),
                    "service_us": outcome.service.to_json(),
                    "by_label": outcome
                        .by_label
                        .iter()
                        .map(|(label, hist)| (label.clone(), hist.to_json()))
                        .collect::<Map<String, Value>>(),
                    "nodes": nodes,
                    "server": server,
                });
                let checks: Vec<Value> = phase
                    .checks
                    .iter()
                    .map(|check| check::evaluate(check, &result))
                    .collect();
                for check in &checks {
                    if check["pass"] == false {
                        if check["timing"] == true {
                            timing_failed = true;
                        } else {
                            failed = true;
                        }
                    }
                }
                result["checks"] = Value::Array(checks);
                phases.push(result);
            }
        }
        None
    };
    if aborted.is_some() {
        failed = true;
    }
    let mut after = Vec::new();
    for step in scenario.after.iter().filter(|_| aborted.is_none()) {
        let started = Instant::now();
        let detail = run_step(step, cluster, &driver).await?;
        after.push(json!({
            "step": step_name(step),
            "seconds": started.elapsed().as_secs_f64(),
            "detail": detail,
        }));
    }
    let verify = if scenario.verify && aborted.is_none() {
        verify(&driver).await
    } else {
        json!({"skipped": true})
    };
    // A cell the sweep cannot read could be hiding a lost write, so it
    // fails the run as a violation does.
    if verify["violations"]
        .as_array()
        .is_some_and(|violations| !violations.is_empty())
        || verify["unreadable"].as_u64().unwrap_or(0) > 0
    {
        failed = true;
    }
    driver.disconnect();
    if options.enforce_timing && timing_failed {
        failed = true;
    }
    Ok(ScenarioRun {
        result: json!({
            "name": scenario.name,
            "description": scenario.description,
            "fixture": scenario.fixture,
            "nodes": scenario.nodes,
            "backend": cluster.backend().name(),
            "env": env,
            "setup": setup,
            "phases": phases,
            "after": after,
            "verify": verify,
            "failed": failed,
            "error": aborted,
            "timing_failed": timing_failed,
        }),
        failed,
    })
}

fn step_name(step: &Step) -> &'static str {
    match step {
        Step::Touch { .. } => "touch",
        Step::Sleep { .. } => "sleep",
        Step::Connect { .. } => "connect",
        Step::Restart { .. } => "restart",
        Step::EvictAll {} => "evict_all",
        Step::Collect { .. } => "collect",
        Step::Signal { .. } => "signal",
        Step::Start { .. } => "start",
        Step::Redeploy { .. } => "redeploy",
    }
}

async fn run_step(step: &Step, cluster: &mut Cluster, driver: &Driver) -> anyhow::Result<Value> {
    Ok(match step {
        Step::Touch {
            request,
            cells,
            concurrency,
        } => {
            let keys = Keyspace::new(cells);
            let (ok, failed) = driver
                .touch(
                    &request.path,
                    &request.query,
                    &keys,
                    request.counts_write,
                    *concurrency,
                )
                .await;
            json!({"ok": ok, "failed": failed})
        }
        Step::Sleep { ms } => {
            tokio::time::sleep(Duration::from_millis(*ms)).await;
            json!({})
        }
        Step::Connect {
            cells,
            per_cell,
            rate,
        } => {
            let keys = Keyspace::new(cells);
            let failed = driver.connect(&keys, *per_cell, *rate).await;
            json!({"open": driver.socket_count(), "failed": failed})
        }
        Step::Restart { wipe_local } => {
            // Nodes keep their ports, so the driver's targets stay valid;
            // their sockets do not survive.
            driver.disconnect();
            cluster.restart(*wipe_local).await?;
            json!({"wipe_local": wipe_local})
        }
        Step::Signal { node, signal } => {
            cluster.signal(*node, signal).await?;
            json!({"node": node, "signal": signal})
        }
        Step::Start { node, wipe_local } => {
            cluster.start_node(*node, *wipe_local).await?;
            json!({"node": node, "wipe_local": wipe_local})
        }
        Step::Redeploy { reload } => {
            cluster.redeploy(*reload).await?;
            json!({"reload": reload})
        }
        Step::EvictAll {} => json!({"asked": cluster.evict_all().await?}),
        Step::Collect {
            request,
            cells,
            concurrency,
        } => {
            let keys = cells.as_ref().map(Keyspace::new);
            driver
                .collect(&request.path, &request.query, keys.as_ref(), *concurrency)
                .await
        }
    })
}

/// Every node's metrics; `null` for a node that is down or does not answer.
async fn snapshot(cluster: &Cluster) -> Vec<Value> {
    let mut snapshots = Vec::new();
    for (index, node) in cluster.nodes.iter().enumerate() {
        let metrics = if cluster.running(index) {
            cluster.metrics(node).await.unwrap_or(Value::Null)
        } else {
            Value::Null
        };
        snapshots.push(metrics);
    }
    snapshots
}

/// Process samples taken once a second during a phase.
struct Sampler {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<Vec<Vec<sysstat::Sample>>>,
}

impl Sampler {
    fn start(cluster: &Cluster) -> Sampler {
        let pids: Vec<Option<u32>> = cluster.nodes.iter().map(|node| node.pid).collect();
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let samples = Arc::new(Mutex::new(vec![Vec::new(); pids.len()]));
        let task = tokio::spawn({
            let samples = samples.clone();
            async move {
                loop {
                    let taken: Vec<Option<sysstat::Sample>> = tokio::task::spawn_blocking({
                        let pids = pids.clone();
                        move || {
                            pids.iter()
                                .map(|pid| pid.and_then(sysstat::sample))
                                .collect()
                        }
                    })
                    .await
                    .unwrap_or_default();
                    for (node, sample) in taken.into_iter().enumerate() {
                        if let Some(sample) = sample {
                            samples.lock().unwrap()[node].push(sample);
                        }
                    }
                    tokio::select! {
                        _ = &mut stopped => break,
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                    }
                }
                // One last sample at the end of the phase.
                let last: Vec<Option<sysstat::Sample>> = pids
                    .iter()
                    .map(|pid| pid.and_then(sysstat::sample))
                    .collect();
                let mut samples = samples.lock().unwrap().clone();
                for (node, sample) in last.into_iter().enumerate() {
                    if let Some(sample) = sample {
                        samples[node].push(sample);
                    }
                }
                samples
            }
        });
        Sampler { stop, task }
    }

    async fn finish(self) -> Vec<Vec<sysstat::Sample>> {
        let _ = self.stop.send(());
        self.task.await.unwrap_or_default()
    }
}

fn number(value: &Value) -> u64 {
    value.as_u64().unwrap_or(0)
}

fn node_deltas(
    cluster: &Cluster,
    before: &[Value],
    after: &[Value],
    samples: &[Vec<sysstat::Sample>],
) -> Vec<Value> {
    cluster
        .nodes
        .iter()
        .enumerate()
        .map(|(index, node)| {
            // A node that restarted during the phase counts from zero.
            let restarted = after[index]["uptime_us"].as_u64().unwrap_or(0)
                < before[index]["uptime_us"].as_u64().unwrap_or(0);
            let (before, after) = (
                if restarted {
                    &Value::Null
                } else {
                    &before[index]
                },
                &after[index],
            );
            let counters: Map<String, Value> = after["counters"]
                .as_object()
                .map(|counters| {
                    counters
                        .iter()
                        .map(|(label, value)| {
                            let delta =
                                number(value).saturating_sub(number(&before["counters"][label]));
                            (label.clone(), json!(delta))
                        })
                        .collect()
                })
                .unwrap_or_default();
            let histograms: Map<String, Value> = after["histograms"]
                .as_object()
                .map(|histograms| {
                    histograms
                        .iter()
                        .map(|(label, value)| {
                            let delta = Hist::from_node_json(value)
                                .since(&Hist::from_node_json(&before["histograms"][label]));
                            (label.clone(), delta.to_json())
                        })
                        .collect()
                })
                .unwrap_or_default();
            let key = |row: &Value| {
                format!(
                    "{}|{}|{}",
                    row["op"].as_str().unwrap_or(""),
                    row["class"].as_str().unwrap_or(""),
                    row["outcome"].as_str().unwrap_or("")
                )
            };
            let earlier: BTreeMap<String, u64> = before["bucket"]["requests"]
                .as_array()
                .map(|rows| {
                    rows.iter()
                        .map(|row| (key(row), number(&row["count"])))
                        .collect()
                })
                .unwrap_or_default();
            let bucket_requests: Vec<Value> = after["bucket"]["requests"]
                .as_array()
                .map(|rows| {
                    rows.iter()
                        .filter_map(|row| {
                            let delta = number(&row["count"])
                                .saturating_sub(*earlier.get(&key(row)).unwrap_or(&0));
                            (delta > 0).then(|| {
                                json!({
                                    "op": row["op"],
                                    "class": row["class"],
                                    "outcome": row["outcome"],
                                    "count": delta,
                                })
                            })
                        })
                        .collect()
                })
                .unwrap_or_default();
            let node_samples = samples.get(index).cloned().unwrap_or_default();
            let cpu_cores = match (node_samples.first(), node_samples.last()) {
                // Over the samples' own span, which includes the drain after
                // the schedule ends, not over the scheduling window alone.
                (Some(first), Some(last)) if last.at_s > first.at_s => {
                    (last.cpu_seconds - first.cpu_seconds) / (last.at_s - first.at_s)
                }
                _ => 0.0,
            };
            json!({
                "index": node.index,
                "cpu_cores": cpu_cores,
                "rss_bytes_max": node_samples.iter().map(|sample| sample.rss_bytes).max(),
                "rss_bytes_end": node_samples.last().map(|sample| sample.rss_bytes),
                "counters": counters,
                "histograms": histograms,
                "bucket_requests": bucket_requests,
            })
        })
        .collect()
}

/// Every node's deltas, summed.
fn merge_nodes(nodes: &[Value]) -> Value {
    let mut counters: BTreeMap<String, u64> = BTreeMap::new();
    let mut histograms: BTreeMap<String, Hist> = BTreeMap::new();
    let mut bucket: BTreeMap<(String, String, String), u64> = BTreeMap::new();
    for node in nodes {
        if let Some(values) = node["counters"].as_object() {
            for (label, value) in values {
                *counters.entry(label.clone()).or_default() += number(value);
            }
        }
        if let Some(values) = node["histograms"].as_object() {
            for (label, value) in values {
                histograms
                    .entry(label.clone())
                    .or_default()
                    .merge(&Hist::from_json(value));
            }
        }
        if let Some(rows) = node["bucket_requests"].as_array() {
            for row in rows {
                let key = (
                    row["op"].as_str().unwrap_or("").to_string(),
                    row["class"].as_str().unwrap_or("").to_string(),
                    row["outcome"].as_str().unwrap_or("").to_string(),
                );
                *bucket.entry(key).or_default() += number(&row["count"]);
            }
        }
    }
    let total: u64 = bucket.values().sum();
    json!({
        "counters": counters,
        "histograms": histograms
            .iter()
            .filter(|(_, hist)| hist.count() > 0)
            .map(|(label, hist)| (label.clone(), hist.to_json()))
            .collect::<Map<String, Value>>(),
        "bucket_requests_total": total,
        "bucket_requests": bucket
            .iter()
            .map(|((op, class, outcome), count)| json!({
                "op": op, "class": class, "outcome": outcome, "count": count,
            }))
            .collect::<Vec<_>>(),
    })
}

/// Compare each written cell's count with what the generator saw: every
/// acknowledged write must be there, and nothing beyond what was sent.
async fn verify(driver: &Driver) -> Value {
    let tracked = driver.tracker.cells();
    if tracked.is_empty() {
        return json!({"cells": 0, "violations": [], "unreadable": 0});
    }
    let names: Vec<String> = tracked.iter().map(|(cell, _, _)| cell.clone()).collect();
    let counts = driver.read_counts(&names).await;
    let mut violations = Vec::new();
    let mut unreadable = Vec::new();
    for (cell, attempted, acked) in &tracked {
        match counts.get(cell) {
            Some(Ok(n)) if n < acked => violations.push(json!({
                "cell": cell, "count": n, "acknowledged": acked, "attempted": attempted,
                "problem": "an acknowledged write is missing",
            })),
            Some(Ok(n)) if n > attempted => violations.push(json!({
                "cell": cell, "count": n, "acknowledged": acked, "attempted": attempted,
                "problem": "more writes than were sent",
            })),
            Some(Ok(_)) => {}
            Some(Err(error)) => unreadable.push(json!({"cell": cell, "error": error})),
            None => unreadable.push(json!({"cell": cell, "error": "no answer"})),
        }
    }
    json!({
        "cells": tracked.len(),
        "violations": violations,
        "unreadable": unreadable.len(),
        "unreadable_sample": unreadable.into_iter().take(10).collect::<Vec<_>>(),
    })
}
