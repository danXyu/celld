// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Human-readable summaries, and comparing two result files.

use rand::Rng;
use serde_json::Value;
use std::collections::BTreeMap;
use std::fmt::Write as _;

fn ms(value: &Value) -> String {
    match value.as_f64() {
        Some(us) => format!("{:.2}", us / 1000.0),
        None => "-".into(),
    }
}

fn per_ok(value: f64, phase: &Value) -> Option<f64> {
    let ok = phase["ok"].as_f64()?;
    (ok > 0.0).then(|| value / ok)
}

/// One line per phase, then its failed checks.
pub fn summary(result: &Value) -> String {
    let mut out = String::new();
    for run in result["runs"].as_array().into_iter().flatten() {
        let status = if run["failed"] == true {
            "FAILED"
        } else if run["timing_failed"] == true {
            "timing checks failed"
        } else {
            "ok"
        };
        let _ = writeln!(
            out,
            "\n{} (repeat {}, {} node(s), {}) — {status}",
            run["name"].as_str().unwrap_or("?"),
            run["repeat"],
            run["nodes"],
            run["backend"].as_str().unwrap_or("?"),
        );
        if let Some(error) = run["error"].as_str() {
            let _ = writeln!(out, "  stopped early: {error}");
        }
        let _ = writeln!(
            out,
            "  {:<24} {:>9} {:>9} {:>8} {:>7} {:>8} {:>8} {:>8} {:>6} {:>8} {:>7} {:>7}",
            "phase",
            "offered/s",
            "ok/s",
            "errors",
            "shed",
            "p50 ms",
            "p99 ms",
            "p999 ms",
            "cpu",
            "rss MiB",
            "bkt/ok",
            "core/ok"
        );
        for phase in run["phases"].as_array().into_iter().flatten() {
            let errors: u64 = phase["errors"]
                .as_object()
                .map(|errors| errors.values().filter_map(Value::as_u64).sum())
                .unwrap_or(0);
            let nodes = phase["nodes"].as_array().cloned().unwrap_or_default();
            let cpu: f64 = nodes
                .iter()
                .filter_map(|node| node["cpu_cores"].as_f64())
                .sum();
            let rss: f64 = nodes
                .iter()
                .filter_map(|node| node["rss_bytes_max"].as_f64())
                .sum();
            let bucket = phase["server"]["bucket_requests_total"]
                .as_f64()
                .and_then(|total| per_ok(total, phase));
            let core = phase["server"]["counters"]["core.messages"]
                .as_f64()
                .and_then(|total| per_ok(total, phase));
            let _ = writeln!(
                out,
                "  {:<24} {:>9.0} {:>9.0} {:>8} {:>7} {:>8} {:>8} {:>8} {:>6.2} {:>8.0} {:>7} {:>7}",
                phase["name"].as_str().unwrap_or("?"),
                phase["offered_rate"].as_f64().unwrap_or(0.0),
                phase["achieved_rate"].as_f64().unwrap_or(0.0),
                errors,
                phase["shed"],
                ms(&phase["latency_us"]["p50"]),
                ms(&phase["latency_us"]["p99"]),
                ms(&phase["latency_us"]["p999"]),
                cpu,
                rss / (1024.0 * 1024.0),
                bucket.map_or("-".into(), |value| format!("{value:.3}")),
                core.map_or("-".into(), |value| format!("{value:.2}")),
            );
            let histograms = &phase["server"]["histograms"];
            let mut notes = Vec::new();
            for (label, short) in [
                ("gate.wait_us", "gate"),
                ("durability.proof_fleet_us", "fleet proof"),
                ("durability.proof_bucket_us", "bucket proof"),
                ("request.cell_route_us", "cold route"),
                ("loop.core_lag_us", "core lag"),
                ("loop.main_lag_us", "main lag"),
            ] {
                let hist = &histograms[label];
                if hist["count"].as_u64().unwrap_or(0) > 0 {
                    notes.push(format!(
                        "{short} p50/p99 {}/{} ms (n={})",
                        ms(&hist["p50"]),
                        ms(&hist["p99"]),
                        hist["count"]
                    ));
                }
            }
            if !notes.is_empty() {
                let _ = writeln!(out, "      {}", notes.join("; "));
            }
            // Peer links: connections opened (and reset) per pair, which
            // is how many tunnels and log streams the phase needed.
            if let Some(links) = phase["network"]["links"].as_object() {
                let peers: Vec<String> = links
                    .iter()
                    .filter(|(link, stats)| {
                        !link.contains("bucket")
                            && stats["connections"].as_u64().unwrap_or(0)
                                + stats["resets"].as_u64().unwrap_or(0)
                                > 0
                    })
                    .map(|(link, stats)| {
                        format!("{link} {}+{}r", stats["connections"], stats["resets"])
                    })
                    .collect();
                if !peers.is_empty() {
                    let _ = writeln!(out, "      peer connections: {}", peers.join(", "));
                }
            }
            if let Some(errors) = phase["errors"]
                .as_object()
                .filter(|errors| !errors.is_empty())
            {
                let _ = writeln!(out, "      errors: {}", Value::Object(errors.clone()));
                for (kind, sample) in phase["error_samples"].as_object().into_iter().flatten() {
                    let sample: String = sample.as_str().unwrap_or("").chars().take(160).collect();
                    let _ = writeln!(out, "        {kind}: {}", sample.replace('\n', " "));
                }
            }
            for check in phase["checks"].as_array().into_iter().flatten() {
                if check["pass"] != true {
                    let _ = writeln!(
                        out,
                        "      {} check failed: {}{} = {} (min {}, max {})",
                        if check["timing"] == true {
                            "timing"
                        } else {
                            "count"
                        },
                        check["metric"].as_str().unwrap_or("?"),
                        if check["per_ok"] == true {
                            " per ok"
                        } else {
                            ""
                        },
                        check["value"],
                        check["min"],
                        check["max"],
                    );
                }
            }
        }
        let verify = &run["verify"];
        if let Some(cells) = verify["cells"].as_u64().filter(|cells| *cells > 0) {
            let violations = verify["violations"].as_array().map_or(0, Vec::len);
            let _ = writeln!(
                out,
                "  verification: {cells} cells, {violations} violation(s), {} unreadable",
                verify["unreadable"]
            );
            for violation in verify["violations"]
                .as_array()
                .into_iter()
                .flatten()
                .take(10)
            {
                let _ = writeln!(out, "      {violation}");
            }
        }
    }
    out
}

/// What a comparison measures for each phase, and which way is better.
#[derive(Clone, Copy)]
enum Kind {
    /// Wall-clock or throughput: needs repeats to call significant.
    Timing { higher_is_better: bool },
    /// A count per successful request: machine-independent.
    Count,
}

fn metrics(phase: &Value) -> Vec<(&'static str, Kind, Option<f64>)> {
    let server = &phase["server"];
    vec![
        (
            "achieved_rate",
            Kind::Timing {
                higher_is_better: true,
            },
            phase["achieved_rate"].as_f64(),
        ),
        (
            "client p50 us",
            Kind::Timing {
                higher_is_better: false,
            },
            phase["latency_us"]["p50"].as_f64(),
        ),
        (
            "client p99 us",
            Kind::Timing {
                higher_is_better: false,
            },
            phase["latency_us"]["p99"].as_f64(),
        ),
        (
            "bucket requests per ok",
            Kind::Count,
            server["bucket_requests_total"]
                .as_f64()
                .and_then(|total| per_ok(total, phase)),
        ),
        (
            "core messages per ok",
            Kind::Count,
            server["counters"]["core.messages"]
                .as_f64()
                .and_then(|total| per_ok(total, phase)),
        ),
    ]
}

fn mean(values: &[f64]) -> f64 {
    values.iter().sum::<f64>() / values.len().max(1) as f64
}

/// A 95% bootstrap interval for the relative change of the mean from
/// `base` to `new`.
fn bootstrap(base: &[f64], new: &[f64]) -> (f64, f64) {
    let mut rng = rand::thread_rng();
    let mut changes: Vec<f64> = (0..2000)
        .map(|_| {
            let resample = |values: &[f64], rng: &mut rand::rngs::ThreadRng| {
                mean(
                    &(0..values.len())
                        .map(|_| values[rng.gen_range(0..values.len())])
                        .collect::<Vec<_>>(),
                )
            };
            let base = resample(base, &mut rng);
            let new = resample(new, &mut rng);
            if base == 0.0 {
                0.0
            } else {
                (new - base) / base
            }
        })
        .collect();
    changes.sort_by(f64::total_cmp);
    (changes[50], changes[1949])
}

/// Compare two result files. A count regression, or a timing regression
/// whose 95% interval clears `threshold` in the bad direction, fails the
/// comparison; a single-run timing change past the threshold is a warning.
pub fn compare(base: &Value, new: &Value, threshold: f64) -> (String, bool) {
    let collect = |result: &Value| {
        let mut phases: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();
        for run in result["runs"].as_array().into_iter().flatten() {
            let scenario = run["name"].as_str().unwrap_or("?").to_string();
            for phase in run["phases"].as_array().into_iter().flatten() {
                let name = phase["name"].as_str().unwrap_or("?").to_string();
                phases
                    .entry((scenario.clone(), name))
                    .or_default()
                    .push(phase.clone());
            }
        }
        phases
    };
    let base = collect(base);
    let new = collect(new);
    let mut out = String::new();
    let mut regressed = false;
    let _ = writeln!(
        out,
        "{:<40} {:<24} {:>12} {:>12} {:>9}  verdict",
        "scenario / phase", "metric", "base", "new", "change"
    );
    for (key, new_phases) in &new {
        let Some(base_phases) = base.get(key) else {
            let _ = writeln!(
                out,
                "{:<40} (new; no baseline)",
                format!("{} / {}", key.0, key.1)
            );
            continue;
        };
        for (index, (name, kind, _)) in metrics(&new_phases[0]).into_iter().enumerate() {
            let values = |phases: &[Value]| -> Vec<f64> {
                phases
                    .iter()
                    .filter_map(|phase| metrics(phase)[index].2)
                    .collect()
            };
            let (base_values, new_values) = (values(base_phases), values(new_phases));
            if base_values.is_empty() || new_values.is_empty() {
                continue;
            }
            let (base_mean, new_mean) = (mean(&base_values), mean(&new_values));
            let change = if base_mean == 0.0 {
                if new_mean == 0.0 {
                    0.0
                } else {
                    f64::INFINITY
                }
            } else {
                (new_mean - base_mean) / base_mean
            };
            let verdict = match kind {
                Kind::Count => {
                    // Background requests (leases, capacity samples) add a
                    // little noise to a per-request count; a real change to
                    // a hot path moves it by far more.
                    if new_mean - base_mean > 0.05 && change > threshold {
                        regressed = true;
                        "REGRESSION (count)".to_string()
                    } else if base_mean - new_mean > 0.05 && -change > threshold {
                        "improved (count)".to_string()
                    } else {
                        String::new()
                    }
                }
                Kind::Timing { higher_is_better } => {
                    let worse = |change: f64| {
                        if higher_is_better {
                            change < -threshold
                        } else {
                            change > threshold
                        }
                    };
                    let better = |change: f64| {
                        if higher_is_better {
                            change > threshold
                        } else {
                            change < -threshold
                        }
                    };
                    if base_values.len() >= 3 && new_values.len() >= 3 {
                        let (low, high) = bootstrap(&base_values, &new_values);
                        if worse(low) && worse(high) {
                            regressed = true;
                            format!(
                                "REGRESSION (95% CI {:+.1}%..{:+.1}%)",
                                low * 100.0,
                                high * 100.0
                            )
                        } else if better(low) && better(high) {
                            format!(
                                "improved (95% CI {:+.1}%..{:+.1}%)",
                                low * 100.0,
                                high * 100.0
                            )
                        } else {
                            String::new()
                        }
                    } else if worse(change) {
                        "possible regression (too few repeats to call)".to_string()
                    } else {
                        String::new()
                    }
                }
            };
            let _ = writeln!(
                out,
                "{:<40} {:<24} {:>12.3} {:>12.3} {:>+8.1}%  {verdict}",
                format!("{} / {}", key.0, key.1),
                name,
                base_mean,
                new_mean,
                change * 100.0
            );
        }
    }
    (out, regressed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn result(rates: &[f64], bucket_per_ok: f64) -> Value {
        json!({"runs": rates.iter().map(|rate| json!({
            "name": "S", "phases": [{
                "name": "p", "ok": 1000, "achieved_rate": rate,
                "latency_us": {"p50": 1000, "p99": 5000},
                "server": {"bucket_requests_total": bucket_per_ok * 1000.0,
                           "counters": {"core.messages": 3000}},
            }]
        })).collect::<Vec<_>>()})
    }

    #[test]
    fn a_count_regression_fails_and_noise_does_not() {
        let (_, regressed) = compare(&result(&[1000.0], 0.0), &result(&[1000.0], 1.0), 0.05);
        assert!(regressed);
        let (_, regressed) = compare(&result(&[1000.0], 0.01), &result(&[1000.0], 0.02), 0.05);
        assert!(!regressed);
    }

    #[test]
    fn a_throughput_drop_needs_repeats_to_fail() {
        let (text, regressed) = compare(&result(&[1000.0], 0.0), &result(&[500.0], 0.0), 0.05);
        assert!(!regressed, "{text}");
        assert!(text.contains("possible regression"), "{text}");
        let (_, regressed) = compare(
            &result(&[1000.0, 1010.0, 990.0], 0.0),
            &result(&[500.0, 505.0, 495.0], 0.0),
            0.05,
        );
        assert!(regressed);
    }
}
