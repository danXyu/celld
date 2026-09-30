// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! Checks on one phase's result.
//!
//! A check's `metric` is one of:
//!
//! - `bucket[:FILTER,...]`: object-store requests the nodes made, summed
//!   over matching rows. Filters are `class=A+B`, `op=A+B`, `outcome=A+B`,
//!   with the labels of `crates/celld/perf_stats.rs`;
//! - `counter:LABEL`: a node counter, e.g. `counter:core.requests`;
//! - `hist_count:LABEL`, `hist_p50:LABEL`, `hist_p99:LABEL`: a node
//!   histogram's count or percentile over the phase;
//! - `client:FIELD`: `ok`, `errors`, `shed`, `error_rate`, `achieved_rate`,
//!   `p50_us`, `p99_us`, `p999_us`;
//! - `node:FIELD`: `cpu_cores` or `rss_bytes_max`, the largest across nodes.
//!
//! With `per_ok`, the value is divided by the phase's successful requests,
//! which makes a count check independent of the phase's length and rate.

use crate::scenario::Check;
use serde_json::{json, Value};

/// Evaluate `check` against a phase result (the JSON [`crate::run`]
/// writes). Returns the check's result object.
pub fn evaluate(check: &Check, phase: &Value) -> Value {
    let value = metric(&check.metric, phase);
    let divisor = match (&check.per, check.per_ok) {
        (Some(per), _) => metric(per, phase),
        (None, true) => phase["ok"].as_f64(),
        (None, false) => Some(1.0),
    };
    let divisor = if check.per_second {
        divisor
            .zip(phase["duration_s"].as_f64())
            .map(|(a, b)| a * b)
    } else {
        divisor
    };
    let value = match (value, divisor) {
        (Some(value), Some(divisor)) if divisor > 0.0 => Some(value / divisor),
        _ => None,
    };
    let pass = match value {
        Some(value) => {
            check.min.is_none_or(|min| value >= min) && check.max.is_none_or(|max| value <= max)
        }
        None => false,
    };
    json!({
        "metric": check.metric,
        "per_ok": check.per_ok,
        "per": check.per,
        "per_second": check.per_second,
        "value": value,
        "min": check.min,
        "max": check.max,
        "timing": check.timing,
        "pass": pass,
    })
}

fn matches(filter: Option<&Vec<String>>, value: &Value) -> bool {
    match filter {
        None => true,
        Some(allowed) => value
            .as_str()
            .is_some_and(|value| allowed.iter().any(|allowed| allowed == value)),
    }
}

fn metric(metric: &str, phase: &Value) -> Option<f64> {
    let (kind, argument) = metric.split_once(':').unwrap_or((metric, ""));
    let server = &phase["server"];
    match kind {
        "bucket" => {
            let mut classes = None;
            let mut ops = None;
            let mut outcomes = None;
            for filter in argument.split(',').filter(|part| !part.is_empty()) {
                let (name, values) = filter.split_once('=')?;
                let values: Vec<String> = values.split('+').map(str::to_string).collect();
                match name {
                    "class" => classes = Some(values),
                    "op" => ops = Some(values),
                    "outcome" => outcomes = Some(values),
                    _ => return None,
                }
            }
            let rows = server["bucket_requests"].as_array()?;
            Some(
                rows.iter()
                    .filter(|row| {
                        matches(classes.as_ref(), &row["class"])
                            && matches(ops.as_ref(), &row["op"])
                            && matches(outcomes.as_ref(), &row["outcome"])
                    })
                    .map(|row| row["count"].as_f64().unwrap_or(0.0))
                    .sum(),
            )
        }
        "counter" => server["counters"][argument].as_f64(),
        "hist_count" => server["histograms"][argument]["count"].as_f64(),
        "hist_p50" => server["histograms"][argument]["p50"].as_f64(),
        "hist_p99" => server["histograms"][argument]["p99"].as_f64(),
        "client" => match argument {
            "ok" | "shed" | "achieved_rate" => phase[argument].as_f64(),
            "errors" => Some(error_total(phase)),
            "error_rate" => {
                let scheduled = phase["scheduled"].as_f64()?;
                (scheduled > 0.0).then(|| error_total(phase) / scheduled)
            }
            "p50_us" => phase["latency_us"]["p50"].as_f64(),
            "p99_us" => phase["latency_us"]["p99"].as_f64(),
            "p999_us" => phase["latency_us"]["p999"].as_f64(),
            _ => None,
        },
        "node" => phase["nodes"]
            .as_array()?
            .iter()
            .filter_map(|node| node[argument].as_f64())
            .reduce(f64::max),
        _ => None,
    }
}

fn error_total(phase: &Value) -> f64 {
    phase["errors"]
        .as_object()
        .map(|errors| errors.values().filter_map(Value::as_f64).sum())
        .unwrap_or(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check(metric: &str, per_ok: bool, max: f64) -> Check {
        Check {
            metric: metric.into(),
            per_ok,
            per: None,
            per_second: false,
            min: None,
            max: Some(max),
            timing: false,
        }
    }

    #[test]
    fn bucket_filters_and_per_ok() {
        let phase = json!({
            "ok": 100,
            "scheduled": 100,
            "errors": {"http_500": 2},
            "latency_us": {"p99": 7000},
            "server": {
                "bucket_requests": [
                    {"op": "get", "class": "nodes", "outcome": "ok", "count": 30},
                    {"op": "put", "class": "cell_data", "outcome": "ok", "count": 100},
                    {"op": "get", "class": "cell_owner", "outcome": "ok", "count": 100},
                ],
                "counters": {"core.requests": 100},
                "histograms": {},
            },
        });
        let result = evaluate(
            &check("bucket:class=cell_data+cell_owner", true, 2.0),
            &phase,
        );
        assert_eq!(result["value"], 2.0);
        assert_eq!(result["pass"], true);
        let result = evaluate(&check("bucket:class=cell_data,op=put", true, 0.5), &phase);
        assert_eq!(result["pass"], false);
        let result = evaluate(&check("counter:core.requests", true, 1.0), &phase);
        assert_eq!(result["pass"], true);
        let result = evaluate(&check("client:error_rate", false, 0.01), &phase);
        assert_eq!(result["value"], 0.02);
        assert_eq!(result["pass"], false);
        let result = evaluate(&check("client:nope", false, 1.0), &phase);
        assert_eq!(result["pass"], false);
        let mut per = check("bucket:class=cell_data", false, 1.0);
        per.per = Some("counter:core.requests".into());
        assert_eq!(evaluate(&per, &phase)["value"], 1.0);
    }
}
