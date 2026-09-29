// Copyright 2026 Deno Land Inc. Apache-2.0 license.

use super::*;
use crate::ownership_store::NodeLoadWire;

// ---------------------------------------------------------------------------
// A protobuf reader just wide enough to walk what `otlp` writes.

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum Wire {
    Varint(u64),
    Fixed64(u64),
    Len(Vec<u8>),
}

fn varint(bytes: &mut &[u8]) -> u64 {
    let mut value = 0u64;
    let mut shift = 0;
    loop {
        let byte = bytes[0];
        *bytes = &bytes[1..];
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return value;
        }
        shift += 7;
    }
}

pub(crate) fn decode(mut bytes: &[u8]) -> Vec<(u64, Wire)> {
    let mut fields = Vec::new();
    while !bytes.is_empty() {
        let key = varint(&mut bytes);
        let value = match key & 7 {
            0 => Wire::Varint(varint(&mut bytes)),
            1 => {
                let value = u64::from_le_bytes(bytes[..8].try_into().unwrap());
                bytes = &bytes[8..];
                Wire::Fixed64(value)
            }
            2 => {
                let len = varint(&mut bytes) as usize;
                let value = bytes[..len].to_vec();
                bytes = &bytes[len..];
                Wire::Len(value)
            }
            wire => panic!("unexpected wire type {wire}"),
        };
        fields.push((key >> 3, value));
    }
    fields
}

pub(crate) fn all(fields: &[(u64, Wire)], field: u64) -> Vec<Wire> {
    fields
        .iter()
        .filter(|(number, _)| *number == field)
        .map(|(_, value)| value.clone())
        .collect()
}

pub(crate) fn one(fields: &[(u64, Wire)], field: u64) -> Wire {
    let mut values = all(fields, field);
    assert_eq!(values.len(), 1, "field {field} in {fields:?}");
    values.remove(0)
}

pub(crate) fn message(fields: &[(u64, Wire)], field: u64) -> Vec<(u64, Wire)> {
    match one(fields, field) {
        Wire::Len(bytes) => decode(&bytes),
        other => panic!("field {field} is not a message: {other:?}"),
    }
}

pub(crate) fn text(fields: &[(u64, Wire)], field: u64) -> String {
    match one(fields, field) {
        Wire::Len(bytes) => String::from_utf8(bytes).unwrap(),
        other => panic!("field {field} is not a string: {other:?}"),
    }
}

fn fixed(fields: &[(u64, Wire)], field: u64) -> u64 {
    match one(fields, field) {
        Wire::Fixed64(value) => value,
        other => panic!("field {field} is not fixed64: {other:?}"),
    }
}

fn double(fields: &[(u64, Wire)], field: u64) -> f64 {
    f64::from_bits(fixed(fields, field))
}

fn varint_of(fields: &[(u64, Wire)], field: u64) -> u64 {
    match one(fields, field) {
        Wire::Varint(value) => value,
        other => panic!("field {field} is not a varint: {other:?}"),
    }
}

fn unzigzag(value: u64) -> i64 {
    ((value >> 1) as i64) ^ -((value & 1) as i64)
}

// ---------------------------------------------------------------------------
// CPU

#[test]
fn cpu_totals_reset_each_interval() {
    let cpu = CellCpu::default();
    let a = cpu.account("a");
    let b = cpu.account("b");
    a.fetch_add(3_000, Ordering::Relaxed);
    a.fetch_add(2_000, Ordering::Relaxed);
    b.fetch_add(7, Ordering::Relaxed);

    let mut first = cpu.take();
    first.sort();
    assert_eq!(first, vec![7, 5_000]);

    // Nothing ran since: nothing to observe.
    assert!(cpu.take().is_empty());

    a.fetch_add(11, Ordering::Relaxed);
    assert_eq!(cpu.take(), vec![11]);
}

#[test]
fn a_cell_with_no_turns_records_nothing() {
    let cpu = CellCpu::default();
    let _idle = cpu.account("idle");
    let busy = cpu.account("busy");
    busy.fetch_add(1, Ordering::Relaxed);
    assert_eq!(cpu.take(), vec![1]);
}

#[test]
fn the_same_cell_shares_one_account() {
    let cpu = CellCpu::default();
    let first = cpu.account("cell");
    let second = cpu.account("cell");
    first.fetch_add(1, Ordering::Relaxed);
    second.fetch_add(2, Ordering::Relaxed);
    assert_eq!(cpu.take(), vec![3]);
}

#[test]
fn an_account_no_event_holds_is_forgotten_once_drained() {
    let cpu = CellCpu::default();
    let held = cpu.account("held");
    let released = cpu.account("released");
    released.fetch_add(5, Ordering::Relaxed);
    drop(released);

    // Its last total is still reported.
    assert_eq!(cpu.take(), vec![5]);
    assert_eq!(cpu.len(), 1, "only the account an event holds remains");

    drop(held);
    assert!(cpu.take().is_empty());
    assert!(cpu.is_empty());
}

// ---------------------------------------------------------------------------
// Heap

#[test]
fn heap_divides_by_resident_cells() {
    let one = tokio::sync::Mutex::new(Some(900u64));
    let three = tokio::sync::Mutex::new(Some(3_000u64));
    let mut heaps = CellHeaps::default();
    heaps.sample([(1, &one), (3, &three)], |bytes| *bytes);
    assert_eq!(
        heaps.isolates,
        vec![
            IsolateHeap {
                cells: 1,
                bytes: 900
            },
            IsolateHeap {
                cells: 3,
                bytes: 3_000
            },
        ]
    );
    assert_eq!(heaps.per_cell(), vec![900.0, 1_000.0, 1_000.0, 1_000.0]);
    assert_eq!(heaps.skipped, 0);
}

#[test]
fn heap_skips_busy_isolates_without_blocking_and_counts_them() {
    let busy = tokio::sync::Mutex::new(Some(1_000u64));
    let idle = tokio::sync::Mutex::new(Some(2_000u64));
    let _turn = busy.try_lock().unwrap();
    let mut heaps = CellHeaps::default();
    // A blocking read would deadlock this single thread on `_turn`.
    heaps.sample([(2, &busy), (1, &idle)], |bytes| *bytes);
    assert_eq!(heaps.skipped, 1);
    assert_eq!(heaps.per_cell(), vec![2_000.0]);
}

#[test]
fn heap_passes_over_empty_and_freed_isolates() {
    let empty = tokio::sync::Mutex::new(Some(5_000u64));
    let freed = tokio::sync::Mutex::new(None::<u64>);
    let _held = empty.try_lock().unwrap();
    let mut heaps = CellHeaps::default();
    heaps.sample([(0, &empty), (4, &freed)], |bytes| *bytes);
    // An empty isolate is not looked at, so holding it is not a skip.
    assert_eq!(heaps.skipped, 0);
    assert!(heaps.isolates.is_empty());
    assert!(heaps.per_cell().is_empty());
}

// ---------------------------------------------------------------------------
// Gauges

fn gauge(gauges: &[Gauge], name: &str) -> Option<GaugeValue> {
    gauges
        .iter()
        .find(|gauge| gauge.name == name)
        .map(|gauge| gauge.value)
}

/// A `/state` body: the actor's top-level counts, the node load exactly as
/// `NodeLoadWire` serializes it, and the deployment's isolate census.
fn state_with(load: &NodeLoadWire) -> serde_json::Value {
    let mut state = serde_json::json!({
        "ownership": "bucket",
        "owned_cells": 40,
        "occupied": 12,
        "restoring": 3,
        "activation_waiting": 2,
        "capacity_waiting": 1,
        "rss_bytes": 1,
        "node_load": serde_json::to_value(load).unwrap(),
        "deployment": {
            "isolates": {
                "stateless": {"live": 9, "cells": 0},
                "services": {},
                "cells": {
                    "a": {"live": 2, "cells": 5},
                    "b": {"live": 1, "cells": 1},
                },
            },
            "draining": [
                {"isolates": {"stateless": {"live": 1, "cells": 0}, "services": {}, "cells": {"a": {"live": 1, "cells": 4}}}},
            ],
        },
    });
    state["shutdown"] = serde_json::json!({});
    state
}

#[test]
fn gauges_equal_the_state_fields_of_the_same_snapshot() {
    let load = NodeLoadWire {
        resident_cells: 12,
        host_websockets: 6,
        rss_bytes: 512 << 20,
        in_use_bytes: Some(400 << 20),
        cgroup_working_set_bytes: Some(450 << 20),
        cpu_percent_x100: 12_345,
        open_fds: 77,
        fd_limit: 1_024,
        pressured: true,
        memory_headroom: Some(false),
        shed_cells: 9,
        draining: Some(true),
        ..Default::default()
    };
    let state = state_with(&load);
    let gauges = node_gauges(&state);
    let int = |name| gauge(&gauges, name);
    use GaugeValue::Double;
    use GaugeValue::Int;

    assert_eq!(int("celld.node.owned_cells"), Some(Int(40)));
    assert_eq!(int("celld.node.resident_cells"), Some(Int(12)));
    assert_eq!(int("celld.node.occupied"), Some(Int(12)));
    assert_eq!(int("celld.node.restoring"), Some(Int(3)));
    assert_eq!(int("celld.node.capacity_waiting"), Some(Int(1)));
    assert_eq!(int("celld.node.activation_waiting"), Some(Int(2)));
    assert_eq!(int("celld.node.host_websockets"), Some(Int(6)));
    assert_eq!(int("celld.node.rss_bytes"), Some(Int(512 << 20)));
    assert_eq!(int("celld.node.in_use_bytes"), Some(Int(400 << 20)));
    assert_eq!(
        int("celld.node.cgroup_working_set_bytes"),
        Some(Int(450 << 20))
    );
    assert_eq!(int("celld.node.cpu_utilization"), Some(Double(1.2345)));
    assert_eq!(int("celld.node.open_fds"), Some(Int(77)));
    assert_eq!(int("celld.node.fd_limit"), Some(Int(1_024)));
    assert_eq!(int("celld.node.pressured"), Some(Int(1)));
    assert_eq!(int("celld.node.memory_headroom"), Some(Int(0)));
    assert_eq!(int("celld.node.draining"), Some(Int(1)));
    assert_eq!(int("celld.node.shed_cells"), Some(Int(9)));
    // Cell pools only, current and draining generations together.
    assert_eq!(int("celld.node.isolates"), Some(Int(4)));
    assert_eq!(int("celld.node.isolate_cells"), Some(Int(10)));
}

#[test]
fn absent_optional_fields_produce_no_gauge() {
    let load = NodeLoadWire::default();
    let state = state_with(&load);
    let gauges = node_gauges(&state);
    for name in [
        "celld.node.in_use_bytes",
        "celld.node.cgroup_working_set_bytes",
        "celld.node.memory_headroom",
        "celld.node.draining",
    ] {
        assert_eq!(gauge(&gauges, name), None, "{name}");
    }
    assert_eq!(
        gauge(&gauges, "celld.node.rss_bytes"),
        Some(GaugeValue::Int(0))
    );
}

#[test]
fn a_node_without_load_or_deployment_reports_only_what_it_has() {
    let state = serde_json::json!({"owned_cells": 2, "occupied": 1, "node_load": null});
    let gauges = node_gauges(&state);
    let names: Vec<_> = gauges.iter().map(|gauge| gauge.name).collect();
    assert_eq!(names, vec!["celld.node.owned_cells", "celld.node.occupied"]);
}

#[test]
fn export_gauges_appear_only_with_an_export_object() {
    let state = state_with(&NodeLoadWire::default());
    let gauges = node_gauges(&state);
    assert!(
        gauges
            .iter()
            .all(|gauge| !gauge.name.starts_with("celld.export.")),
        "export off must add no gauge"
    );

    let mut state = state;
    state["export"] = serde_json::json!({
        "queue_bytes": 4096,
        "pending_commits": 3,
        "dropped_records": 2,
        "gaps": 1,
        "bulk_commits": 5,
        "attribution_mismatches": 0,
    });
    let gauges = node_gauges(&state);
    use GaugeValue::Int;
    assert_eq!(gauge(&gauges, "celld.export.queue_bytes"), Some(Int(4096)));
    assert_eq!(gauge(&gauges, "celld.export.pending_commits"), Some(Int(3)));
    assert_eq!(gauge(&gauges, "celld.export.dropped_records"), Some(Int(2)));
    assert_eq!(gauge(&gauges, "celld.export.gaps"), Some(Int(1)));
    assert_eq!(gauge(&gauges, "celld.export.bulk_commits"), Some(Int(5)));
    assert_eq!(
        gauge(&gauges, "celld.export.attribution_mismatches"),
        Some(Int(0))
    );
}

// ---------------------------------------------------------------------------
// Exponential histogram

fn bounds(histogram: &ExpHistogram, bucket: usize) -> (f64, f64) {
    // `base^index` as one `powf`: repeated products of a base this close to
    // one drift by more than a bucket at fine scales.
    let index = (histogram.offset + bucket as i32) as f64;
    let step = 2f64.powi(-histogram.scale);
    (2f64.powf(index * step), 2f64.powf((index + 1.0) * step))
}

#[test]
fn histogram_summary_is_exact() {
    let values = [0.25, 3.0, 0.0, 1_000.0, 7.5];
    let histogram = ExpHistogram::from_values(&values).unwrap();
    assert_eq!(histogram.count, 5);
    assert_eq!(histogram.sum, 1_010.75);
    assert_eq!(histogram.min, 0.0);
    assert_eq!(histogram.max, 1_000.0);
    assert_eq!(histogram.zero_count, 1);
    assert_eq!(histogram.counts.iter().sum::<u64>(), 4);
}

#[test]
fn every_value_lands_in_its_bucket() {
    let values: Vec<f64> = (1..=500).map(|i| (i as f64).powf(1.7) * 0.001).collect();
    let histogram = ExpHistogram::from_values(&values).unwrap();
    assert!(histogram.counts.len() as i64 <= MAX_BUCKETS);
    for value in &values {
        let index = bucket_index(*value, histogram.scale) - histogram.offset as i64;
        let (low, high) = bounds(&histogram, index as usize);
        let tolerance = high * 1e-9;
        assert!(
            *value > low - tolerance && *value <= high + tolerance,
            "{value} not in ({low}, {high}] at scale {}",
            histogram.scale
        );
    }
}

#[test]
fn a_wide_spread_downscales_to_fit_the_bucket_budget() {
    let values = [1e-6, 1.0, 1e9];
    let histogram = ExpHistogram::from_values(&values).unwrap();
    assert!(histogram.counts.len() as i64 <= MAX_BUCKETS);
    assert!(histogram.scale < MAX_SCALE);
    assert_eq!(histogram.counts.iter().sum::<u64>(), 3);
    assert_eq!(histogram.counts.first(), Some(&1));
    assert_eq!(histogram.counts.last(), Some(&1));
}

#[test]
fn a_single_value_keeps_the_finest_scale() {
    let histogram = ExpHistogram::from_values(&[4096.0]).unwrap();
    assert_eq!(histogram.scale, MAX_SCALE);
    assert_eq!(histogram.counts, vec![1]);
    // An exact power of two is its bucket's inclusive upper bound.
    let (low, high) = bounds(&histogram, 0);
    assert!(low < 4096.0 && (high - 4096.0).abs() < 1e-6);
}

#[test]
fn no_observations_is_no_histogram() {
    assert_eq!(ExpHistogram::from_values(&[]), None);
    assert_eq!(ExpHistogram::from_values(&[f64::NAN, -1.0]), None);
}

// ---------------------------------------------------------------------------
// Encoding

fn sample_snapshot() -> Snapshot {
    let load = NodeLoadWire {
        resident_cells: 3,
        cpu_percent_x100: 5_000,
        ..Default::default()
    };
    let state = state_with(&load);
    let busy = tokio::sync::Mutex::new(Some(10u64));
    let idle = tokio::sync::Mutex::new(Some(6_000u64));
    let _turn = busy.try_lock().unwrap();
    let mut heaps = CellHeaps::default();
    heaps.sample([(1, &busy), (2, &idle)], |bytes| *bytes);
    Snapshot::new(
        1_000,
        61_000_000_001,
        Some(&state),
        &[1_500_000, 250_000_000],
        &heaps,
        2,
    )
}

#[test]
fn metrics_request_round_trips() {
    let snapshot = sample_snapshot();
    let bytes = crate::otlp::metrics_request(&snapshot, "node-1", "us-east", "celld");

    let request = decode(&bytes);
    let resource_metrics = message(&request, 1);
    let resource = message(&resource_metrics, 1);
    let attributes: Vec<(String, String)> = all(&resource, 1)
        .into_iter()
        .map(|kv| match kv {
            Wire::Len(bytes) => {
                let kv = decode(&bytes);
                (text(&kv, 1), text(&message(&kv, 2), 1))
            }
            other => panic!("{other:?}"),
        })
        .collect();
    assert!(attributes.contains(&("service.name".into(), "celld".into())));
    assert!(attributes.contains(&("service.instance.id".into(), "node-1".into())));

    let scope_metrics = message(&resource_metrics, 2);
    assert_eq!(text(&message(&scope_metrics, 1), 1), "celld");
    let metrics: Vec<Vec<(u64, Wire)>> = all(&scope_metrics, 2)
        .into_iter()
        .map(|metric| match metric {
            Wire::Len(bytes) => decode(&bytes),
            other => panic!("{other:?}"),
        })
        .collect();
    let named = |name: &str| {
        metrics
            .iter()
            .find(|metric| text(metric, 1) == name)
            .unwrap_or_else(|| panic!("no metric {name}"))
            .clone()
    };

    // A gauge carries an int or a double and no attributes.
    let resident = named("celld.node.resident_cells");
    let point = message(&message(&resident, 5), 1);
    assert_eq!(fixed(&point, 6), 3);
    assert_eq!(fixed(&point, 3), 61_000_000_001);
    assert!(all(&point, 7).is_empty(), "no attributes");
    let cpu = named("celld.node.cpu_utilization");
    assert_eq!(double(&message(&message(&cpu, 5), 1), 4), 0.5);

    // A delta, monotonic sum.
    let skipped = named("celld.cell.heap_samples_skipped");
    let sum = message(&skipped, 7);
    assert_eq!(varint_of(&sum, 2), 1, "delta");
    assert_eq!(varint_of(&sum, 3), 1, "monotonic");
    let point = message(&sum, 1);
    assert_eq!(fixed(&point, 6), 1);
    assert_eq!(fixed(&point, 2), 1_000);
    let shed = named("celld.metrics.shed");
    assert_eq!(fixed(&message(&message(&shed, 7), 1), 6), 2);

    // Exponential histograms: min, max, sum, and count round-trip.
    for (name, unit, expected) in [
        (
            "celld.cell.cpu_time",
            "s",
            snapshot.distributions[0].histogram.clone().unwrap(),
        ),
        (
            "celld.cell.heap_bytes",
            "By",
            snapshot.distributions[1].histogram.clone().unwrap(),
        ),
    ] {
        let metric = named(name);
        assert_eq!(text(&metric, 3), unit);
        let histogram = message(&metric, 10);
        assert_eq!(varint_of(&histogram, 2), 1, "delta");
        let point = message(&histogram, 1);
        assert!(all(&point, 1).is_empty(), "no attributes");
        assert_eq!(fixed(&point, 2), 1_000);
        assert_eq!(fixed(&point, 3), 61_000_000_001);
        assert_eq!(fixed(&point, 4), expected.count);
        assert_eq!(double(&point, 5), expected.sum);
        assert_eq!(unzigzag(varint_of(&point, 6)), expected.scale as i64);
        assert_eq!(expected.zero_count, 0);
        assert!(all(&point, 7).is_empty(), "a zero count is the default");
        assert_eq!(double(&point, 12), expected.min);
        assert_eq!(double(&point, 13), expected.max);
        let positive = message(&point, 8);
        assert_eq!(unzigzag(varint_of(&positive, 1)), expected.offset as i64);
        let Wire::Len(packed) = one(&positive, 2) else {
            panic!("bucket counts are packed");
        };
        let mut counts = Vec::new();
        let mut rest = packed.as_slice();
        while !rest.is_empty() {
            counts.push(varint(&mut rest));
        }
        assert_eq!(counts, expected.counts);
    }

    let heap = snapshot.distributions[1].histogram.as_ref().unwrap();
    assert_eq!((heap.count, heap.min, heap.max), (2, 3_000.0, 3_000.0));
    let cpu = snapshot.distributions[0].histogram.as_ref().unwrap();
    assert_eq!((cpu.count, cpu.min, cpu.max), (2, 0.0015, 0.25));
}

#[test]
fn an_empty_distribution_is_left_out() {
    let snapshot = Snapshot::new(1, 2, None, &[], &CellHeaps::default(), 0);
    let bytes = crate::otlp::metrics_request(&snapshot, "n", "r", "s");
    let scope_metrics = message(&message(&decode(&bytes), 1), 2);
    let names: Vec<String> = all(&scope_metrics, 2)
        .into_iter()
        .map(|metric| match metric {
            Wire::Len(bytes) => text(&decode(&bytes), 1),
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        names,
        vec!["celld.cell.heap_samples_skipped", "celld.metrics.shed"]
    );
}

// ---------------------------------------------------------------------------
// Configuration

fn config(vars: &[(&str, &str)]) -> anyhow::Result<Option<crate::telemetry::Config>> {
    let vars: HashMap<String, String> = vars
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    crate::telemetry::Config::from_lookup(|name| Ok(vars.get(name).cloned()))
}

#[test]
fn metrics_default_on_with_a_collector() {
    let config = config(&[("CELLD_OTEL", "http://collector:4318")])
        .unwrap()
        .unwrap();
    assert!(config.metrics);
    assert_eq!(config.metrics_interval, std::time::Duration::from_secs(60));
    assert!(config.resource_attributes.is_empty());
}

#[test]
fn metrics_configuration_is_read() {
    let config = config(&[
        ("CELLD_OTEL", "http://collector:4318"),
        ("OTEL_METRICS_EXPORTER", "none"),
        ("OTEL_METRIC_EXPORT_INTERVAL", "15000"),
        (
            "OTEL_RESOURCE_ATTRIBUTES",
            "celld.fleet=agentzero, k8s.pod.name=celld-0,note=a%2Cb%3Dc",
        ),
    ])
    .unwrap()
    .unwrap();
    assert!(!config.metrics);
    assert_eq!(config.metrics_interval, std::time::Duration::from_secs(15));
    assert_eq!(
        config.resource_attributes,
        vec![
            ("celld.fleet".to_string(), "agentzero".to_string()),
            ("k8s.pod.name".to_string(), "celld-0".to_string()),
            ("note".to_string(), "a,b=c".to_string()),
        ]
    );
}

#[test]
fn bad_metrics_configuration_is_refused() {
    let base = ("CELLD_OTEL", "http://collector:4318");
    assert!(config(&[base, ("OTEL_METRICS_EXPORTER", "prometheus")]).is_err());
    assert!(config(&[base, ("OTEL_METRIC_EXPORT_INTERVAL", "0")]).is_err());
    assert!(config(&[base, ("OTEL_RESOURCE_ATTRIBUTES", "novalue")]).is_err());
}
