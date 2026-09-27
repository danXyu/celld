// Copyright 2026 Deno Land Inc. Apache-2.0 license.

//! OTLP/HTTP protobuf encoding for the telemetry sink, by hand.
//!
//! The export messages celld emits are a small, stable corner of the
//! OTLP proto — flat spans and log records with scalar attributes — and
//! protobuf's wire format is varints and length-delimited fields. Hand
//! encoding keeps prost, tonic, and the generated proto crates out of
//! the binary, the same trade the Parquet sink made by skipping arrow.
//! Field numbers follow opentelemetry-proto v1: trace/v1/trace.proto,
//! logs/v1/logs.proto, metrics/v1/metrics.proto, common/v1/common.proto,
//! resource/v1/resource.proto.

use crate::metrics::ExpHistogram;
use crate::metrics::GaugeValue;
use crate::metrics::Snapshot;
use crate::telemetry::Log;
use crate::telemetry::Span;

const WIRE_VARINT: u64 = 0;
const WIRE_FIXED64: u64 = 1;
const WIRE_LEN: u64 = 2;

fn varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn key(out: &mut Vec<u8>, field: u64, wire: u64) {
    varint(out, (field << 3) | wire);
}

fn field_bytes(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    key(out, field, WIRE_LEN);
    varint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn field_str(out: &mut Vec<u8>, field: u64, value: &str) {
    field_bytes(out, field, value.as_bytes());
}

fn field_varint(out: &mut Vec<u8>, field: u64, value: u64) {
    key(out, field, WIRE_VARINT);
    varint(out, value);
}

fn field_fixed64(out: &mut Vec<u8>, field: u64, value: u64) {
    key(out, field, WIRE_FIXED64);
    out.extend_from_slice(&value.to_le_bytes());
}

fn field_double(out: &mut Vec<u8>, field: u64, value: f64) {
    field_fixed64(out, field, value.to_bits());
}

/// `sint32`: zigzag, so a small negative value stays one byte.
fn field_sint32(out: &mut Vec<u8>, field: u64, value: i32) {
    field_varint(out, field, ((value << 1) ^ (value >> 31)) as u32 as u64);
}

/// common.v1.AnyValue: string_value=1, bool_value=2, int_value=3.
fn any_string(value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(value.len() + 4);
    field_str(&mut out, 1, value);
    out
}

fn any_bool(value: bool) -> Vec<u8> {
    let mut out = Vec::new();
    field_varint(&mut out, 2, value as u64);
    out
}

fn any_int(value: i64) -> Vec<u8> {
    let mut out = Vec::new();
    field_varint(&mut out, 3, value as u64);
    out
}

/// common.v1.KeyValue: key=1, value=2 (AnyValue).
fn key_value(out: &mut Vec<u8>, field: u64, name: &str, value: Vec<u8>) {
    let mut kv = Vec::with_capacity(name.len() + value.len() + 8);
    field_str(&mut kv, 1, name);
    field_bytes(&mut kv, 2, &value);
    field_bytes(out, field, &kv);
}

/// resource.v1.Resource: attributes=1.
fn resource(node: &str, region: &str, service: &str) -> Vec<u8> {
    resource_with(
        node,
        region,
        service,
        crate::telemetry::resource_attributes(),
    )
}

/// The resource with the operator's `OTEL_RESOURCE_ATTRIBUTES` appended.
/// celld's own keys come first and win a collision, as `OTEL_SERVICE_NAME`
/// wins over a `service.name` in that list under the otel spec.
fn resource_with(node: &str, region: &str, service: &str, extra: &[(String, String)]) -> Vec<u8> {
    const OWN: [&str; 4] = [
        "service.name",
        "service.version",
        "service.instance.id",
        "celld.region",
    ];
    let mut out = Vec::new();
    key_value(&mut out, 1, "service.name", any_string(service));
    key_value(
        &mut out,
        1,
        "service.version",
        any_string(env!("CARGO_PKG_VERSION")),
    );
    key_value(&mut out, 1, "service.instance.id", any_string(node));
    key_value(&mut out, 1, "celld.region", any_string(region));
    for (name, value) in extra {
        if !OWN.contains(&name.as_str()) {
            key_value(&mut out, 1, name, any_string(value));
        }
    }
    out
}

/// common.v1.InstrumentationScope: name=1, version=2.
fn scope() -> Vec<u8> {
    let mut out = Vec::new();
    field_str(&mut out, 1, "celld");
    field_str(&mut out, 2, env!("CARGO_PKG_VERSION"));
    out
}

/// trace.v1.Span.
fn span_message(span: &Span) -> Vec<u8> {
    let mut out = Vec::new();
    field_bytes(&mut out, 1, &span.ids.trace_id);
    field_bytes(&mut out, 2, &span.ids.span_id);
    if let Some(parent) = span.parent_span_id {
        field_bytes(&mut out, 4, &parent);
    }
    field_str(&mut out, 5, span.name);
    field_varint(&mut out, 6, span.kind as u64);
    let start_ns = span.start_unix_us.max(0) as u64 * 1_000;
    let end_ns = start_ns + span.duration_us.max(0) as u64 * 1_000;
    field_fixed64(&mut out, 7, start_ns);
    field_fixed64(&mut out, 8, end_ns);
    // Attributes (9): the same promoted columns the Parquet schema has,
    // under otel semantic-convention names where one exists.
    if let Some(url) = &span.url {
        key_value(&mut out, 9, "url.full", any_string(url));
    }
    if let Some(status) = span.http_status {
        key_value(
            &mut out,
            9,
            "http.response.status_code",
            any_int(status as i64),
        );
    }
    if let Some(request_id) = &span.request_id {
        key_value(&mut out, 9, "celld.request_id", any_string(request_id));
    }
    if let Some(cell) = &span.cell {
        key_value(&mut out, 9, "celld.cell", any_string(cell));
    }
    if let Some(epoch) = span.epoch {
        key_value(&mut out, 9, "celld.epoch", any_int(epoch as i64));
    }
    if let Some(isolate) = span.isolate {
        key_value(&mut out, 9, "celld.isolate", any_int(isolate as i64));
    }
    if let Some(queue_wait_us) = span.queue_wait_us {
        key_value(&mut out, 9, "celld.queue_wait_us", any_int(queue_wait_us));
    }
    if let Some(remote) = span.parent_remote {
        key_value(&mut out, 9, "celld.parent_remote", any_bool(remote));
    }
    // Status (15): unset when ok, per the spec; ERROR (code=3 value 2)
    // with the message when not.
    if !span.ok {
        let mut status = Vec::new();
        if let Some(error) = &span.error {
            field_str(&mut status, 2, error);
        }
        field_varint(&mut status, 3, 2);
        field_bytes(&mut out, 15, &status);
    }
    out
}

/// collector.v1.ExportTraceServiceRequest: resource_spans=1, holding one
/// ResourceSpans{resource=1, scope_spans=2{scope=1, spans=2}}.
pub fn traces_request(spans: &[Span], node: &str, region: &str, service: &str) -> Vec<u8> {
    let mut scope_spans = Vec::new();
    field_bytes(&mut scope_spans, 1, &scope());
    for span in spans {
        field_bytes(&mut scope_spans, 2, &span_message(span));
    }
    let mut resource_spans = Vec::new();
    field_bytes(&mut resource_spans, 1, &resource(node, region, service));
    field_bytes(&mut resource_spans, 2, &scope_spans);
    let mut out = Vec::new();
    field_bytes(&mut out, 1, &resource_spans);
    out
}

/// logs.v1.LogRecord: time=1, severity_number=2, body=5, trace_id=9,
/// span_id=10, observed_time=11.
fn log_message(log: &Log) -> Vec<u8> {
    let mut out = Vec::new();
    let time_ns = log.time_unix_us.max(0) as u64 * 1_000;
    field_fixed64(&mut out, 1, time_ns);
    field_varint(&mut out, 2, 9); // SEVERITY_NUMBER_INFO
    field_bytes(&mut out, 5, &any_string(&log.body));
    if let Some(trace_id) = log.trace_id {
        field_bytes(&mut out, 9, &trace_id);
    }
    if let Some(span_id) = log.span_id {
        field_bytes(&mut out, 10, &span_id);
    }
    field_fixed64(&mut out, 11, time_ns);
    out
}

/// collector.v1.ExportLogsServiceRequest: resource_logs=1, holding one
/// ResourceLogs{resource=1, scope_logs=2{scope=1, log_records=2}}.
pub fn logs_request(logs: &[Log], node: &str, region: &str, service: &str) -> Vec<u8> {
    let mut scope_logs = Vec::new();
    field_bytes(&mut scope_logs, 1, &scope());
    for log in logs {
        field_bytes(&mut scope_logs, 2, &log_message(log));
    }
    let mut resource_logs = Vec::new();
    field_bytes(&mut resource_logs, 1, &resource(node, region, service));
    field_bytes(&mut resource_logs, 2, &scope_logs);
    let mut out = Vec::new();
    field_bytes(&mut out, 1, &resource_logs);
    out
}

/// metrics.v1.AggregationTemporality.
const TEMPORALITY_DELTA: u64 = 1;

/// metrics.v1.NumberDataPoint: start_time=2, time=3, as_double=4,
/// as_int=6 (sfixed64).
fn number_point(start_ns: u64, time_ns: u64, value: GaugeValue) -> Vec<u8> {
    let mut out = Vec::new();
    if start_ns != 0 {
        field_fixed64(&mut out, 2, start_ns);
    }
    field_fixed64(&mut out, 3, time_ns);
    match value {
        GaugeValue::Double(value) => field_double(&mut out, 4, value),
        GaugeValue::Int(value) => field_fixed64(&mut out, 6, value as u64),
    }
    out
}

/// metrics.v1.ExponentialHistogramDataPoint: start_time=2, time=3,
/// count=4, sum=5, scale=6, zero_count=7, positive=8{offset=1,
/// bucket_counts=2 packed}, min=12, max=13.
fn exponential_point(start_ns: u64, time_ns: u64, histogram: &ExpHistogram) -> Vec<u8> {
    let mut out = Vec::new();
    field_fixed64(&mut out, 2, start_ns);
    field_fixed64(&mut out, 3, time_ns);
    field_fixed64(&mut out, 4, histogram.count);
    field_double(&mut out, 5, histogram.sum);
    field_sint32(&mut out, 6, histogram.scale);
    if histogram.zero_count != 0 {
        field_fixed64(&mut out, 7, histogram.zero_count);
    }
    if !histogram.counts.is_empty() {
        let mut positive = Vec::new();
        field_sint32(&mut positive, 1, histogram.offset);
        let mut packed = Vec::new();
        for count in &histogram.counts {
            varint(&mut packed, *count);
        }
        field_bytes(&mut positive, 2, &packed);
        field_bytes(&mut out, 8, &positive);
    }
    field_double(&mut out, 12, histogram.min);
    field_double(&mut out, 13, histogram.max);
    out
}

/// metrics.v1.Metric: name=1, description=2, unit=3, and one of gauge=5,
/// sum=7, exponential_histogram=10.
fn metric(name: &str, description: &str, unit: &str, field: u64, data: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    field_str(&mut out, 1, name);
    field_str(&mut out, 2, description);
    field_str(&mut out, 3, unit);
    field_bytes(&mut out, field, data);
    out
}

/// collector.v1.ExportMetricsServiceRequest: resource_metrics=1, holding
/// one ResourceMetrics{resource=1, scope_metrics=2{scope=1, metrics=2}}.
///
/// No data point carries an attribute: a series is a node, never a cell,
/// so cardinality follows the fleet's size and not its tenancy.
pub fn metrics_request(snapshot: &Snapshot, node: &str, region: &str, service: &str) -> Vec<u8> {
    let mut scope_metrics = Vec::new();
    field_bytes(&mut scope_metrics, 1, &scope());
    for gauge in &snapshot.gauges {
        // Gauge: data_points=1.
        let mut data = Vec::new();
        field_bytes(
            &mut data,
            1,
            &number_point(0, snapshot.time_ns, gauge.value),
        );
        field_bytes(
            &mut scope_metrics,
            2,
            &metric(gauge.name, gauge.description, gauge.unit, 5, &data),
        );
    }
    for sum in &snapshot.sums {
        // Sum: data_points=1, aggregation_temporality=2, is_monotonic=3.
        let mut data = Vec::new();
        field_bytes(
            &mut data,
            1,
            &number_point(
                snapshot.start_ns,
                snapshot.time_ns,
                GaugeValue::Int(sum.value as i64),
            ),
        );
        field_varint(&mut data, 2, TEMPORALITY_DELTA);
        field_varint(&mut data, 3, 1);
        field_bytes(
            &mut scope_metrics,
            2,
            &metric(sum.name, sum.description, sum.unit, 7, &data),
        );
    }
    for distribution in &snapshot.distributions {
        let Some(histogram) = &distribution.histogram else {
            continue;
        };
        // ExponentialHistogram: data_points=1, aggregation_temporality=2.
        let mut data = Vec::new();
        field_bytes(
            &mut data,
            1,
            &exponential_point(snapshot.start_ns, snapshot.time_ns, histogram),
        );
        field_varint(&mut data, 2, TEMPORALITY_DELTA);
        field_bytes(
            &mut scope_metrics,
            2,
            &metric(
                distribution.name,
                distribution.description,
                distribution.unit,
                10,
                &data,
            ),
        );
    }
    let mut resource_metrics = Vec::new();
    field_bytes(&mut resource_metrics, 1, &resource(node, region, service));
    field_bytes(&mut resource_metrics, 2, &scope_metrics);
    let mut out = Vec::new();
    field_bytes(&mut out, 1, &resource_metrics);
    out
}

#[cfg(test)]
mod tests {
    use crate::metrics::tests::all;
    use crate::metrics::tests::decode;
    use crate::metrics::tests::message;
    use crate::metrics::tests::text;
    use crate::metrics::tests::Wire;

    fn attributes(resource: &[u8]) -> Vec<(String, String)> {
        all(&decode(resource), 1)
            .into_iter()
            .map(|kv| match kv {
                Wire::Len(bytes) => {
                    let kv = decode(&bytes);
                    (text(&kv, 1), text(&message(&kv, 2), 1))
                }
                other => panic!("{other:?}"),
            })
            .collect()
    }

    #[test]
    fn resource_attributes_are_appended_and_cannot_override_celld() {
        let extra = [
            ("celld.fleet".to_string(), "caddy".to_string()),
            ("service.name".to_string(), "impostor".to_string()),
            ("service.instance.id".to_string(), "impostor".to_string()),
        ];
        let attributes = attributes(&super::resource_with("node-1", "eu", "celld", &extra));
        let names: Vec<&str> = attributes.iter().map(|(name, _)| name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "service.name",
                "service.version",
                "service.instance.id",
                "celld.region",
                "celld.fleet",
            ]
        );
        assert_eq!(attributes[0].1, "celld");
        assert_eq!(attributes[2].1, "node-1");
        assert_eq!(attributes[4].1, "caddy");
    }
}
