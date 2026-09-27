# Telemetry

celld can record traces and logs for the requests it serves, and it can
export metrics on node load and on the CPU and memory its cells use. The
feature is off by default, and the off state costs nothing. Set
`CELLD_OTEL=1` to write telemetry to the fleet bucket.
Set `CELLD_OTEL=http://collector:4318` to send telemetry to an OTLP collector.

The default sink is the fleet bucket. celld writes Parquet files under
the `telemetry/` prefix, so a fleet with a bucket has observability
with no other service. DuckDB can query these files directly. An
alternative sink sends the same data to an OpenTelemetry collector.

The schema is version `v0-unstable`. The column names can change
before a stable release, and each file carries the schema version in
its object metadata under the name `celld-schema`. Azure Blob Storage
does not accept a hyphen in a metadata name, so on an `az://` bucket
the name is `celld_schema`.

## Configuration

| variable | default | effect |
| --- | --- | --- |
| `CELLD_OTEL` | `0` | `0` disables telemetry. `1` writes Parquet to the fleet bucket. An HTTP(S) collector base URL selects OTLP/HTTP protobuf. |
| `CELLD_OTEL_BUCKET` | the fleet bucket | A different bucket for the Parquet files, on the same endpoint and credentials. |
| `CELLD_OTEL_RETENTION` | `30d` | celld deletes telemetry files older than this. `none` disables the deletion, so your own lifecycle rules can control the data. |
| `CELLD_OTEL_FLUSH_MS` | `300000` | celld writes a Parquet file after this many milliseconds of buffered events. |
| `CELLD_OTEL_FLUSH_BYTES` | `5242880` | The estimated buffered bytes that trigger a flush before the interval ends. |
| `OTEL_TRACES_SAMPLER` | `parentbased_always_on` | A standard sampler name. `traceidratio` with `OTEL_TRACES_SAMPLER_ARG` records a fraction of the traces. |
| `OTEL_EXPORTER_OTLP_HEADERS` | unset | A comma-separated list of `name=value` headers for the collector. |
| `OTEL_EXPORTER_OTLP_TIMEOUT` | `10000` | The collector request timeout in milliseconds. |
| `OTEL_SERVICE_NAME` | `celld` | The service name in the exported resource. |
| `OTEL_RESOURCE_ATTRIBUTES` | unset | Extra `key=value` resource attributes, comma-separated, with percent-encoded values, such as `celld.fleet=prod`. celld sends them with every OTLP signal. They cannot replace celld's own `service.name`, `service.version`, `service.instance.id`, or `celld.region`. |
| `OTEL_METRICS_EXPORTER` | `otlp` | `none` turns off metrics while keeping traces and logs. |
| `OTEL_METRIC_EXPORT_INTERVAL` | `60000` | The metrics export interval in milliseconds. Each export covers the interval since the previous one. |

The `bucket` sink requires the node to have a fleet bucket
(`CELLD_BUCKET`). The `otlp` sink works on a node without one.

Use a full HTTP(S) collector base URL without a query or a fragment.
celld adds `/v1/traces`, `/v1/logs`, and `/v1/metrics` to its path for the three signals.
`CELLD_OTEL` supplies the collector address, so celld does not read
`OTEL_EXPORTER_OTLP_ENDPOINT`.

`CELLD_OTEL_SINK` is removed. Remove this setting before starting the node.
For a collector, copy its full base URL into `CELLD_OTEL`. For the fleet bucket, keep `CELLD_OTEL=1`.


## What celld records

celld records a span for each request a stateless Worker serves, for
each event a cell serves (a fetch, an alarm, an RPC, a WebSocket
message), for each outbound `fetch()`, and for each cell start. A
span carries the request id, the cell, the isolate, the
queue wait, the outbound URL and status, and the durability facts the
runtime already knows.

celld also records each `console.log` line as a log record. The log
record carries the trace id and the span id of the handler that wrote
it, so a query can join the logs to the traces. The correlation
survives `await`.

celld reads the W3C `traceparent` header on incoming requests, so its
spans join the trace of the system in front of it. celld sends a
`traceparent` header on outbound `fetch()`, so downstream systems can
join too. celld ignores a malformed header and starts a new trace. A
Worker call to a Durable Object stays in one trace.

The sampler decides at the start of a request. An unsampled request
records nothing and costs almost nothing. Under load, telemetry sheds
before requests do, and celld counts what it sheds.

A sampling ratio of `0` records no traces, and a ratio of `1` records
every trace. An intermediate ratio makes the same trace-id decision on
each node.

celld keeps a valid incoming context when the sampler rejects it. An
outbound `fetch()` or Durable Object call keeps the trace id, uses a new
span id, and keeps the sampled flag clear.

## Metrics

celld exports metrics only to an OTLP collector (`CELLD_OTEL=<url>`). The
bucket sink carries traces and logs, but not metrics. celld exports
one set of metrics per interval (`OTEL_METRIC_EXPORT_INTERVAL`, 60 seconds by
default).

No data point carries a cell, script, or request attribute. Each series
belongs to a node, so the number of series grows with the number of nodes and
not with the number of cells. Use `OTEL_RESOURCE_ATTRIBUTES` to name the
fleet, so a dashboard can group its nodes.

### Node load

These metrics are gauges. celld reads them from the same snapshot that
`/state` serves, so a chart and `/state` agree. celld omits a gauge whose
`/state` field is absent or `null`, because an unknown value is not a zero.

| metric | `/state` source |
| --- | --- |
| `celld.node.owned_cells` | `owned_cells` |
| `celld.node.resident_cells` | `node_load.resident_cells` |
| `celld.node.occupied` | `occupied` |
| `celld.node.restoring` | `restoring` |
| `celld.node.capacity_waiting` | `capacity_waiting` |
| `celld.node.activation_waiting` | `activation_waiting` |
| `celld.node.host_websockets` | `node_load.host_websockets` |
| `celld.node.rss_bytes` | `node_load.rss_bytes` |
| `celld.node.in_use_bytes` | `node_load.in_use_bytes` |
| `celld.node.cgroup_working_set_bytes` | `node_load.cgroup_working_set_bytes` |
| `celld.node.cpu_utilization` | `node_load.cpu_percent_x100 / 10000`, a ratio of one core |
| `celld.node.open_fds`, `celld.node.fd_limit` | `node_load` |
| `celld.node.pressured`, `celld.node.memory_headroom`, `celld.node.draining` | `node_load`, as 0 or 1 |
| `celld.node.shed_cells` | `node_load.shed_cells` |
| `celld.node.isolates`, `celld.node.isolate_cells` | the `live` and `cells` counts of the cell pools in `deployment.isolates.cells`, summed with the cell pools of the generations the node still drains |

### Cell distributions

These metrics are delta exponential histograms. Each point carries its
`min`, `max`, `sum`, and `count`. A backend can merge the points of
all the nodes into fleet-wide percentiles, with no bucket bounds to
choose. Datadog maps them to distributions. celld omits a histogram when an
interval has no observations.

`celld.cell.cpu_time` (seconds) has one observation per cell that ran
JavaScript in the interval. The observation is the thread CPU time of that
cell's turns. It is exact, and a facet's CPU counts toward its root cell.
It is a distribution over active cells, not over requests or turns.
When metrics are on, celld reads the thread CPU clock at each end of every
cell turn. It does this even when no CPU limit is set. Stateless Worker
requests are not in this metric.

`celld.cell.heap_bytes` (bytes) has one observation per resident cell. The
observation is its isolate's V8 heap, physical plus external, divided by the
number of cells that share that isolate. Up to 32 cells can share an isolate,
and V8 cannot cheaply attribute memory to one of them, so this value is an
average:

- It is exact for an isolate that holds one cell. When a heavy cell shares an
  isolate with light cells, the metric understates the maximum.
- It counts only the V8 heap. It does not count memory on the Rust side, such
  as storage buffers and sockets.

The heap sample never waits for an isolate that is running a turn. celld skips
that isolate and counts it in `celld.cell.heap_samples_skipped`.

### Delivery

Metrics use the retry and backoff rules of the other OTLP signals. The exporter
retries one export and holds one more export in a queue. celld drops an export
that arrives while both places are taken. It counts the drop in
`celld.metrics.shed`, and it reports that count in the next export that
goes out.

## Query the bucket with DuckDB

```sql
INSTALL httpfs; LOAD httpfs;
CREATE SECRET celld_telemetry (
  TYPE s3, KEY_ID '...', SECRET '...',
  ENDPOINT 's3.example.com', URL_STYLE 'path'
);
CREATE VIEW traces AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/traces/*/*/*/*/*/*.parquet');
CREATE VIEW logs AS SELECT * FROM
  read_parquet('s3://YOUR-BUCKET/telemetry/logs/*/*/*/*/*/*.parquet');

-- The slowest requests.
SELECT name, duration_us, trace_id FROM traces
  ORDER BY duration_us DESC LIMIT 20;

-- Every log line, inside the span that wrote it.
SELECT l.body, t.name, t.duration_us FROM logs l
  JOIN traces t ON l.trace_id = t.trace_id AND l.span_id = t.span_id;
```

An S3-compatible endpoint such as minio needs `URL_STYLE 'path'` in
the secret, and a plain-HTTP endpoint also needs `USE_SSL false`. AWS
itself does not need either.

The files are partitioned by node and by hour:
`telemetry/traces/<node>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet`. A query
that reads one day therefore touches only that day's files.

## File size and compaction

celld writes one Parquet file for each flush, on a time limit or a
size target, whichever it reaches first. The default interval is 5 minutes
(`CELLD_OTEL_FLUSH_MS=300000`). The default size target is 5 MiB of estimated
buffered events (`CELLD_OTEL_FLUSH_BYTES=5242880`). The event that reaches
the target can take the batch past it.

Keep the defaults if you run no compaction job. They make files large
enough for a fast query with no other moving part. The default batching
delay can reach 5 minutes, so the default suits an investigation after the fact.

Set `CELLD_OTEL_FLUSH_MS=5000` for a five-second batching interval.
An upload, a retry, or collector processing can add a delivery delay.
A short flush makes many small files, and DuckDB
opens every file a query reads, so you must also run the compaction
job below. Turn on the compaction job first, then shorten the flush,
or queries grow slow within hours.

The `otlp` sink is the other route to a near-live view. It sends each
batch to a collector, so set the same short `CELLD_OTEL_FLUSH_MS`.

The `otlp` sink makes no more than five attempts for a batch when a
transient failure occurs. It uses exponential backoff with jitter, and it
uses an applicable `Retry-After` value from the collector. The exporter caps
each delay at 30 seconds, so a collector cannot suspend a batch indefinitely.
HTTP 408, 429, 502, 503, and 504 responses are transient failures. A
permanent refusal drops the batch immediately.

The exporter owns one retrying batch, and the input channel holds 8192
new events. An outage cannot make either bound grow. celld drops and counts
new telemetry when the channel is full, so request handling continues.

The retention sweep runs at startup and six hours after each completed sweep. It deletes expired
objects and preserves objects within the configured retention window.

## Compaction

Run a compaction job on a maintenance node, not on a celld node. celld
does not compact its own files, because the serving path must not do
storage maintenance and each node writes only the files it produced.

The job rewrites one past hour of small files into one large file.
DuckDB does the work:

```sql
COPY (
  SELECT * FROM
    read_parquet('s3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/*.parquet')
  ORDER BY start_unix_us
) TO 's3://YOUR-BUCKET/telemetry/traces/<node>/2026/08/09/22/compacted.parquet'
  (FORMAT parquet, COMPRESSION zstd);
```

Run the job once an hour, for the hour that just ended. Do not compact
the current hour, because a node still writes to it. Delete the source
files after DuckDB writes the compacted file. The compacted file is
also smaller, because zstd compresses one sorted batch better than
many separate files.
