# Change export

celld can export every committed change to the SQLite databases of its
cells as a stream of records, for a warehouse such as Snowflake to load.
The [design](design/change-export.md) describes the record format, the
capture, and the delivery guarantees.

The feature is under construction. A node with `CELLD_EXPORT=1` exports
the row changes of its root cells through the bucket sink: Parquet objects
under `export/changes/<node>/` in the bucket, released only after the
change is durable and the node still owns the cell, and followed by
watermarks that certify what the bucket holds. Facets, schema records,
activation links, the `kv` mapping of `_cf_KV` and repair are not built
yet.

The blob-stream sink sends each record to a
[blob-stream](https://github.com/bitdriftlabs/blob-stream) topic instead. Its
client is behind the `export-blob-stream` Cargo feature, which a default
build leaves out; a node without it refuses to start with
`CELLD_EXPORT_SINK=blob-stream`. Build with
`cargo build --features export-blob-stream` (the client's protobuf code
generation needs `protoc` on the build machine). A node exports through one
sink at a time: `bucket,blob-stream` refuses to start.

blob-stream partitions a topic by writer, one writer per zone of the broker
deployment. For a topic with several writers, list its zones in writer order
in `CELLD_EXPORT_ZONES` and give each node its zone in `CELLD_ZONE`; the
node produces as the writer at its zone's position. A single-writer topic
needs neither.

Export is off by default, and the off state costs nothing: with
`CELLD_EXPORT` unset or `0`, celld opens no capture session, holds no
export buffer, and starts no export task. celld still checks the values of
the other `CELLD_EXPORT_*` variables, so a malformed value fails the boot
that carries it, not the later one that turns export on. The rules that
relate one variable to another apply only with `CELLD_EXPORT=1`.

## Configuration

| variable | default | effect |
| --- | --- | --- |
| `CELLD_EXPORT` | `0` | `0` disables export. `1` enables it. |
| `CELLD_EXPORT_SINK` | `bucket` | `bucket`, `blob-stream`, or both as `bucket,blob-stream`. |
| `CELLD_EXPORT_BUCKET` | the fleet bucket | A different bucket for the bucket sink, on the same endpoint and credentials. |
| `CELLD_EXPORT_CLASSES` | application classes, `__D1Database`, `__KvNamespace` | A comma-separated allow list of Durable Object classes. Facets follow their root's class. |
| `CELLD_EXPORT_TABLES` | unset | A comma-separated deny list of `Class.table` entries. |
| `CELLD_EXPORT_MAX_TX_BYTES` | `4194304` | The session memory above which a transaction is exported as `bulk`. It is also the largest table celld snapshots inline after a schema change. |
| `CELLD_EXPORT_MAX_RECORD_BYTES` | `1048576` | The fragment size. It must not exceed `CELLD_EXPORT_QUEUE_BYTES`. |
| `CELLD_EXPORT_QUEUE_BYTES` | `268435456` | The shared budget for pending commits and the node buffer. |
| `CELLD_EXPORT_FLUSH_MS` | `10000` | The bucket sink flush interval and watermark cadence. |
| `CELLD_EXPORT_FLUSH_BYTES` | `8388608` | The buffered bytes that trigger an early bucket sink flush. |
| `CELLD_EXPORT_RETENTION` | `none` | `<n>d` makes the bucket sink delete its files after `n` days. `none` leaves the lifecycle to the consumer. |
| `CELLD_EXPORT_TOPIC` | `celld-changes` | The blob-stream topic. |
| `CELLD_EXPORT_BROKERS` | unset | Comma-separated `host:port` brokers, or `k8s://NAMESPACE/SERVICE`. Required when the blob-stream sink is on. |
| `CELLD_EXPORT_PARTITIONS` | unset | The topic's partition count, which every producer and consumer of the topic must agree on. Required when the blob-stream sink is on. |
| `CELLD_EXPORT_ZONES` | unset | The topic's writer zones, comma-separated in the broker deployment's writer order: a zone's writer number is its position, from 0. Every node must list them alike. Unset means a single-writer topic. |
| `CELLD_EXPORT_WRITER_ID` | the node's zone (`CELLD_ZONE`) | The zone whose writer this node produces as. It must be one of `CELLD_EXPORT_ZONES`. |
| `CELLD_EXPORT_RETRY_MS` | `30000` | The blob-stream retry deadline before a record counts as dropped. |
| `CELLD_EXPORT_RECONCILE` | `24h` | The reconciler interval, as `<n>s`, `<n>m`, `<n>h`, or `<n>d`. The loader deployment runs the reconciler, not the node. |

The bucket sink requires the node to have a fleet bucket (`CELLD_BUCKET`),
including when `CELLD_EXPORT_BUCKET` names another bucket, because that
bucket uses the fleet bucket's endpoint and credentials.

Queue brokers (`__Queue`), Workflow instances (`__Workflow` and every
`__Workflow.<script>` class), and cron cells (`.cron`) are never exported.
celld refuses to start when `CELLD_EXPORT_CLASSES` names one of them.

## Metrics

With export on, the node reports these gauges from its `/state` snapshot.
A node with export off reports none of them.

| gauge | meaning |
| --- | --- |
| `celld.export.queue_bytes` | Encoded bytes held against `CELLD_EXPORT_QUEUE_BYTES`. |
| `celld.export.pending_commits` | Captured commits that wait for a durability proof. |
| `celld.export.dropped_records` | Records dropped over the budget or after the retry deadline since the process started. |
| `celld.export.gaps` | Gap notes emitted since the process started. |
| `celld.export.bulk_commits` | Commits exported as `bulk` since the process started. |
| `celld.export.attribution_mismatches` | Commits the capture could not attribute since the process started. |
