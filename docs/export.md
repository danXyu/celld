# Change export

celld can export every committed change to the SQLite databases of its
cells as a stream of records, for a warehouse such as Snowflake to load.
The [design](design/change-export.md) describes the record format, the
capture, and the delivery guarantees.

The feature is under construction. A node with `CELLD_EXPORT=1` exports
the row changes of its root cells through the bucket sink: Parquet objects
under `export/changes/<node>/` in the bucket, released only after the
change is durable and the node still owns the cell, and followed by
watermarks that certify what the bucket holds. Facets, activation links,
repair and the blob-stream sink are not built yet. A node with
`CELLD_EXPORT_SINK=blob-stream` refuses
to start.

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
| `CELLD_EXPORT_WRITER_ID` | the node's zone | The blob-stream writer id. |
| `CELLD_EXPORT_RETRY_MS` | `30000` | The blob-stream retry deadline before a record counts as dropped. |
| `CELLD_EXPORT_RECONCILE` | `24h` | The reconciler interval, as `<n>s`, `<n>m`, `<n>h`, or `<n>d`. The loader deployment runs the reconciler, not the node. |

The bucket sink requires the node to have a fleet bucket (`CELLD_BUCKET`),
including when `CELLD_EXPORT_BUCKET` names another bucket, because that
bucket uses the fleet bucket's endpoint and credentials.

Queue brokers (`__Queue`), Workflow instances (`__Workflow` and every
`__Workflow.<script>` class), and cron cells (`.cron`) are never exported.
celld refuses to start when `CELLD_EXPORT_CLASSES` names one of them.

## Schema changes

Every exported table has a generation, and rows of different generations
never merge. celld compares each cell's schema with what it last exported
at the same safe point it pulls row changes. A create opens generation
one. A drop closes the generation. An alteration, a rename, or a drop and
recreate under the same name opens the next generation, and so does a
table that changed while export was off for its cell. Each change is a
`schema` record at the commit that made it, and every generation that
opens is snapshotted inline at that commit, or exported as `bulk` when it
is larger than `CELLD_EXPORT_MAX_TX_BYTES`.

Generations are stored in the cell itself, in the `_cf_EXPORT` table, so
they survive restarts, moves and restores, and `deleteAll()` keeps them: a
table created after it continues from its old generation, so old rows
cannot come back. A cell's first export starts every table it already has
at generation one with no snapshot, like any change that happened before
export was on; the planned `celld export backfill` covers those.

## Key-value tables

The Durable Object key-value API (`ctx.storage.get`, `put`, `kv`) keeps its
values in `_cf_KV`. The export carries that table as `kv`, with columns
`key` and `value` and the key alone as its primary key. `value` is JSON
text: V8's own deserializer reads the stored bytes, and the types JSON
lacks come out as one-key objects, for example `{"$bigint": "12"}`,
`{"$date": "2026-01-01T00:00:00.000Z"}`, `{"$map": [[key, value]]}`,
`{"$undefined": true}` or `{"$bytes": {"base64": …, "type": "Uint8Array"}}`.
A stored object with a key that starts with `$` comes out wrapped as
`{"$object": {…}}`, so every such key in the JSON is one of these tags. The
full list is in `crates/celld/export_kv.rs`. A value that does not decode,
such as one that refers to itself, one stored in more than 2 MiB, or one
whose JSON would grow far past its stored size (a large sparse array), is
exported as its stored bytes, a `{"$blob": …}` in the record.
`CELLD_EXPORT_TABLES` names the table as `Class.kv`.

A KV namespace's `__kv` table keeps its columns and gains `blob_key`: the
bucket object that holds a value too large to store inline, such as
`kv/blobs-v2/<cell>/e<epoch>/<digest>`, or `NULL`. The export does not copy
the blob.

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
