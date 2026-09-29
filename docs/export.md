# Change export

Change export streams every committed change to the SQLite databases of
your cells out of celld, as records a warehouse such as Snowflake can load.
The warehouse ends up holding a copy of each exported table of each cell,
kept current as the cells change, with a way to tell which parts of that
copy are certified complete and a way to repair the parts that are not.

This guide covers what the export promises, how to turn it on, where the
records go, how to load them into Snowflake, and the `celld export`
commands that keep the copy complete. The
[design](design/change-export.md) goes deeper into the capture, the record
format and the delivery guarantees.

## Status

Change export is on `main` and is not in a fork release yet. What works
today:

- Row changes, schema changes and key-value data of root cells and their
  facets, exported through the **bucket sink** as Parquet objects in the
  fleet bucket. Each facet exports on a stream of its own.
- The **blob-stream sink**, in builds with the `export-blob-stream` Cargo
  feature.
- Completeness records: watermarks, activation links, facet deletes, and
  `recovered` records from dead-node recovery.
- The `celld export` commands: `repair`, `backfill`, `inspect`,
  `reconcile`, `verify`, and `erase`.
- The Snowflake tables, views, Dynamic Tables and the `celld-export-loader`
  binary, which consumes the blob-stream topic and lands its records in
  Snowflake through Snowpipe Streaming.

Still to come:

- **Repairing facet streams.** `repair` and `backfill` skip facet streams;
  a gap in one is reported but cannot be filled yet.
- **Loading the bucket sink into Snowflake.** The loader reads blob-stream
  only. A fleet on the bucket sink can pipe `celld export inspect` into
  `celld-export-loader ingest` by hand, but nothing loads it continuously.
- **Auditing against Snowflake.** `reconcile` and `verify` compare the
  bucket with the bucket sink's records, not with the Snowflake tables, so
  a blob-stream fleet cannot run them yet.
- **Both sinks at once.** A node exports through one sink:
  `CELLD_EXPORT_SINK=bucket,blob-stream` refuses to start.
- **A real Snowflake account.** The SQL and the Snowpipe Streaming client
  are tested against an emulator. The steps to check them on a real
  account are in
  [the loader's README](../crates/export-snowflake/README.md#verifying-on-a-real-account).

## What the export promises

The export is a **convergent mirror of current state**, not a transaction
log.

- For every exported table of every exported cell, a consumer that has
  applied the stream holds the rows the cell holds, and catches up with the
  cell's head shortly after the cell goes quiet.
- Every state a consumer passes through is a real committed state of the
  cell, though not necessarily every one of them: several transactions can
  arrive as one net change.
- A change leaves the node only after the write that made it is durable
  and the node still owns the cell. The export never shows a state celld
  could lose, and a fenced node exports nothing.
- Records can be lost after they leave the node and before a sink stores
  them. Every such loss is detected, at the latest by the next
  reconciliation, and repair replaces the affected cell's state with an
  authoritative snapshot. A consumer can always tell certified state from
  provisional state.
- Export is off by default and costs nothing when off.

It does not promise every intermediate transaction, the before-image of
every update, exactly-once delivery, or a Kafka-compatible stream. A
consumer drops duplicates by key and orders by position, never by arrival.

## Concepts

**Stream.** Every record belongs to one stream: the state of one cell. A
stream is named by `script`, `class`, `cell` (the cell scope, `Class:id`),
`facet` (the facet path, absent for a root cell), and `incarnation`. A root
cell's incarnation is its first epoch. A facet's incarnation is stamped when
the facet is created, so a facet deleted and recreated at the same path is
a different stream.

**Position.** `(epoch, txid, commit)`: the cell epoch, the LTX transaction
that holds the commit's last WAL frame, and the commit's sequence in the
epoch. Positions order a stream totally.

**Table generation.** `(table, generation)`. A table's generation starts at
one and moves on with every schema change that touches it. Rows of
different generations never merge, so a dropped and recreated table cannot
bring old rows back.

**Row key.** The table's declared primary key, in declared order, or the
rowid (`_rowid_`) for a table without one.

**Certified.** Each node follows its records with `watermark` records that
count what it delivered between two positions. A consumer that holds
exactly those counts can certify the range. What lies past the certified
position is provisional.

**Gap.** A range of a stream the consumer may be missing. Gaps show up as
`gap` records, as `link` or `recovered` records that point past what was
certified, as `bulk` records, and as reconciler findings. Repair closes a
gap by writing a snapshot of the stream that replaces the consumer's copy.

## Quick start

### Try it locally

`celld dev` exports like a fleet node, into the development object store:

```sh
CELLD_EXPORT=1 CELLD_EXPORT_FLUSH_MS=1000 celld dev
```

Write to a Durable Object, and within a flush interval Parquet objects
appear under `export/changes/` in `.celld/dev/objects.sqlite3`.

### Turn it on in a fleet

1. Pick the classes to export. Unset, `CELLD_EXPORT_CLASSES` exports every
   application class plus D1 databases and KV namespaces.
2. Give the nodes a fleet bucket (`CELLD_BUCKET`), which they already have
   in a fleet, and optionally a separate export bucket on the same endpoint:

   ```sh
   CELLD_EXPORT=1
   CELLD_EXPORT_CLASSES=Cart,Order
   CELLD_EXPORT_BUCKET=acme-celld-export   # optional
   ```

3. Roll the nodes. Each node starts exporting the cells it activates from
   then on. Changes made before export was on are not exported; backfill
   them (step 6).
4. Point a consumer at the export. For Snowflake, export through the
   [blob-stream sink](#blob-stream-sink) and run the loader
   ([Loading into Snowflake](#loading-into-snowflake)).
5. Watch the [metrics](#metrics), especially `celld.export.gaps` and
   `celld.export.dropped_records`.
6. Backfill the cells that existed before export was on:

   ```sh
   celld export backfill --class Cart
   celld export backfill --class Order
   ```

7. Schedule the reconciler and a sample verify
   ([Keeping the copy complete](#keeping-the-copy-complete)).

## Configuration

Export is configured with environment variables on each node. With
`CELLD_EXPORT` unset or `0`, celld opens no capture session, holds no export
buffer, and starts no export task. It still checks the values of the other
`CELLD_EXPORT_*` variables, so a malformed value fails the boot that
carries it, not the later one that turns export on. The rules that relate
one variable to another apply only with `CELLD_EXPORT=1`, so you can stage
the settings with export still off.

| variable | default | effect |
| --- | --- | --- |
| `CELLD_EXPORT` | `0` | `0` disables export. `1` enables it. |
| `CELLD_EXPORT_SINK` | `bucket` | `bucket` or `blob-stream`. |
| `CELLD_EXPORT_BUCKET` | the fleet bucket | A different bucket for the bucket sink, on the same endpoint and credentials. |
| `CELLD_EXPORT_CLASSES` | application classes, `__D1Database`, `__KvNamespace` | A comma-separated allow list of Durable Object classes. Facets follow their root's class. |
| `CELLD_EXPORT_TABLES` | unset | A comma-separated deny list of `Class.table` entries. |
| `CELLD_EXPORT_MAX_TX_BYTES` | `4194304` | The capture memory above which a transaction is exported as `bulk`. It is also the largest table celld snapshots inline after a schema change. |
| `CELLD_EXPORT_MAX_RECORD_BYTES` | `1048576` | The fragment size. It must not exceed `CELLD_EXPORT_QUEUE_BYTES`. |
| `CELLD_EXPORT_QUEUE_BYTES` | `268435456` | The shared memory budget for commits waiting on durability and records waiting on the sink. |
| `CELLD_EXPORT_FLUSH_MS` | `10000` | The bucket sink flush interval and watermark cadence. |
| `CELLD_EXPORT_FLUSH_BYTES` | `8388608` | The buffered bytes that trigger an early bucket sink flush. |
| `CELLD_EXPORT_RETENTION` | `none` | `<n>d` makes the bucket sink delete its objects after `n` days. `none` leaves the lifecycle to you. |
| `CELLD_EXPORT_TOPIC` | `celld-changes` | The blob-stream topic. |
| `CELLD_EXPORT_BROKERS` | unset | Comma-separated `NODE_ID=host:port` brokers, or `k8s://NAMESPACE/SERVICE`. A static broker's `NODE_ID` must be the node ID the broker itself is configured with (its `node_identity`), because the producer assigns partitions by node ID. Required with the blob-stream sink. |
| `CELLD_EXPORT_PARTITIONS` | unset | The topic's partition count, which every producer and consumer of the topic must agree on. Required with the blob-stream sink. |
| `CELLD_EXPORT_ZONES` | unset | The topic's writer zones, comma-separated in the broker deployment's writer order: a zone's writer number is its position, from 0. Every node must list them alike. Unset means a single-writer topic. |
| `CELLD_ZONE` | unset | The node's zone. With `CELLD_EXPORT_ZONES` set, it picks the writer this node produces as. |
| `CELLD_EXPORT_WRITER_ID` | the node's zone (`CELLD_ZONE`) | The zone whose writer this node produces as, when it differs from the node's zone. It must be one of `CELLD_EXPORT_ZONES`. |
| `CELLD_EXPORT_RETRY_MS` | `30000` | How long the blob-stream sink retries a record before it counts as dropped. |
| `CELLD_EXPORT_RECONCILE` | `24h` | The interval of `celld export reconcile --schedule`, as `<n>s`, `<n>m`, `<n>h`, or `<n>d`. The node itself does not reconcile. |

Zone names are 1 to 128 ASCII letters, digits, `.`, `-` or `_`.

The bucket sink needs the node's fleet bucket (`CELLD_BUCKET`) even when
`CELLD_EXPORT_BUCKET` names another bucket, because the export bucket uses
the fleet bucket's endpoint and credentials.

Queue brokers (`__Queue`), Workflow instances (`__Workflow` and every
`__Workflow.<script>` class), and cron cells (`.cron`) are never exported.
celld refuses to start when `CELLD_EXPORT_CLASSES` names one of them.

## Sinks

### Bucket sink

The default sink writes Parquet objects to the export bucket at

```text
export/changes/<node>/<yyyy>/<mm>/<dd>/<hh>/<unix_us>-<rand>.parquet
```

One object holds many records, one row per record. The columns are the
envelope fields (`kind`, `script`, `class`, `cell`, `cell_name`, `facet`,
`incarnation`, `epoch`, `txid`, `commit`, `committed_at`, `node`, `origin`,
`fragment`, `fragments`) and `body`, the rest of the record as a JSON
string. The `cell` column has a bloom filter, so a reader looking for one
cell can skip most objects.

A node flushes every `CELLD_EXPORT_FLUSH_MS`, or earlier once
`CELLD_EXPORT_FLUSH_BYTES` are buffered. A record counts as delivered only
once the object holding it is written. A retried write can land a record
twice; consumers drop duplicates.

With `CELLD_EXPORT_RETENTION=<n>d` the node deletes its export objects
after `n` days. Leave it at `none` unless your consumer is sure to load
objects well within that window: the bucket may be the only copy of a
record the consumer has not loaded yet. Snapshots written by `repair` and
`backfill` with the bucket sink also land under `export/changes/`.

### blob-stream sink

The blob-stream sink sends each record to a
[blob-stream](https://github.com/bitdriftlabs/blob-stream) topic, keyed by
stream so one producer keeps a stream's records in order. A record counts
as delivered once the broker has stored it durably.

Its client is behind the `export-blob-stream` Cargo feature, which a
default build, and the fork's release artifacts, leave out. A node without
it refuses to start with `CELLD_EXPORT_SINK=blob-stream`. Build it with

```sh
cargo build --release --features export-blob-stream
```

The client's protobuf code generation needs `protoc` with the well-known
types on the build machine, and a `--locked` build fetches the pinned git
dependencies from github.com.

```sh
CELLD_EXPORT=1
CELLD_EXPORT_SINK=blob-stream
CELLD_EXPORT_BROKERS=k8s://blob-stream/brokers
CELLD_EXPORT_PARTITIONS=64
CELLD_EXPORT_ZONES=us-east-1a,us-east-1b,us-east-1c
CELLD_ZONE=us-east-1b   # this node produces as writer 1
```

blob-stream partitions a topic by writer, one writer per zone of the broker
deployment. For a topic with several writers, list its zones in writer
order in `CELLD_EXPORT_ZONES` on every node and give each node its zone in
`CELLD_ZONE`. A single-writer topic needs neither. A stream that moves to a
node in another zone continues in that zone's partitions, so consumers
order by position, not by partition offset.

A record the brokers do not accept within `CELLD_EXPORT_RETRY_MS` is
dropped, counted in `celld.export.dropped_records`, and reported as a gap.

## Records

Every record is one JSON object: an envelope and a body that depends on
`kind`. The bucket sink stores the envelope as columns and the body as the
`body` column; blob-stream carries the whole object.

### Envelope

| field | meaning |
| --- | --- |
| `kind` | `rows`, `snapshot`, `snapshot_end`, `schema`, `link`, `recovered`, `deleted`, `watermark`, `bulk`, or `gap` |
| `script`, `class`, `cell`, `facet`, `incarnation` | the stream |
| `cell_name` | the Durable Object's name when the cell has one; descriptive only |
| `epoch`, `txid`, `commit` | the position |
| `committed_at` | milliseconds since the Unix epoch, taken at the commit |
| `node` | the node that produced the record, or the tool that did (`export-cli`, `reconciler`) |
| `origin` | `live` for the running node, `snapshot` for inline snapshots, `repair` for `repair` and `backfill` |
| `fragment`, `fragments` | `i` of `k` for a record split under `CELLD_EXPORT_MAX_RECORD_BYTES`; `1` of `1` otherwise |

### Kinds

| kind | adds | meaning |
| --- | --- | --- |
| `rows` | `table`, `generation`, `columns`, `key_columns`, `rows` | The row changes of one table in one commit. A commit that touched several tables has one `rows` record per table at the same position. |
| `snapshot` | the `rows` fields and `snapshot_id` | A table's rows at a position, every `op` `I`. |
| `snapshot_end` | `snapshot_id`, `scope`, `tables`, `records` | Closes a snapshot. `scope` is `stream` (the snapshot replaces the whole stream, so a table generation it does not list is emptied) or `tables` (it replaces only the listed generations). |
| `schema` | `table`, `generation`, `sql`, `columns`, and `dropped`, `renamed_from` or `unsupported` | A table generation's definition. `dropped` closes the generation. `unsupported` marks a virtual table, which is not exported. |
| `link` | `start_txid`, `prev_epoch`, `prev_txid`, `mode` | Emitted when a cell activates, before it serves. `mode` is `fresh` (nothing restored), `clone` (a whole image restored), `paged` (the previous epoch's chain paged in) or `resume` (the same epoch reopened after a clean reload). |
| `recovered` | `session`, `head`, `loss`, `cells` | Emitted by dead-node recovery for a cell epoch it saved to the bucket. See [Dead-node recovery](#dead-node-recovery). |
| `deleted` | `target_facet`, `target_incarnation`, `subtree`, `through_incarnation` | A stream no longer exists. Without a target it names its own stream; a facet delete names the facet path on the root's stream. |
| `watermark` | `from`, `through`, `commits`, `records` | Certifies `(from, through]`: the number of distinct commit positions and of live records other than watermarks in that range. |
| `bulk` | `tables` | These table generations changed in a way the stream does not carry, such as a transaction over `CELLD_EXPORT_MAX_TX_BYTES`. The consumer's copy of them is unknown until a snapshot covers them. |
| `gap` | `from`, `to`, `reason` | The stream may be missing changes between these positions. |

A row change is `[op, key, row]`. `op` is `I`, `U` or `D`; `key` holds the
values of `key_columns`; `row` holds the values of `columns`: the full
after-image for `I` and `U`, the full before-image for `D`.

```json
{"kind":"rows","script":"shop","class":"Cart","cell":"Cart:5f0c…","cell_name":"alice",
 "incarnation":3,"epoch":3,"txid":42,"commit":7,"committed_at":1790000000000,
 "node":"node-a","origin":"live","fragment":1,"fragments":1,
 "table":"items","generation":1,"columns":["sku","qty"],"key_columns":["sku"],
 "rows":[["U",["A-100"],["A-100",2]],["D",["B-200"],["B-200",1]]]}
```

### Values

Integers are JSON integers, reals are JSON numbers, text is a string, `NULL`
is `null`, and a blob is `{"$blob": "<base64>"}`. An infinite real is
`{"$real": "inf"}` or `{"$real": "-inf"}`.

### Applying records

A consumer applies a stream's records in position order and follows these
rules. The reference consumer in
[`crates/export-format`](../crates/export-format/src/consumer.rs) and the
Snowflake views implement them.

- **Duplicates.** Drop a record already seen: the same stream, position,
  kind, origin, table generation and fragment, and for a snapshot the same
  `snapshot_id`. Per row, the key is
  `(stream, position, table, generation, key, fragment)`.
- **Fragments.** Use a fragmented record only once all its fragments are
  present.
- **Newest wins.** For each `(stream, table, generation, key)`, keep the
  change with the highest position. At an equal position, `repair` beats
  `snapshot`, which beats `live`. Drop keys whose newest change is `D`.
- **Snapshots replace.** A complete snapshot (every `snapshot` record and its
  `snapshot_end`) at position `P` replaces everything in its scope at or
  below `P`, including rows the snapshot does not contain.
- **Generations.** A `schema` record with `dropped` removes the
  generation's rows. Rows of different generations never merge.
- **Deletes.** A `deleted` record removes its stream at its position, or,
  for a facet delete, every stream at or below the facet path whose
  incarnation is at or below `through_incarnation`.

## What gets exported

### Tables

Every ordinary table of an exported cell, whatever its key: tables with a
declared primary key, rowid tables, and `WITHOUT ROWID` tables. D1
databases need nothing special. celld's own tables (`_cf_*`, apart from
`_cf_KV` below) and SQLite's (`sqlite_*`) are left out, and so are the
tables listed in `CELLD_EXPORT_TABLES`.

### Schema changes

celld compares each cell's schema with what it last exported at the same
point it reads row changes. A create opens generation one. A drop closes
the generation. An alteration, a rename, or a drop and recreate under the
same name opens the next generation, and so does a table that changed while
export was off for its cell. Each change is a `schema` record at the commit
that made it, and every generation that opens is snapshotted inline at that
commit, or exported as `bulk` when the table is larger than
`CELLD_EXPORT_MAX_TX_BYTES`.

Generations are stored in the cell itself, in the `_cf_EXPORT` table, so
they survive restarts, moves and restores, and `deleteAll()` keeps them: a
table created after it continues from its old generation. A cell's first
export starts every table it already has at generation one with no
snapshot, like any change that happened before export was on;
`celld export backfill` provides those snapshots.

### Key-value storage

The Durable Object key-value API (`ctx.storage.get`, `put`, `kv`) keeps its
values in `_cf_KV`. The export carries that table as `kv`, with columns
`key` and `value` and the key alone as its primary key. `value` is JSON
text: V8's own deserializer reads the stored bytes, and the types JSON
lacks come out as one-key objects, for example `{"$bigint": "12"}`,
`{"$date": "2026-01-01T00:00:00.000Z"}`, `{"$map": [[key, value]]}`,
`{"$undefined": true}` or `{"$bytes": {"base64": …, "type": "Uint8Array"}}`.
A stored object with a key that starts with `$` comes out wrapped as
`{"$object": {…}}`, so every such key in the JSON is one of these tags. The
full list is in [`export_kv.rs`](../crates/celld/export_kv.rs). A value that
does not decode, such as one that refers to itself, one stored in more than
2 MiB, or one whose JSON would grow far past its stored size (a large
sparse array), is exported as its stored bytes, a `{"$blob": …}` in the
record. `CELLD_EXPORT_TABLES` names the table as `Class.kv`.

An application SQL table literally named `kv` exports as `_cf_SQL_kv`,
so it cannot collide with the storage API table. Its deny rule is
`Class._cf_SQL_kv`; denying `Class.kv` affects only the storage API table.
If a previous exporter already published a SQL `kv` table, run a repair
snapshot after upgrading to replace the old ambiguous table identity.

A KV namespace's `__kv` table keeps its columns and gains `blob_key`: the
bucket object that holds a value too large to store inline, such as
`kv/blobs-v2/<cell>/e<epoch>/<digest>`, or `NULL`. The export does not copy
the blob.

### Facets

Each facet exports on a stream of its own, named by its root's class and
cell plus the facet path, with the facet's incarnation. The stream starts
with a `link` record like a root's, and its commits are released only after
the root's node proves it still owns the root cell. A nested facet gets its
own stream at its full path.

Deleting a facet ends its stream: the root's stream carries a `deleted` record with
`subtree` set, which removes the facet and every facet below it, including
facets created before the delete that were never resident on the node. A
facet recreated at the same path afterwards has a higher incarnation and is
not affected.

### Not exported

- Queue message bodies, Workflow state, cron cells and R2 objects, by
  policy.
- Virtual tables, including `sqlite-vec` indexes. Their shadow tables are
  filtered, and a `schema` record marks the virtual table `unsupported`.
- Rows whose declared primary key contains `NULL`, which SQLite's session
  extension does not record. `verify` reports drift for such rows.
- History from before export was turned on for a cell. Backfill gives the
  current state, not the past transactions.

## Loading into Snowflake

The [`celld-export-snowflake`](../crates/export-snowflake/README.md) crate
holds the Snowflake objects and `celld-export-loader`, which deploys them,
feeds them from the blob-stream topic, and keeps them in step. Records flow
like this:

1. Nodes export through the [blob-stream sink](#blob-stream-sink).
2. The loader reads the topic as a member of the consumer group `snowflake`
   and appends records in batches to the pipe `EXPORT_LANDING_PIPE` through
   [Snowpipe Streaming](https://docs.snowflake.com/user-guide/snowpipe-streaming/data-load-snowpipe-streaming-overview),
   which lands them in `EXPORT_LANDING`. It commits its topic offsets only
   once Snowflake has acknowledged every row of a batch, so a restart
   replays at most the batches in flight, and duplicates are dropped
   downstream.
3. A task running every minute routes landed rows: `rows` and `snapshot`
   into `CELL_CHANGES`, everything else into `CELL_META`, dropping
   tombstoned streams.
4. Views derive current state from those two tables, following the rules
   in [Applying records](#applying-records).
5. One Dynamic Table per `(script, class, table)` holds the current rows of
   that table across every cell of the class, with typed columns.

Snowpipe Streaming is billed per GB ingested, with no warehouse, so
loading costs about the same as Snowpipe. The warehouse runs the route
task, the erase task and the Dynamic Tables' refreshes.

### Build and deploy

The loader is not in the release artifacts. Build it from the repository
with both features; like the sink, the consumer needs `protoc`:

```sh
cargo build --release -p celld-export-snowflake --features sql-api,blob-stream --bin celld-export-loader
```

Prepare Snowflake as an administrator: a database, a warehouse, and a role
with `USAGE` on both, `CREATE SCHEMA` on the database and `EXECUTE TASK` on
the account. Give the loader a user with that role and an RSA key pair. The
[loader README](../crates/export-snowflake/README.md#verifying-on-a-real-account)
has the exact statements.

The loader reads blob-stream's own storage, S3 and DynamoDB, and asks the
brokers for recent data, so it needs the consumer settings blob-stream's
brokers use: a YAML or JSON file in the form of
[`examples/consumer.yaml`](../crates/export-snowflake/examples/consumer.yaml),
with the topic, blob store, metadata store and broker discovery your
brokers use, and AWS credentials that can read them.

Then configure and run:

```sh
export SNOWFLAKE_ACCOUNT=myorg-myaccount
export SNOWFLAKE_USER=celld_loader
export SNOWFLAKE_PRIVATE_KEY_FILE=rsa_key.p8
export SNOWFLAKE_DATABASE=CELLD
export SNOWFLAKE_SCHEMA=EXPORT
export SNOWFLAKE_WAREHOUSE=CELLD_EXPORT_WH
export EXPORT_BLOB_STREAM_CONFIG=consumer.yaml
export EXPORT_MEMBER_ID=loader-0

celld-export-loader run 60
```

`run` deploys every missing object, resumes the tasks, then consumes the
topic until it gets `SIGINT` or `SIGTERM`, syncing the Dynamic Tables every
60 seconds. On a stop it lands what it has read and gives up its
partitions. Several loaders with different member ids share the topic's
partitions between them.

| setting | required | meaning |
| --- | --- | --- |
| `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER` | yes | The account and the loader's user. |
| `SNOWFLAKE_PRIVATE_KEY_FILE` | yes | A PKCS#8 or PKCS#1 PEM key. An encrypted PKCS#8 key takes `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE`. |
| `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA` | yes | Where the objects live. |
| `SNOWFLAKE_WAREHOUSE` | yes | Runs the loader's statements, the tasks and the Dynamic Tables. |
| `SNOWFLAKE_ROLE`, `SNOWFLAKE_URL` | no | A role other than the user's default; an API URL other than the account's. |
| `EXPORT_BLOB_STREAM_CONFIG` | `run` | The blob-stream consumer config, `.yaml`, `.yml` or `.json`. |
| `EXPORT_MEMBER_ID` | `run` | This loader's member id in the consumer group, stable across restarts. Default: `HOSTNAME`. |
| `EXPORT_GROUP` | no | The consumer group. Default `snowflake`, or the config file's. |
| `EXPORT_BATCH_RECORDS`, `EXPORT_BATCH_BYTES`, `EXPORT_BATCH_MS` | no | A batch lands at 10000 records, 8 MiB, or 5 seconds after its first record, whichever comes first. |
| `EXPORT_TARGET_LAG` | no | The Dynamic Tables' target lag. Default `1 minute`. |
| `EXPORT_DYNAMIC_TABLE_PREFIX` | no | The Dynamic Tables' name prefix. Default `CF`. |

A batch that fails to land is retried, with backoff, until it lands;
nothing more is read meanwhile. A message on the topic that is not a
record is skipped and logged; the record it should have been is then
missing from its stream's watermark counts, so the stream is not certified
past it and `EXPORT_GAPS` lists it for repair.

`deploy` creates objects `IF NOT EXISTS` and never changes one that exists.
When an upgrade changes a table, the pipe, the stream or a task, drop that
object and deploy again. Upgrading from a loader that read the bucket
sink's files: drop `EXPORT_PIPE`, `EXPORT_STAGE` and `EXPORT_PARQUET`, and
rename the `FILE_NAME` column of `EXPORT_LANDING`, `CELL_CHANGES` and
`CELL_META` to `SOURCE` (or drop and recreate the three tables and reload).

### Loader commands

| command | what it does |
| --- | --- |
| `deploy` | Create what is missing, resume the tasks, and sync the Dynamic Tables. |
| `sync` | Create or replace each Dynamic Table whose schema changed. Replacing one restarts it with a full refresh. |
| `run [SECONDS]` | `deploy`, then land the blob-stream topic through Snowpipe Streaming and `sync` every SECONDS (default 60) until stopped. Needs the `blob-stream` feature. |
| `ingest FILE` | Land the records in FILE, JSON lines as `celld export inspect` prints them (`-` for stdin), through Snowpipe Streaming, and route them now. A line that is not a record fails the command after the rest land. |
| `erase SCRIPT CLASS CELL [--facet P] [--incarnation N] [--reason R]` | Tombstone a stream in Snowflake and delete its rows. |
| `query SQL [BIND...]` | Run a statement with each `?` bound to a JSON value, and print the rows. |
| `gaps`, `certified` | Print `EXPORT_GAPS` or `CELL_CERTIFIED`. |

### What you query

| object | holds |
| --- | --- |
| `CF_<SCRIPT>_<CLASS>_<TABLE>_<hash>` | The Dynamic Table of one table: its current rows across every cell of the class, one typed column per column any generation had, plus `_CF_SCRIPT`, `_CF_CLASS`, `_CF_CELL`, `_CF_FACET`, `_CF_INCARNATION`, `_CF_CELL_NAME`, `_CF_GENERATION`, the position, `_CF_COMMITTED_AT`, `_CF_ORIGIN`, `_CF_KEY`, and the row exactly as exported in `_CF_COLUMNS` and `_CF_ROW`. |
| `CELL_CHANGES`, `CELL_META` | Every routed record, append-only. |
| `CELL_CHANGES_CURRENT`, `CELL_META_CURRENT` | The same without duplicates and incomplete fragments. |
| `CELL_STREAMS` | Every stream seen, and whether it was deleted or erased. |
| `CELL_SNAPSHOTS` | The winning snapshot per stream and table generation. |
| `CELL_GENERATIONS` | Every table generation per stream, and whether it is closed. |
| `CELL_CERTIFIED` | Per stream and epoch, the position the watermarks certify. |
| `EXPORT_GAPS` | What needs repair. See the next section. |
| `EXPORT_TOMBSTONES` | Erased streams. |

Columns are typed by SQLite's affinity rules for their declared type. A
column whose type differs between generations, a `NUMERIC` column, and a
column with no declared type are `VARIANT`. A value that does not fit its
column's type is `NULL` there and intact in `_CF_ROW`. A root cell's
`_CF_FACET` is `''`.

## Keeping the copy complete

Losing records between a node and a sink is expected in rare cases: a node
that dies with records not yet flushed, a broker outage longer than the
retry deadline, or a queue over budget. The export makes every such loss
visible, and repair fixes it.

How a loss becomes visible:

- **Watermarks** let the consumer certify ranges. Missing records leave the
  counts short, so the range stays uncertified.
- **Links.** Every activation starts its stream with a `link` naming the
  state it restored. A link past what the consumer certified in the
  previous epoch is a gap, so a restart or a move between nodes exposes
  what the old node did not deliver.
- **Recovered records** from dead-node recovery do the same for a cell that
  never activates again. See [Dead-node recovery](#dead-node-recovery).
- **The reconciler** compares every cell in the bucket with what the
  consumer certified, and catches whatever the first three missed.
- **`gap` records** from the node itself, when it drops records over the
  queue budget, after the blob-stream retry deadline, or when it cannot
  attribute a commit to its transaction.
- **`bulk` records** mark tables whose copy is unknown.

In Snowflake, every one of these ends up in `EXPORT_GAPS`, one row per
stream and finding. The repair loop is:

```sh
# Unload EXPORT_GAPS as JSON lines (drop the header line the loader prints).
celld-export-loader query \
  "SELECT TO_JSON(OBJECT_CONSTRUCT(*)) FROM EXPORT_GAPS" | tail -n +2 > gaps.jsonl

# Snapshot every stream it names, through the highest position its rows name.
# With CELLD_EXPORT_SINK=blob-stream and the nodes' blob-stream settings, the
# snapshots go to the topic, and the running loader lands them.
celld export repair --gaps gaps.jsonl
```

`repair` reads the rows' `script`, `class`, `cell`, `facet`, `incarnation`,
`gap_kind`, `bound_epoch` and `bound_txid`, in any letter case.

Repair does not recover the lost transactions. It restores the stream from
the bucket, read-only, and writes, through the sink `CELLD_EXPORT_SINK`
names, a `schema` record per table generation, a
`snapshot` of every exported table and a `snapshot_end`, all with
`origin: repair`. The snapshot replaces the consumer's copy of the stream,
including rows the consumer has and the cell no longer does.

### The celld export commands

`celld export` runs offline against the bucket from any machine with
access to it. Every subcommand takes the fleet flags (`--bucket` or
`CELLD_BUCKET`, `--endpoint` or `S3_ENDPOINT`, `--region` or `AWS_REGION`)
and `--export-bucket` (or `CELLD_EXPORT_BUCKET`) when the export writes
somewhere else. A scope is a cell scope as records carry it in `cell`,
`Class:id`. `CELLD_EXPORT_MAX_RECORD_BYTES` and `CELLD_EXPORT_TABLES` apply
to the snapshots these commands write, as on a node, and so do
`CELLD_EXPORT_SINK` and its settings: with `blob-stream`, `repair` and
`backfill` produce to the topic with `CELLD_EXPORT_BROKERS`,
`CELLD_EXPORT_PARTITIONS`, `CELLD_EXPORT_TOPIC` and the writer's zone
(`CELLD_EXPORT_WRITER_ID`, or `CELLD_ZONE` with `CELLD_EXPORT_ZONES`).

#### repair

```sh
celld export repair --stream SCOPE [--at EPOCH:TXID] [OPTIONS]
celld export repair --gaps FILE [--class CLASS] [OPTIONS]
```

Restores a root cell's stream from the bucket at the first position at or
after `--at`, or at the bucket's newest position without it, and writes a
snapshot there. The bucket only restores at the positions it holds cuts
for, so repair may land above `--at`. With `--gaps` it repairs every stream
of an `EXPORT_GAPS` unload, optionally of one class.

The bucket's newest position is not necessarily the fleet's: a node may
hold writes it has not uploaded yet. When the bucket does not reach the
target, repair snapshots what it holds and reports `covers_target: false`;
run it again later.

#### backfill

```sh
celld export backfill --class CLASS [--after SCOPE] [OPTIONS]
celld export backfill --gaps FILE [OPTIONS]
```

Snapshots streams at the bucket's newest position: every cell of a class,
or every stream an `EXPORT_GAPS` unload names. Use it after turning export
on for existing cells, after adding a class to `CELLD_EXPORT_CLASSES`, and
for the reconciler's `unknown_stream` findings. `--after` resumes a class
after a scope.

Options for `repair` and `backfill`:

| option | default | meaning |
| --- | --- | --- |
| `--script NAME` | the fleet's current deployment | The stream's script. Ignored with `--gaps`, whose rows name it. |
| `--node NAME` | `export-cli` | The `node` recorded on the snapshots, and their object prefix under `export/changes/`. |
| `--concurrency N` | `4` | Streams restored at once. |
| `--rate N` | `100` | Bucket reads per second across all streams; `0` for no limit. |
| `--dry-run` | | Print the streams and targets without restoring. |

Both print one JSON report per stream, with the position its snapshot
reached, and exit non-zero when any stream failed. Tombstoned streams are
skipped. Facet streams are skipped: restoring a facet's state is not built
yet.

#### inspect

```sh
celld export inspect [--node NODE] [--hour YYYY/MM/DD[/HH]] [FILTERS]
celld export inspect --file PATH [--file PATH]... [FILTERS]
```

Prints the records of bucket sink objects as JSON lines, from the bucket or
from downloaded files. `--node` and `--hour` narrow the listing (`--hour`
needs `--node`); `--after KEY` resumes after an object key and
`--objects N` reads N objects (default 100). Filters: `--cell SCOPE`,
`--kind KIND`, `--origin live|snapshot|repair`. `--summary` prints one line
per stream instead of every record.

```sh
celld export inspect --node node-a --hour 2026/09/29/14 --cell Cart:5f0c… --summary
```

#### reconcile

```sh
celld export reconcile [--settle DUR] [--dry-run] [--schedule] [--json]
```

Lists `cells/` and `log/`, derives each cell's head the way a restore does,
and compares it with what the consumer certified. It reports:

| finding | meaning |
| --- | --- |
| `gap` | The bucket holds changes the consumer has not certified. |
| `lost` | The consumer certified changes the cell no longer has: past the end of a closed epoch, in an epoch the chain skips, or past the head after a declared loss. |
| `missing_deleted` | A facet has no objects left while its root does, and the consumer still has it. |
| `unknown_stream` | An exported cell the consumer has never seen; backfill it. |
| `unrestorable` | A cell whose objects form no restorable chain. |

A difference counts only once the evidence it rests on is older than
`--settle` (default `1h`). For a gap that is when the first change the
consumer lacks reached the bucket, not the cell's latest write, so a busy
cell cannot defer an old gap. Findings go to `export/reconcile/<ms>.json`,
and `gap` and `deleted` records with `origin: repair` and `node: reconciler`
go to `export/changes/reconciler/`, where a consumer of the bucket sink
picks them up. To bring them into Snowflake, pipe them into the loader:
`celld export inspect --node reconciler | celld-export-loader ingest -`.
`--dry-run` writes nothing. `--schedule` runs forever, every
`CELLD_EXPORT_RECONCILE` (default `24h`).

The consumer `reconcile` and `verify` compare against today is the
reference consumer over the records under `export/changes/`, not the
Snowflake tables. They answer "does the bucket sink's output cover the
cells", so they do not yet work for a fleet that exports through
blob-stream.

These commands index export objects in SQLite on local disk. Use
`--cache /path/to/export-audit.sqlite` to reuse unchanged objects across
invocations. The index is bound to the endpoint, bucket and prefix; choose
one path per destination. A scheduled reconciler reuses a temporary index
when no path is supplied. Each refresh still lists the export prefix to
find late arrivals, replacements and retention deletes, but downloads only
new or changed objects. Tombstone changes invalidate and filter the index.
The cache contains exported data, so keep it on appropriate local storage.

Consumer evaluation loads one cell's history at a time; `verify --cell`
and `erase --cell` evaluate only that cell. Histories over 64 MiB of
encoded records per cell fail with an explicit diagnostic instead of
exhausting memory. Set `--max-cell-history N` (bytes) to raise this budget
when the audit host has sufficient memory. This local audit limit is separate from the warehouse
consumer. The first index build must read all export objects, since an
object may contain several cells.

#### verify

```sh
celld export verify [--sample N | --cell SCOPE [--facet PATH]] [--json]
```

Restores streams read-only at their bucket head, a random sample of
`--sample` streams (default 10) or the one `--cell` names, and compares
every exported table, row by row, with the consumer's state at that
position. A stream the consumer has not certified that far is reported as
behind. It exits non-zero on drift. The `kv` table and tables a `bulk`
record left uncertain are skipped. Run it on a schedule to catch what
nothing else would.

#### erase

```sh
celld export erase --cell SCOPE [--script NAME] [--facet PATH] [--incarnation N] [--reason TEXT]
celld export erase --cell SCOPE --clear
```

Writes a tombstone under
`export/tombstones/<cell>/<script>/<root | f.facet>/<all | incarnation>.json`
for the root and every facet the consumer holds, or only for `--script`,
`--facet` and `--incarnation` when given. Without an incarnation it erases
every incarnation of the scope. The reconciler, the reference consumer,
repair and backfill then skip the stream. `--clear` removes matching
tombstones, so a stream recreated under the same scope, which for a Durable
Object means the same name, exports again.

Erasure needs both sides. `celld export erase` stops the bucket-side paths.
`celld-export-loader erase SCRIPT CLASS CELL` tombstones the stream in
Snowflake and deletes its rows: routing drops its records from then on, the
views hide it at once, an hourly task deletes rows that arrive later, and
deleted rows stay in time travel for a day. The
bucket sink's objects already under `export/changes/` stay until
`CELLD_EXPORT_RETENTION` or your bucket's lifecycle rules remove them, and
records on the blob-stream topic until its retention expires, so keep both
within your erasure window.

## Dead-node recovery

When a node dies, the node that recovers its log emits a `recovered`
record for each cell epoch it saves into the bucket, after the upload and
before it seals the log. The record's `head` is what the bucket then holds
for that epoch, and `loss` marks a recovery that declared a bounded loss: no
complete copy of the log survived, so writes acknowledged after `head` may
be gone, and a consumer certified past `head` holds changes the cell no
longer has. The loss is also kept at `log/<session>.e<epoch>.loss.json` for
the reconciler.

Recovery only visits cells with rows left in the dead node's log, so a cell
whose writes were already in the bucket gets no record, and the reconciler
covers it. `cells` is the number of `recovered` records the recovery
emitted, so a consumer holding fewer knows some were lost.

Recovery does not know a cell's script or incarnation. The record carries
an empty `script` and incarnation `0`, and applies to the root stream of
its class and cell whose incarnation is the newest at or below
`head.epoch`, or, for a facet, to every stream at its facet path.

## Metrics

With export on, a node reports these gauges in its `/state` snapshot and
through [OTLP metrics](telemetry.md). A node with export off reports none of
them.

| gauge | meaning |
| --- | --- |
| `celld.export.queue_bytes` | Encoded bytes held against `CELLD_EXPORT_QUEUE_BYTES`. |
| `celld.export.pending_commits` | Captured commits waiting for their durability proof. |
| `celld.export.dropped_records` | Records dropped over the budget or after the retry deadline since the process started. |
| `celld.export.gaps` | Gap records emitted since the process started. |
| `celld.export.bulk_commits` | Commits exported as `bulk` since the process started. |
| `celld.export.attribution_mismatches` | Commits the capture could not attribute to a transaction since the process started. |

A growing `queue_bytes` means the sink is not keeping up or not reachable.
Any rise in `dropped_records`, `gaps` or `attribution_mismatches` means
`EXPORT_GAPS` will have work for repair. Frequent `bulk_commits` mean
transactions or schema changes larger than `CELLD_EXPORT_MAX_TX_BYTES`;
raise it, or expect repair snapshots for those tables.

## Memory and overflow

A node's export memory is bounded by `CELLD_EXPORT_QUEUE_BYTES`, plus one
transaction's capture (up to `CELLD_EXPORT_MAX_TX_BYTES` and the rows of the
statement that crossed it), plus a small record per resident stream.

| stage | limit | when it is exceeded |
| --- | --- | --- |
| capturing a transaction and materializing its changes | `CELLD_EXPORT_MAX_TX_BYTES` | capture stops for that transaction and the commit becomes `bulk` for the tables it touched |
| commits waiting for durability, records waiting for the sink | `CELLD_EXPORT_QUEUE_BYTES` | the oldest `rows` records are dropped, the stream stops advancing, and one `gap` per affected stream is emitted |
| a record | `CELLD_EXPORT_MAX_RECORD_BYTES` | the record is split into fragments; a single row that does not fit becomes `bulk` for its table |

After the first attribution/overflow gap, a residency emits no more row,
metadata or advance records. This prevents a sink outage from accumulating
one gap record per transaction. A later activation resumes capture; repair
and reconciliation cover the missing tail.

Delivery bookkeeping keeps at most 256 stream entries in memory and spills
older entries to a process-local SQLite file. It evaluates only streams
changed by the current acknowledgement batch. Spilled watermark counts
survive same-epoch reopenings. If the spill fails, export stops and
`/state` reports `export.delivery_failed: true`; restart after correcting
the local disk problem, then reconcile and repair.

## Failure modes

| event | what the consumer sees | what to do |
| --- | --- | --- |
| a node dies with records not yet delivered | the stream stops being certified; the next `link`, a `recovered` record or the reconciler shows the gap | repair |
| a node is fenced | nothing after the fence, so no rows celld could lose | nothing |
| brokers unreachable | records queue up to the budget, then `gap` records | repair once the brokers are back |
| a large transaction or schema change | `bulk` | repair |
| a schema change | a new generation, snapshotted inline | nothing; `sync` updates the Dynamic Table |
| a facet deleted | a `deleted` record removes it and the facets below it | nothing |
| a duplicate delivery | the same record twice | nothing; readers drop duplicates |
| export turned on for existing cells | new changes only | backfill |
| a stream erased | tombstones in the bucket and Snowflake | nothing; every path skips it |

## Performance benchmarks

See [export benchmarks](export-benchmarks.md) for the Criterion suites, timing
boundaries, local baseline comparisons and CI smoke checks.
