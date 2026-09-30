# Change export: a convergent mirror of every cell's rows

Status: design, revision 2, 2026-09-28. Nothing in this document is
implemented. Revision 2 answers a review of revision 1; the
[Decisions](#decisions) section records what changed and why.

celld replicates each cell as SQLite pages. A warehouse needs rows. This
design adds an optional exporter to the node that captures the rows each
transaction changed, releases them under the durability rule that governs
every other output of a cell, and sends them to a stream. The application
writes its SQLite database however it likes. The exporter imposes no schema,
no primary-key requirement, no outbox table, and no change to application
code.

The reference deployment is an AI assistant with one cell per customer, ten
million customers active every day, and Snowflake as the first consumer.
The sink is [blob-stream](https://github.com/bitdriftlabs/blob-stream), or
Kafka for a fleet that already runs it, with the fleet bucket as a third
sink for snapshots, repair, and fleets outside AWS.

## Contents

- [The contract](#the-contract)
- [What celld already provides](#what-celld-already-provides)
- [Architecture](#architecture)
- [Identity](#identity)
- [Capture](#capture)
- [Attributing a commit to a transaction id](#attributing-a-commit-to-a-transaction-id)
- [Release](#release)
- [Positions: released, delivered, applied](#positions-released-delivered-applied)
- [Records](#records)
- [DDL and table generations](#ddl-and-table-generations)
- [Facets](#facets)
- [Sinks](#sinks)
- [Completeness](#completeness)
- [Snapshots and repair](#snapshots-and-repair)
- [The Snowflake loader](#the-snowflake-loader)
- [Erasure](#erasure)
- [Memory and overflow](#memory-and-overflow)
- [Configuration](#configuration)
- [Cost](#cost)
- [Coverage and limitations](#coverage-and-limitations)
- [Failure modes](#failure-modes)
- [Testing](#testing)
- [Rollout](#rollout)
- [Decisions](#decisions)
- [Open questions](#open-questions)

## The contract

The export is a **convergent current-state mirror**, not a recoverable
transaction history.

- For every exported table in every exported cell, a consumer that has
  applied the stream holds the rows the cell holds, and converges to the
  cell's head state within a bounded delay after the cell goes quiet.
- The intermediate states a consumer passes through are real committed
  states of the cell, though not necessarily every one of them: several
  transactions can arrive as one net change.
- A row change leaves the node only after the write that made it has a
  durability proof and the node still owns the cell. The export never shows
  a state celld could lose, and a fenced node exports nothing.
- Loss is possible after release and before delivery. Every such loss is
  detected no later than the next reconciliation pass, and repair replaces
  the affected cell's state with an authoritative snapshot. A consumer can
  tell certified state from provisional state.
- The whole feature is off by default and costs nothing when off.

What the export does not promise: the sequence of every transaction, the
before-image of every update, or exactly-once delivery, even through the
Kafka sink. It does not export Queue message bodies, Workflow step state, or R2
objects.

## What celld already provides

1. **One connection per cell, opened in one place.** Every production cell
   database is opened by `open_at_epoch` in
   [`storage.rs`](../../crates/celld/storage.rs), and every facet by the
   facet open path in the same file. The cell is single-writer, and every
   application statement runs on the cell thread.
2. **An authorizer and a cursor check on that connection.** `authorize_sql`
   sees every `CREATE`, `ALTER`, and `DROP`, and denies `ATTACH` and
   therefore `VACUUM`. `ensure_no_unfinished_write_cursor` already refuses to
   publish while an implicit write cursor is open, which is exactly the
   predicate a capture safe point needs.
3. **Transactions with recorded WAL ranges.** Each LTX capture in
   [`db.rs`](../../crates/ltx/src/db.rs) takes the WAL frames written since
   the previous capture as one transaction id, and its header records the
   WAL salts, byte offset, and byte size of the range it read.
4. **Durability tickets and the output gate.** A write takes a ticket after
   it commits. The capture loop reads the ticket counter before it reads the
   WAL, so a ticket taken before a capture is covered by it. The gate in
   [`output_gate.rs`](../../crates/logic/output_gate.rs) holds every route
   that can reveal cell state until the ticket's barrier settles, including
   the ownership read a bucket proof requires. `handoff_wait` in
   [`ltx_repl.rs`](../../crates/celld/ltx_repl.rs) shows a node-internal
   client taking its own ticket.
5. **Dead-node recovery enumerates cells.** Recovery folds each recovered
   cell's tail into its per-cell prefix, so the code already visits every
   cell a dead session left behind.
6. **A bucket Parquet sink.** [`telemetry.rs`](../../crates/celld/telemetry.rs)
   batches per node, writes Parquet with the native column writer,
   partitions by hour, and sweeps retention.

The SQLite build has what capture needs, but it is not switched on. The
bundled SQLite is 3.45.0 with session and preupdate support in its source,
rusqlite 0.31 wraps the session extension behind its `session` feature, and
the bindings include `sqlite3session_object_config`. celld's resolved features
today are `backup`, `bundled`, `hooks`, and `serialize`; enabling `session`
rebuilds the bundled library with the session and preupdate flags and is the
first implementation step.

## Architecture

```
cell thread                            node                        stream / bucket
────────────────────────────────       ──────────────────────      ───────────────
WAL hook after each transaction:
  note (salts, frames)
host call returns, no open cursor,
autocommit on:
  ├─ changeset pulled (net rows)
  ├─ full images read
  └─ pending[cell].push(commit)
  └─ exporter takes a ticket

LTX capture (replication thread)
  each file: (salts, offset, size) ──►  label commits inside its boundary

exporter ticket settles          ──►  release labeled commits ≤ proof
                                        └─ node buffer ──► sink.produce()
                                              ├─ blob-stream (ack = durable in S3)
                                              ├─ Kafka (ack = every in-sync replica)
                                              └─ bucket Parquet
                                        └─ delivered position per sink
                                        └─ watermark after acks

activation, recovery, DDL,       ──►  link / recovered / schema /
facet delete                          deleted records (same path)

reconciliation (daily)           ──►  bucket inventory heads vs consumer

consumer group "snowflake"       ◄──  segments in S3, or the Kafka topic
  └─ loader ──► CELL_CHANGES ──► generations, snapshots, Dynamic Tables
```

Inside the node: **capture** on the cell thread, **attribution** in the
capture loop, **release** through the gate, a node-wide **sink** with a
per-sink **delivered position**. Outside: a **loader**, a **reconciler**
that compares bucket inventory against the consumer, and a **repair**
command that emits authoritative snapshots.

## Identity

Every record names the state it belongs to with four identifiers.

- **Stream.** The cell scope, and for a facet the root scope plus the facet
  path plus an **incarnation**: a 64-bit value the node writes into the
  facet's `_cf_METADATA` when it first creates the stream. A facet
  deleted and recreated under the same path has a new incarnation, so its
  records cannot be confused with the old stream's. Facet incarnations are
  ordered per root (the root's epoch, then a counter kept durably
  beside the root's epoch database), so every facet created before a delete has a smaller incarnation
  than every facet created after it. A root cell's incarnation is its
  first epoch.
- **Position.** `(epoch, txid, commit)`: the cell epoch, the LTX transaction
  id that contains the commit's last WAL frame, and the commit's sequence in
  the epoch. Positions order a stream totally. Consumers never order by
  stream offset.
- **Table generation.** `(table name, generation)`. The generation starts at
  one when the exporter first sees the table and increments on every DDL
  that touches it. Rows from different generations never merge. A drop and
  recreate under the same name is a new generation, so old rows cannot
  resurrect.
- **Row key.** The declared primary key columns when the table has one, in
  declared order; otherwise the rowid, which the session tracks through
  `SQLITE_SESSION_OBJCONFIG_ROWID`. A table with a declared key has no
  usable rowid in a changeset, and a rowid-only table's rowid can be changed
  by application SQL, in which case the session reports a delete and an
  insert.

## Capture

### The session

`open_at_epoch` creates one SQLite session on the cell's connection when
export is enabled for the cell's class. Before attaching, it sets the rowid
option. It then attaches every table through a filter that excludes
`_litestream_seq`, `_litestream_lock`, every `_cf_` table except `_cf_KV`,
`sqlite_` tables, shadow tables of virtual tables found through
`PRAGMA table_list`, and the reserved control tables of KV, Queue, and
Workflow cells by name.

The session records the key and old values of every row a statement
changes, nets repeated changes to one row within its lifetime, and omits a
row whose final values equal its original values. A `ROLLBACK TO` a
savepoint therefore needs no special handling: the reverted rows disappear
from the changeset and the surviving ones remain. The session does not
record virtual tables, rows whose declared key contains `NULL`, or DDL.

The rusqlite `Session` borrows the connection, and the cell's storage struct
owns the connection. The exporter holds a raw `sqlite3_session` pointer
beside it and calls the four ffi functions directly.

### The safe point

The changeset is pulled only at a **safe point**: the host call is
returning to the isolate, the connection reports autocommit mode, and no
statement is active on the connection. The third condition matters.
`sqlite3_get_autocommit` returns one while an `INSERT ... RETURNING` cursor
is still open with its write transaction uncommitted, and the cursor path
returns to the isolate after the first row. `ensure_no_unfinished_write_cursor`
already detects this state; the safe point reuses it, and additionally walks
`sqlite3_next_stmt` for any busy statement. A pull anywhere else would read
uncommitted rows and have no final WAL position.

One host call can complete several transactions: the cursor start path runs
prefix statements to completion before it returns, and a batch commits
several. The session nets them, and the pending commit that results
represents the state after the last of them. This is consistent with the
contract, which promises convergence and real intermediate states, not every
transaction.

Every path that ends a transaction on the cell thread reaches a safe point:
the storage entry points in
[`storage_ops.rs`](../../crates/celld/js/storage_ops.rs), `transaction_control`,
the D1 batch path, cursor completion, cursor reset, and every error return.
The implementation adds one call at the shared return of the storage op
dispatcher rather than at each site.

At the safe point, if the session is not empty, the exporter pulls the
changeset, recreates the session, materializes the commit, appends it to
the cell's pending list, and takes a durability ticket for it as described
under [Release](#release).

### Materializing a commit

For each changed row the exporter records the table generation, the
operation, and the row key. For an insert or update it reads the full row
after the commit with one keyed lookup. For a delete it takes the old row
from the session, which stores the full pre-image. Column names and
generated columns come from `PRAGMA table_xinfo`, cached per table
generation.

Every `rows` record carries whole rows. This is a decision: a consumer's
current state is the newest record per key and nothing more, and a lost
partial image would corrupt a row rather than delay it.

The exporter also stamps the pending commit with the WAL position of its
last transaction: the salts of the current WAL generation and the frame
count after the commit. Both come from the WAL hook, which SQLite invokes
after each transaction is written to the WAL, with the salts read from the
32-byte WAL header on the same thread before any later statement.

### Special tables

- **`_cf_KV`** holds the Durable Object key-value API. Values are V8
  serialized bytes, decoded to JSON with Deno's `v8_valueserializer` crate
  and exported as table `kv` with columns `key` and `value`. A value that
  does not decode is exported as a tagged blob.
- **`__kv`** in a KV namespace cell exports its key, text or bytes value,
  metadata, expiry, and for a value above one mebibyte the blob reference
  into `kv/blobs-v2/`, not the blob.
- **D1 tables** need nothing special.

## Attributing a commit to a transaction id

Consumers and repair reason in LTX positions because those are the cuts the
bucket holds. A pending commit needs the id of the capture that contains its
last WAL frame. The relation is not one to one: a capture takes every frame
since the previous capture, so one id can hold several commits, and a commit
that lands while a capture is reading belongs to the next id.

The label must be exact. A label later than the true capture lets a
delivered position claim state the stream has not sent; a label earlier
than the true capture releases a change before its capture is durable, which
breaks the release rule. The exporter never guesses.

Each pending commit carries `(salt1, salt2, frames_after)`. Every LTX file
a capture produces carries `(wal_salt1, wal_salt2, wal_offset, wal_size)`
and the page size. A frame `n` occupies bytes `32 + (n-1)·(24+page_size)` to
`32 + n·(24+page_size)` of its WAL generation, so a commit belongs to the
file with the same salts whose byte range contains its last frame. One
`db.sync()` can produce more than one file when a checkpoint intervenes;
attribution runs per file, in the order the capture wrote them.

A capture that writes a full database image has a boundary too: the WAL
generation and frame count visible to the read transaction it built the
image under. The capture records that boundary in memory beside the file it
wrote, and attribution treats it like any other range: every pending commit
at or below the boundary in that generation, and every pending commit in an
earlier generation, belongs to it. No rule assigns "every pending commit"
to anything, because a commit can complete after a capture's read and before
its return.

A commit whose frame lies beyond every boundary stays pending until the next
capture. A commit that cannot be matched after a capture whose boundary
passes it, which indicates a bug, is dropped, and the exporter emits a `gap`
record for the cell from its delivered position to the current durable
position and raises a metric. Repair handles the gap.

## Release

A pending commit is released when a durability proof covers its label and
the node has confirmed it still owns the cell. The exporter does not rely
on the request that made the commit: a request can commit and die before it
responds, and a later request can read that commit, so the commit is part of
the durable state whether or not its caller heard back. Release is defined
over the **durable commit prefix**: every labeled commit with a label at or
below the proven position.

The exporter is a gate client of its own. After each pull it takes a ticket,
the way `handoff_wait` does, and waits on it. When the ticket settles, the
proof covers every capture up to the position the capture loop credited to
that ticket, and the ownership read has passed for a bucket proof. The
exporter then releases every pending commit whose label is at or below
`durable_txid` at that moment, in commit order, to the node buffer. A fenced
node's tickets never settle, so it releases nothing.

The **released position** of a cell after a release is the largest label
such that every commit with that label or lower has been released. Labels
are non-decreasing in commit order, so it is one less than the smallest label
still pending, or the largest released label when nothing is pending.

## Positions: released, delivered, applied

Revision 1 emitted a watermark at release. That certified nothing, because
the node buffer could still drop the records behind it. Revision 2 separates
three positions.

- **Released position.** Node-internal. The commits the gate has let out of
  the cell.
- **Delivered position, per sink.** The largest position such that every
  record of every commit at or below it has been acknowledged by that sink,
  in commit order, with every fragment present. The sink adapter inspects
  the terminal result of every record it submits; blob-stream returns one
  per input record. The delivered position advances only on acknowledgement
  and never regresses. A dropped record freezes it and produces a `gap`.
- **Applied position.** Consumer-side. The largest position the consumer has
  fully applied, which it may only advance when it holds every fragment of
  every commit up to it.

A **watermark** record certifies a delivered position. The sink adapter
emits it after the acknowledgements it depends on, through the same sink, and
it carries the number of commits and records between the previous watermark
and this one. A consumer certifies a range only when the watermark has
arrived and its counts match what the consumer holds. Records that arrive
after a watermark, out of order, are provisional until their own watermark.

## Records

A record is one JSON object. The stream key is the stream identity, so one
producer keeps a stream's records ordered. Every record carries:

| field | meaning |
| --- | --- |
| `kind` | `rows`, `snapshot`, `snapshot_end`, `schema`, `link`, `recovered`, `deleted`, `watermark`, `bulk`, or `gap` |
| `script`, `class`, `cell`, `cell_name`, `facet`, `incarnation` | stream identity; `cell_name` is `_cf_METADATA.actor_name` when present |
| `epoch`, `txid`, `commit` | position |
| `committed_at` | milliseconds since the Unix epoch, from the cell thread at the commit |
| `node`, `origin` | producing node; `origin` is `live`, `snapshot`, or `repair` |
| `fragment`, `fragments` | `i` of `k` for a record split under the size limit; `1` of `1` otherwise |

A `rows` record adds `table`, `generation`, `columns`, `key_columns`, and
`rows: [[op, key, row], ...]`. One `rows` record covers one table of one
commit; a commit that touched several tables emits several records with the
same position, and the commit's record count is part of the watermark
arithmetic. `op` is `I`, `U`, or `D`. `row` is the full after-image for `I`
and `U` and the full before-image for `D`. Integers are JSON integers, reals
are JSON numbers, text is a string, a blob is `{"$blob": "<base64>"}`, and
`NULL` is `null`.

A record whose encoded size exceeds `CELLD_EXPORT_MAX_RECORD_BYTES` is split
by rows into fragments of the same table and position. A single row that
does not fit becomes a `bulk` record for its table.

A `snapshot` record has the same shape as `rows` with `op` always `I`, and a
`snapshot_id`. A `snapshot_end` record closes a snapshot with its scope, the
set of table generations it covered, and its record count. A `schema` record
adds `table`, `generation`, `sql`, `columns`, and `dropped` or `renamed_from`.
A `link` record adds `start_txid`, `prev_epoch`, `prev_txid`, and `mode`
(`fresh`, `clone`, `paged`, or `resume` for a clean reload of the same
epoch). A `recovered` record adds `session`, the
recovered head position, `loss` when recovery declared a bounded loss for the
session, and `cells`, the number of `recovered` records the recovery emitted. A `deleted` record names a stream and takes effect
at its position. A `bulk` record adds `tables`. A `gap` record adds
`from` and `to` positions and `reason`.

**Precedence.** Records for one stream apply in position order. A `snapshot`
with `snapshot_end` at position `P` supersedes every record of its scope
with a position at or below `P`, including rows the snapshot does not
contain, which are thereby deleted. At an equal position, `origin: repair`
precedes `origin: live`. A `deleted` record supersedes everything for its
stream at or below its position.

## DDL and table generations

The authorizer sees `CREATE TABLE`, `ALTER TABLE`, and `DROP TABLE` before
they run. The exporter notes the affected tables in the current transaction
and, at the safe point that ends it, acts per table:

- **Create.** A `schema` record opens generation one.
- **Drop.** A `schema` record with `dropped: true` closes the generation.
  The consumer deletes the cell's rows in that generation.
- **Rename, add column, drop column, or any other alteration.** The old
  generation closes and a new one opens with `renamed_from` when the name
  changed. Because a rename or an added default changes the logical rows
  without a row event, the exporter emits a `snapshot` of the table under
  the new generation at the same position, unless the table exceeds
  `CELLD_EXPORT_MAX_TX_BYTES`, in which case it emits `bulk` and repair
  provides the snapshot.

Schema records also accompany the first `rows` record of a table generation
in a stream, every snapshot, and every `link`, so a consumer that joins late
or a cell in the middle of a rolling migration always has the definition its
rows were encoded under. Two cells of one class can carry different
generations of a table at the same time; the consumer's typed projection is
per class and table and widens across generations under a policy described
with the loader.

## Facets

A facet is a cell with its own connection and its own LTX stream under the
root's prefix. The facet open path installs the same session and stamps the
facet's incarnation. Facet records carry the root scope, the facet path, and
the incarnation.

`FacetStreams::delete` in
[`facet_streams.rs`](../../crates/celld/facet_streams.rs) removes a facet and
every facet below it, resident or not, locally and in the bucket, with no
row or DDL event. The exporter emits a `deleted` record for the subtree from
the root's cell thread at that point, positioned at the root's current
position, and releases it through the root's next ticket. The record names
the facet path with `subtree` set and carries `through_incarnation`, a bound
above every incarnation handed out before the delete and below every one
handed out after it: it removes every stream
at or under the path with an incarnation at or below that bound, including
nested facets that were not resident and whose incarnations the node never
read. A recreated facet
under the same path gets a new incarnation, and its first records follow the
`deleted` record in the root stream's order. Repair cannot restore a deleted
facet, so the `deleted` record is authoritative; if it is lost, the
reconciler notices a consumer stream with no bucket prefix and emits it.

## Sinks

`ExportSink` is a trait: submit records for a set of streams and report a
terminal result per record. Three implementations ship, and
`CELLD_EXPORT_SINK` picks one. The blob-stream and Kafka sinks share
everything but their client: each record is one message on the fleet's
topic, the record's JSON keyed by its stream identity, with `committed_at`
as the event time; submits become overlapping produce calls whose results
are reported in submission order; the client connects in the background,
so a broker outage at boot delays export rather than failing the node; and
a record not acknowledged within `CELLD_EXPORT_RETRY_MS` is dropped and
freezes its stream's delivered position with a `gap`.

### blob-stream

The node runs the `blob-stream-producer` client behind a Cargo feature,
`export-blob-stream`, pinned to a git revision, because the crate is not
published and depends on a family of bitdrift crates and a patched protobuf
fork that a default celld build should not carry. Discovery is a static
broker list or a Kubernetes service. One topic per fleet. The writer id
follows the broker deployment's zone scheme.

A produce is acknowledged only after the broker has written the segment to
S3 and its metadata row to DynamoDB, so an acknowledged record is durable in
object storage. The producer batches for up to 500 ms; end-to-end latency
to a consumer is a few seconds.

blob-stream is at-least-once with possible offset gaps, and its partitions
are zone-local, so a stream that fails over to another zone spans two
virtual partitions. Consumers dedup on `(stream, position, table,
generation, key, fragment)` and order by position.

### Kafka

The node runs librdkafka, through `rdkafka`, behind a Cargo feature,
`export-kafka`, because it builds a C library (and OpenSSL, for SASL and
TLS) that a default celld build should not carry. `CELLD_EXPORT_KAFKA_BROKERS`
lists the bootstrap servers; `CELLD_EXPORT_KAFKA_PROPERTIES` names a file of
librdkafka properties applied over the sink's own, for security, SASL
credentials, compression and batching. One topic per fleet, created by the
operator with the partition count and replication the fleet needs.

The producer runs with `acks=all` and idempotence, so a produce is
acknowledged only once every in-sync replica has the message, and a retry
neither duplicates nor reorders it within its partition. The properties
file may not lower `acks`: a delivered position would otherwise certify
records a broker failure can still lose. Kafka's default partitioner hashes
the key, so a stream stays in one partition while the partition count does
not change. Connecting fetches the topic's metadata with topic
auto-creation off, so a missing topic or an unreachable cluster is reported
as the reason records are dropped, never answered with a topic on broker
defaults. The producer's `message.max.bytes` is `CELLD_EXPORT_MAX_RECORD_BYTES`
plus 64 KiB of framing, and the topic's own `max.message.bytes` must allow
the same, so every fragment fits in one message. `CELLD_EXPORT_RETRY_MS` is
`message.timeout.ms`; the sink waits for each message's delivery report,
and a report missing past the deadline drops the record, so the properties
file may not move the timeout or turn off successful delivery reports.

Kafka is at-least-once to consumers as well: the loader commits offsets
after landing, so a crash replays a batch. Consumers dedup on the same key
as for blob-stream and order by position, never by offset, because a
stream that moves nodes or a topic that gains partitions spreads a stream
over several partitions.

### The fleet bucket

The bucket sink writes Parquet under
`export/changes/<node>/<yyyy>/<mm>/<dd>/<hh>/<unix_us>-<rand>.parquet`, one
column per envelope field and the body as a JSON string, through the
telemetry writer. It is the sink for a fleet that runs neither blob-stream
nor Kafka,
the output of snapshots, repair, and backfill, and the way to inspect an
export with DuckDB. Its default flush is ten seconds, because a one-second
flush on a hundred nodes is over eight million objects a day.

The delivered position is tracked per sink, and a node runs one sink, so a
consumer follows the one the fleet chose.

## Completeness

The exporter is asynchronous after the gate, so a process loss can lose
released records the sink had not acknowledged. Three mechanisms make every
loss visible.

- **Links.** Every activation emits a `link` before the cell serves, naming
  the predecessor position from the chain the activation builds in
  `activate_with`. A link whose `prev_txid` exceeds the predecessor epoch's
  last certified position exposes a gap. Watermarks therefore certify up to
  the released TXID, not only up to the last commit with a record: a write
  to a table the export skips has a TXID but no record, and a link past it
  would otherwise always look like a gap.
- **Recovered records.** A node that acknowledged a write and died before
  export may never see that cell activate again. Dead-node recovery visits
  the cell epochs with rows in the dead session's bundles or follower tails
  when it folds them into the bucket. It does not visit a cell whose writes
  the session had already folded, so these records are best effort and the
  reconciler is the bound. After its last upload and before it seals the
  log, the recovering node emits a `recovered` record per visited cell epoch
  whose head is what the bucket then holds for that epoch, so a repair at
  the bucket's head reaches it; a recoverer that dies before emitting leaves
  it to the node that takes the recovery over. Recovery knows neither the
  cell's script nor its incarnation, so the record's stream carries an
  empty `script` and incarnation 0 (with the facet path for a facet), and
  the consumer applies it to the root stream of the same class and cell
  whose incarnation is the newest at or below the head's epoch, or, for a
  facet, to every stream at that facet path. A consumer compares the head with its certified
  position exactly as it does a link. With `loss`, writes acknowledged past
  the head may be in no copy and the cell restores without them, so a
  consumer certified past the head holds changes the cell lost; that is a
  gap too, and only a snapshot past those changes, such as one in the
  cell's next epoch, clears it. A loss can also touch cells recovery did
  not visit; the bucket keeps it at `log/<session>.e<epoch>.loss.json` for
  the reconciler. A consumer holding fewer records for a session than their
  `cells` knows some were lost.
- **Reconciliation.** A cell can be untouched by recovery and never
  activate again, or the `recovered` record itself can be lost. The
  reconciler runs on a schedule, daily by default. It reads the bucket's
  object inventory, S3 Inventory where available and a `LIST` walk
  otherwise, derives each cell's head position from the per-cell prefix
  names, includes bundle footers under `log/` for tails not yet folded, and
  compares against the consumer's certified positions and stream set. Every
  difference becomes a `gap`, a missing `deleted`, or an unknown stream to
  backfill. A bundle-only tail lags the inventory by at most one fold,
  eviction, or recovery.

The contract's detection bound is therefore the reconciliation interval, not
"from the stream alone".

## Snapshots and repair

Repair does not diff two states. Revision 1 diffed the states at both ends
of a gap, which cannot see an insert-then-delete or an update-and-revert
inside the gap and would leave a phantom row forever. Revision 2 repairs by
**authoritative snapshot replacement**.

`celld export repair --stream ID --at POSITION` restores the stream from the
bucket at the nearest position it can prove, emits a `schema` record for
every table generation present, a `snapshot` of every exported table, and a
`snapshot_end`, all at that position with `origin: repair`. By the
precedence rule the consumer replaces the stream's state, including deleting
rows the snapshot lacks. Repair never needs the gap's start.

The restorable positions are the cuts the bucket holds. Recovery merges a
folded tail into one LTX range through `merge_l0_rows`, compaction adds
ranges, and the planner restores to the latest cut at or before a target, so
a position inside a merged range is not restorable. The command therefore
restores at the requested position or the first cut above it, and always at
the head when asked for it; it reports the position it actually restored,
and the snapshot carries that position. A gap that ends below the head is
repaired at the head, which is always a cut.

`celld export backfill` snapshots every stream in a class at its head, with
a concurrency limit and a bucket rate limit. It is the same code as repair.
`celld export verify --sample N` restores a sample of streams at their head
after the consumer's applied position reaches that head and compares every
table against the consumer's current state.

Repair is not a history recovery. A consumer that wants the transactions
lost in a gap does not get them; it gets the state after them.

## The Snowflake loader

`celld-export-loader` is a Rust service in consumer group `snowflake` of the
fleet's topic: on `blob-stream-consumer`, or on a librdkafka consumer for a
Kafka fleet, chosen by `EXPORT_SOURCE`. Both are one `Source` to the same
loop, so batching, landing and committing do not depend on the transport;
a landed row's `source` is `blob-stream/<partition>/<offset>` or
`kafka/<partition>/<offset>`. The Kafka consumer commits only explicitly,
never automatically, and when the group revokes partitions it lands and
commits what it read from them before letting go, as the blob-stream
consumer does. It groups records into batches and sends each
batch through the Snowpipe Streaming REST API to one pipe,
`EXPORT_LANDING_PIPE`, whose `COPY` casts each record into a row of
`EXPORT_LANDING`; a task routes landed rows into the tables below. There is
no stage and no Parquet on this path, and no warehouse runs to load:
Snowpipe Streaming bills per GB ingested, and records land within seconds.
Elastic channels acknowledge durably but do not order, so the loader
advances its source offsets only after every record of a batch is
acknowledged, and it treats arrival order as meaningless: completeness comes
from positions and watermarks, never from order.

The model:

- `CELL_CHANGES`: envelope columns typed, `rows VARIANT`, clustered by
  `committed_at`. Append-only. Dedup key
  `(stream, position, table, generation, key, fragment)`.
- `CELL_META`: every non-row record.
- `CELL_SNAPSHOTS`: for each stream and scope, the latest position with a
  complete `snapshot_end`, derived from `CELL_META`.
- `CELL_CERTIFIED`: per stream, the applied position the loader may
  certify, derived from watermarks whose counts match.
- One Dynamic Table per `(script, class, table)`: takes rows with position at
  or above the stream's latest snapshot position for that table generation,
  keeps the newest per `(stream, generation, key)` by position with
  `origin: repair` first at ties, drops `D` rows and rows of closed
  generations, drops streams with a later `deleted`, and projects the row
  object into typed columns. The projection is generated from the union of
  `schema` records for the class and table: a new column is added, a type
  conflict widens to `VARIANT`, and a column that exists only in some
  generations is nullable. The pattern is the newest-row-per-key form that
  Snowflake refreshes incrementally.
- `EXPORT_GAPS`: `gap` records, links and recovered records whose
  predecessor position exceeds the certified position, and reconciler
  findings. The repair driver polls it.
- `EXPORT_TOMBSTONES`: see [Erasure](#erasure).

Snapshot, repair, and backfill records go through the same sink as live
records, so they reach the topic and the loader like any other. Records
that only exist as bucket-sink files are landed with
`celld-export-loader ingest`, through the same pipe.

## Erasure

A purge in Snowflake is temporary if a replayed segment, a repair, a
backfill, or an ingested file can reinsert the data. Erasure therefore has a
durable tombstone that every ingestion path consults.

`celld export erase --stream ID` writes a tombstone object under
`export/tombstones/<stream>` in the bucket and a row in
`EXPORT_TOMBSTONES`. The loader drops records for a tombstoned stream before
they reach `CELL_CHANGES`; repair, backfill, and the reconciler skip
tombstoned streams; the Dynamic Tables exclude them. A scheduled task deletes
the stream's rows from `CELL_CHANGES` and `CELL_META`, and time-travel
retention on the export tables is set low enough for that deletion to
complete inside the compliance window. The bucket sink's export objects are
covered by lifecycle rules with a retention no longer than the
window, and blob-stream's topic retention is days. A stream recreated after
erasure, which for a Durable Object means the same name and therefore the
same scope, is a new incarnation and clears the tombstone by an explicit
operator action.

## Memory and overflow

Revision 1 claimed a hard cap it did not have. These are the actual bounds,
each with its overflow policy.

| stage | bound | on overflow |
| --- | --- | --- |
| session tracking | `CELLD_EXPORT_MAX_TX_BYTES`, checked after every writing statement; one statement can overshoot by the rows it touches, which is at most the table it touches | tracking stops for the rest of the transaction; the commit becomes `bulk` for the tables the transaction touched |
| materialization | bounded changeset output and a shared `CELLD_EXPORT_MAX_TX_BYTES` budget for full images and reshaped rows; one row can overshoot while being read | the affected table becomes `bulk` |
| pending commits and node buffer | one shared budget, `CELLD_EXPORT_QUEUE_BYTES`, counted in encoded bytes | the oldest released `rows` records are dropped; the delivered position freezes; one gap note per affected stream, which is O(resident streams) and tiny |
| meta records | bounded by streams with pending notes, one entry each | never dropped; the budget accounting excludes them because their total is bounded by the resident set |
| pending commits with no proof | a fenced or partitioned node never releases; the pending list grows with writes | the same shared budget; dropped pending commits become a gap note released with the next proof, and a fenced node's notes die with it and surface through links |

After an attribution gap, the residency stops submitting records and
advances. Delivery tracks only streams touched by an acknowledgement batch,
with a 256-entry hot cache and a process-local SQLite spill preserving
older watermark chains. Offline audit commands use a separate incremental
SQLite object index and evaluate one cell at a time; `docs/export.md`
describes its scope and history limit.

The exporter's memory is therefore bounded by the shared budget plus one
transaction's overshoot plus the resident stream count.

## Configuration

| variable | default | effect |
| --- | --- | --- |
| `CELLD_EXPORT` | `0` | `0` disables export. `1` enables it. |
| `CELLD_EXPORT_SINK` | `bucket` | `bucket`, `blob-stream`, or `kafka`. |
| `CELLD_EXPORT_BUCKET` | the fleet bucket | A different bucket for the bucket sink, same endpoint and credentials. |
| `CELLD_EXPORT_CLASSES` | application classes, `__D1Database`, `__KvNamespace` | Allow list. `__Queue`, `__Workflow.*`, and cron scopes are never exported. Facets follow their root's class. |
| `CELLD_EXPORT_TABLES` | unset | Deny list of `Class.table`. |
| `CELLD_EXPORT_MAX_TX_BYTES` | `4194304` | Session, changeset output and materialized row budget above which a transaction becomes `bulk`; also the largest table snapshotted inline on DDL. |
| `CELLD_EXPORT_MAX_RECORD_BYTES` | `1048576` | Fragment size. |
| `CELLD_EXPORT_QUEUE_BYTES` | `268435456` | Shared budget for pending commits and the node buffer. |
| `CELLD_EXPORT_FLUSH_MS` | `10000` | Bucket sink flush interval and watermark cadence. |
| `CELLD_EXPORT_FLUSH_BYTES` | `8388608` | Bucket sink early flush. |
| `CELLD_EXPORT_RETENTION` | `none` | Bucket sink sweep; `none` leaves lifecycle to the consumer. |
| `CELLD_EXPORT_TOPIC` | `celld-changes` | blob-stream or Kafka topic. |
| `CELLD_EXPORT_BROKERS` | unset | Static brokers, or `k8s://NAMESPACE/SERVICE`. |
| `CELLD_EXPORT_WRITER_ID` | the node's zone | blob-stream writer id. |
| `CELLD_EXPORT_KAFKA_BROKERS` | unset | Kafka bootstrap servers, `host:port`. |
| `CELLD_EXPORT_KAFKA_PROPERTIES` | unset | A file of librdkafka properties over the Kafka sink's own. |
| `CELLD_EXPORT_RETRY_MS` | `30000` | blob-stream or Kafka delivery deadline before a record counts as dropped. |
| `CELLD_EXPORT_RECONCILE` | `24h` | Reconciler interval, run by the loader deployment, not the node. |

`celld export` subcommands: `repair`, `backfill`, `verify`, `erase`,
`reconcile`, and `inspect`.

## Cost

Per transaction, capture costs a hash insert and an old-row copy per changed
row inside the transaction, one keyed lookup per inserted or updated row at
the safe point, JSON encoding, and two small reads of the WAL. For an
assistant that writes a few kilobytes per event this is tens of microseconds
on the cell thread. The lab measures it before it ships.

At ten million daily-active customers with about twenty transactions each:

| quantity | value |
| --- | --- |
| commits per second, fleet-wide | about 2,300 average, about 10,000 at peak |
| export bytes per second | about 5 MB/s |
| blob-stream brokers | two small brokers per zone; their reported six-broker cluster carries a thousand times this |
| blob-stream object operations | one segment and one metadata row per broker flush, on the order of 10 per second fleet-wide |
| bucket sink objects at a 10 s flush on 100 nodes | about 860,000 per day, plus list and sweep operations for a consumer that follows it |
| Snowflake ingest at 400 GB/day | about 1.5 credits/day |
| Snowflake Dynamic Table refresh | proportional to changed partitions; the dominant warehouse cost, sized in the lab |
| reconciliation | one inventory read per day, or a `LIST` walk of about ten thousand requests |

## Coverage and limitations

The exporter covers every ordinary table regardless of key shape, `WITHOUT
ROWID` tables, the Durable Object key-value API, D1 databases, and KV
namespaces. It does not cover:

- virtual tables, including `sqlite-vec` indexes; their shadow tables are
  filtered and a `schema` record marks the virtual table unsupported;
- a row whose declared primary key contains `NULL`, which the session does
  not record; `verify` reports drift from such rows;
- writes through the incremental blob API, which the session does not
  record; no application path in celld reaches that API, since the storage
  surface is SQL and the key-value methods;
- Queue message bodies, Workflow state, and R2 objects, by policy.

## Failure modes

| event | what happens | what the consumer sees |
| --- | --- | --- |
| node process loss | released records not yet acknowledged are lost | the delivered position froze before them; the next link, a recovered record, or the reconciler exposes the gap; repair snapshots the stream |
| node fenced | nothing released after the fence | no phantom rows; an unacknowledged tail a later restore exposes is covered by the next snapshot |
| request commits and dies | the exporter's own ticket releases the commit once durable | the commit appears |
| takeover to another zone | one stream spans two virtual partitions | ordering by position |
| brokers unreachable | queue to the budget, then drop oldest rows and note gaps | `gap` records; repair |
| capture attribution mismatch | commits dropped, `gap`, metric | repair |
| WAL restart between captures | boundaries carry the generation | nothing |
| several transactions in one host call | one net commit at the last position | a real committed state |
| DDL | new generation; snapshot or `bulk` | typed view updates; no resurrection |
| facet deleted | `deleted` record from the root | subtree removed |
| large transaction | `bulk` | repair snapshots the tables |
| duplicate delivery | at-least-once stream or retried flush | dedup key |
| export enabled on a fleet with existing cells | new changes flow; no history | `backfill` |
| erasure | tombstone in bucket and warehouse | every path skips the stream |

## Testing

- **Unit:** changeset decoding for every key shape, WAL frame arithmetic,
  released-position arithmetic under interleaved labels, fragmenting and
  reassembly, generation transitions, precedence.
- **Probes that the review ran, kept as regression tests:** a pull attempted
  while an `INSERT ... RETURNING` cursor is open must be refused; an insert
  and delete of one row in one host call must produce no row change; a
  rename must produce a snapshot under a new generation; a rowid change on a
  keyed table must produce no change and on a rowid-only table a delete and
  an insert.
- **Deterministic interleaving:** commits on the cell thread and captures on
  the replication thread in every order, asserting each commit's label equals
  the file whose recorded WAL range contains its last frame, including
  full-image captures and captures split by a checkpoint.
- **Property:** random transactions against random schemas with DDL and
  savepoints, a reference consumer applying the stream, and final equality
  with the database at every watermark's counts.
- **Fault injection:** kill the node between release and acknowledgement;
  run the loader; assert `EXPORT_GAPS` names the stream; run repair; assert
  equality. Kill a broker mid-produce; assert no duplicate escapes the dedup
  key. Delete a facet and recreate it; assert the old rows are gone and the
  new incarnation's rows present.
- **Reconciliation:** a cell that never activates again after a loss must
  appear in `EXPORT_GAPS` after one reconciler run.
- **Lab:** the write-latency bench with export off, on with the bucket
  sink, and on with blob-stream, per transaction size.
- **Continuous:** `verify --sample` on a schedule in production.

## Rollout

1. Enable the `session` feature and confirm the bundled build; the sink
   trait, the envelope, the bucket sink, and the Snowflake DDL with
   synthetic records.
2. Capture at the safe point, attribution, gate release, delivered
   positions, watermarks, links, schema and generations. Bucket sink only.
   Verified against restores.
3. Snapshots, `repair`, `backfill`, `verify`, `erase`, and the reconciler.
4. Facet `deleted` records and `recovered` records from dead-node recovery.
5. The blob-stream sink behind its Cargo feature and the loader on the
   consumer crate; the Kafka sink and the loader's Kafka consumer behind
   theirs.
6. Enable on one class in one fleet with `verify` and the reconciler
   running; then widen.

A fleet outside AWS stops at step four with a consumer on the bucket sink.

## Decisions

- **2026-09-28. Updates carry the full row.** Every `rows` record holds the
  complete after-image for an insert or update and the complete before-image
  for a delete. Consumers never merge partial images.
- **2026-09-28, revision 2. The export is a convergent current-state mirror,
  not a transaction history.** Several transactions may arrive as one net
  change, repair replaces state with a snapshot rather than replaying
  changes, and completeness is defined as convergence with detection bounded
  by the reconciliation interval. The alternative, a recoverable history,
  would require export records to ride the node log and follower disks so
  that recovery replays them, which is a change to the replication protocol
  and its recovery proofs. The mirror fits the replication architecture,
  which already restores state rather than history.
- **2026-09-28, revision 2. Row keys are the declared primary key, else the
  rowid.** Revision 1 keyed rowid tables by rowid, which a changeset does not
  carry for a table with a declared key.
- **2026-09-28, revision 2. Release covers the durable commit prefix, through
  the exporter's own ticket.** Revision 1 tied release to the request's
  ticket, which would have held a dying request's committed write out of the
  export while later requests could read it.
- **2026-09-28, revision 2. Watermarks certify delivery, not release.**
- **2026-09-30. Kafka is an alternative to blob-stream, chosen by
  configuration.** A fleet that already runs Kafka should not have to run
  blob-stream brokers to export. The Kafka sink and the loader's Kafka
  consumer sit behind their own Cargo features, share the topic sink and
  the consumer loop with blob-stream, and the loader still reads
  blob-stream unless told otherwise. A node still runs one sink.

## Open questions

- Whether the full-image capture's boundary should be recorded in the LTX
  header itself, which is a format change gated by `BUCKET_FORMAT`, or kept
  in memory beside the file as designed. In memory is enough for the live
  path; the header would let an offline tool reproduce attribution.
- Whether the reconciler should read bundle footers, which requires
  DynamoDB-free access to `log/` and a parser for the bundle footer, or
  accept that a bundle-only tail is invisible until the next fold, eviction,
  or recovery.
- How the blob-stream writer id maps to zones for a fleet that is not zone
  aware, and whether one writer id per fleet is acceptable at this volume.
- Whether the loader should resolve `__kv` blob references from the bucket,
  or leave that to a consumer that asks for it.
- The typed-projection policy for a column whose type changes across
  generations: widen to `VARIANT` as designed, or keep the newest type and
  cast older rows on read.
