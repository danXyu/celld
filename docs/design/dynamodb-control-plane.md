# DynamoDB control plane: an optional home for fleet coordination

Status: design, revision 1, 2026-09-29. Nothing in this document is
implemented.

celld coordinates a fleet through conditional writes to the fleet bucket.
The bucket holds each cell's ownership record, each node's lease, and a
handful of fleet singletons, next to the LTX data those records govern.
This design adds Amazon DynamoDB as an optional second home for that
coordination state. The bucket stays the default and stays the only
required infrastructure. A fleet that opts in keeps every byte of cell
data in the bucket and moves only the small, mutable, compare-and-swapped
records to one DynamoDB table.

The motivation is latency, request cost, and scan shape at scale. S3
conditional writes take tens of milliseconds at the median and hundreds at
the tail, and that tail is spent out of the node lease's self-fence
margin. Every node lists and reads every lease several times a minute.
The reference deployment, one cell per customer and ten million customers
active every day, pays for an ownership write on every cold activation.

## Contents

- [Goals and non-goals](#goals-and-non-goals)
- [What moves and what stays](#what-moves-and-what-stays)
- [Selecting the backend](#selecting-the-backend)
- [The table](#the-table)
- [The store contract](#the-store-contract)
- [Records](#records)
- [Ordering across two stores](#ordering-across-two-stores)
- [Code structure](#code-structure)
- [The startup probe](#the-startup-probe)
- [Partition limits and scan cost](#partition-limits-and-scan-cost)
- [Latency](#latency)
- [Cost](#cost)
- [Failure modes](#failure-modes)
- [Security](#security)
- [Configuration](#configuration)
- [Testing](#testing)
- [Migration](#migration)
- [Rollout](#rollout)
- [Later: the wake index](#later-the-wake-index)
- [Decisions](#decisions)
- [Open questions](#open-questions)

## Goals and non-goals

Goals:

- A fleet can choose DynamoDB for coordination state with one setting.
  Nothing changes for a fleet that does not.
- Both guarantees in [guarantees.md](../guarantees.md) hold unchanged in
  either mode: at most one node owns a cell, and an acknowledged write
  survives any single-node loss.
- Every caller reaches coordination state through one typed interface.
  No module outside the two backends builds a coordination key or calls
  the bucket's conditional write for one.
- A fleet can move from bucket to DynamoDB, and back, with a short
  coordinated stop and no bulk copy of per-cell records.

Non-goals:

- Moving LTX files, node-log bundles, loss records, deployments, blobs,
  or export output out of the bucket.
- A general "metadata database". The table holds only records that are
  compare-and-swapped or scanned as a set.
- Other databases. The interface admits one, but this design qualifies
  DynamoDB alone.
- Multi-region. Global tables are refused (see
  [The store contract](#the-store-contract)).

## What moves and what stays

| Record | Today | DynamoDB mode | Why |
|---|---|---|---|
| Cell ownership `cells/<cell>/own.json` | bucket | **table** | Written on every activation and release; read on every bucket-proof ack |
| Node lease and folded node-log record `nodes/<node>.json` | bucket | **table** | Compare-and-swapped every TTL/3; scanned by five loops |
| Node load telemetry (inside the lease today) | bucket | **table, own item** | Advisory; split out so lease scans stay small |
| Fleet capacity sample `fleet/capacity-v1.json` | bucket | **removed** | A query over load items replaces it |
| Waker role lease `wake/waker.json` | bucket | **table** | Compare-and-swapped every tick from two loops |
| Drain token `drain/token.json` | bucket | **table** | Polled every second during drains |
| Deploy pointers `deploy/current.json`, `deploy/<script>/current.json` | bucket | **table** | Switched together in one transaction |
| Queue attachments `deploy/queues/<q>/consumer.json` | bucket | **table** | Part of the same deploy transaction |
| Backend marker `fleet/control.json` | — | **bucket (new)** | Tells every node and CLI where coordination lives |
| Wake index `wake/entries/`, `wake/retired/`, `wake/format.json` | bucket | bucket | Its protocol depends on immutable names; see [Later](#later-the-wake-index) |
| Peer-auth secret `fleet/peer-auth.json` | bucket | bucket | Written once, read at boot; nothing to gain |
| LTX data `cells/<cell>/ltx/` | bucket | bucket | Data |
| Node-log bundles, recovery checkpoints, loss records `log/` | bucket | bucket | Data; the export reconciler lists loss records |
| Deploy modules, manifests, assets | bucket | bucket | Immutable, addressed through the pointers |
| `probe/`, `preview-snapshots/`, `node-cells/` | bucket | bucket | Unchanged |

The LTX epoch chain stays a bucket listing. Restore derives it from
`cells/<cell>/ltx/` after the ownership write, and an index of it in the
table would add an ordering problem between the LTX PUT and the index
write that the listing does not have.

## Selecting the backend

`CELLD_CONTROL` names the backend: unset or `bucket` for the bucket,
`dynamodb://TABLE` for a table. The choice belongs to the fleet, not to
one node, so the bucket records it.

`fleet/control.json` is created once, with a conditional create:

```json
{"format": 1, "backend": "dynamodb", "table": "celld-prod", "region": "us-east-1", "fleet": "b6f1…"}
```

- A node creates the marker at startup if it is absent, from its own
  `CELLD_CONTROL`. The bucket form is `{"format":1,"backend":"bucket"}`.
- A node whose `CELLD_CONTROL` disagrees with the marker refuses to
  start and names both values. Two nodes can therefore never coordinate
  one fleet through two stores.
- Creating a `dynamodb` marker requires that `nodes/` holds no unexpired
  lease, the same refusal check `wake_format.rs` uses. A running bucket
  fleet cannot be joined by a DynamoDB node.
- The `fleet` value is random. The table holds the same value in its
  meta item, so a bucket prefix pointed at another fleet's table is
  refused.
- CLIs (`celld deploy`, `celld cell`, `celld queue`, `celld export`) read
  the marker and open the same backend. Operators do not pass a new flag
  to them.

A bucket without a marker is a bucket fleet. This keeps every existing
fleet valid. Releases before this one do not read the marker, so the
upgrade that introduces it needs the coordinated stop described in
[Migration](#migration) before any fleet switches backend, exactly as the
wake format change did.

## The table

One table. String partition key `pk`, string sort key `sk`. No secondary
indexes, no local indexes, no streams in revision 1. On-demand capacity by
default. Point-in-time recovery and deletion protection on. DynamoDB TTL
may be enabled only on the attribute `gc_at`, which celld sets on load
items and never on an authority item.

| Item | `pk` | `sk` | Attributes |
|---|---|---|---|
| Fleet meta | `meta` | `fleet` | `fleet`, `format`, `lease_shards` |
| Cell owner | `cell#<cell>` | `own` | `node` (empty when released), `epoch`, `v` |
| Node lease | `nodes#<shard>` | `<node>` | lease fields, `log` map, `v`, `updated_ms` |
| Node load | `load` | `<node>` | load fields, `updated_ms`, `gc_at` |
| Waker role | `fleet` | `waker` | `node`, `expires_ms`, `v` |
| Drain token | `fleet` | `drain` | `node`, `expires_ms`, `restoration_baseline`, `v` |
| Fleet pointer | `deploy` | `current` | pointer fields, `v` |
| Named pointer | `deploy` | `script#<name>` | pointer fields, `v` |
| Queue attachment | `deploy` | `queue#<queue>` | attachment fields, `v` |
| Probe | `probe` | `<random>` | `v` |

`<shard>` is a stable hash of the node name modulo `lease_shards`,
which is fixed when the fleet is created (default 1; see
[Partition limits](#partition-limits-and-scan-cost)).

Every item stores its fields as native attributes rather than a JSON
blob, except the folded `log` object, which keeps its JSON shape
(`NodeLogWire`) as a map. `NodeLeaseWire` keeps unknown fields today by
preserving raw lease bodies for mixed-version readers. The table
preserves them the same way: a writer loads the item, replaces the fields
it owns, and writes back every attribute it read.

## The store contract

The guarantees need four properties from the bucket: conditional create,
conditional overwrite, read-after-write, and exact ranged reads. The table
must provide the first three for every record it holds.

**Version tokens.** Every authority item carries `v`, a random 128-bit
hex string that the writer generates for each write. The existing
`CasGuard::{Absent, Match(token)}` (`crates/logic/types.rs:285`) carries
it unchanged. The core never parses a token, so an etag and a `v` are
interchangeable.

- `CasGuard::Absent` becomes `attribute_not_exists(pk)`.
- `CasGuard::Match(t)` becomes `v = :t`.

**Resolving ambiguity.** A write that times out can have committed. The
bucket resolves that with a readback. The table does the same, but the
answer is exact: if the read returns `v` equal to the token this writer
generated, the write committed; any other value means it did not, or was
overwritten after. Every write also sets
`ReturnValuesOnConditionCheckFailure = ALL_OLD`, so a rejected write
returns the current item and the core's follow-up read is free.

**Error classes.** Each response maps onto the classes the bucket lane
already uses (`LeaseCasError`, `ownership_store.rs:355`):

| Response | Class |
|---|---|
| `ConditionalCheckFailedException`, or `TransactionCanceledException` whose reasons include `ConditionalCheckFailed` | clean rejection |
| `ProvisionedThroughputExceededException`, `ThrottlingException`, `RequestLimitExceeded` | not committed |
| `ValidationException`, `AccessDeniedException`, `ResourceNotFoundException`, `UnrecognizedClientException` | not committed |
| `InternalServerError`, any 5xx, a timeout, a reset connection | ambiguous |
| `TransactionConflictException`, `TransactionInProgressException` | not committed |

Throttling is not committed: DynamoDB rejects a throttled request before
applying it. This matters, because it lets a throttled lease renewal
retry with its current token instead of spending a readback.

**No transport retries on writes.** The bucket's conditional client
retries zero times (`bucket.rs`, `cas_retry`), and the reason carries
over: a hidden retry of a write that already committed answers as a lost
race. The DynamoDB client retries reads and nothing else. A
`TransactWriteItems` call carries a `ClientRequestToken`, which makes a
retried transaction idempotent for ten minutes; the deploy path uses that
and retries.

**Consistency.** Every read of an authority item uses
`ConsistentRead = true`. The table is refused if it is a global table.
Reads never go through DAX. There are no secondary indexes to read by
mistake. Load items are advisory and read eventually consistent, which
halves their cost.

**Clocks.** `capacity_record_is_recent` (`ownership_store.rs:344`) uses
the bucket's `Last-Modified` to drop stale leases from placement. The
table has no server write time, so every lease and load item carries
`updated_ms`, the writer's wall clock. The filter already tolerates three
TTLs of skew; it keeps that window.

**Timeouts.** The lease lane keeps its own client, pool and user agent,
as it does on the bucket (`fleet::lease_bucket_client_with_credentials`).
Its bounds stay configurable and start at the bucket's values: connect
3 s, request 15 s. The self-fence arithmetic in the core already bounds a
renewal attempt by the authority it has left, so faster responses need no
core change to benefit.

## Records

### Cell ownership

`read_owner`, `cas_owner` and `release_owner` map one to one onto
`GetItem`, conditional `PutItem`, and conditional `UpdateItem`. The epoch
rule is unchanged: every acquire writes `epoch + 1`, and a release writes
an empty `node` and keeps the epoch. `Effect::VerifyOwnership`
(`actor.rs:3851`) stays one read.

Two behaviors change:

- `delete_streams` (`ltx_repl.rs:2957`) deletes everything under
  `cells/<cell>` in the bucket, which today includes `own.json`. In table
  mode the owner item is not deleted with the streams. The epoch stays
  monotonic across a delete, which the fence already assumes.
- `celld cell list` enumerates `cells/` prefixes. A cell that was
  acquired but never wrote an LTX file has a prefix today because of
  `own.json`. In table mode it has none and is not listed. Such a cell
  holds no data, so the listing is still complete for data.

### Node leases and the folded log

`cas_node_lease` becomes a conditional `PutItem` on `v`. The folded log
record rides in the same item, so the node-log paths that CAS a dead
node's lease (`node_log.rs`, `write_dead_record`) and the claim and seal
steps of recovery use the same call.

The rule "a folded record is never deleted, and an absent record proves
the bucket is complete" is unchanged. What changes is dead-node GC
(`dead_node_gc.rs:369`). The bucket has no conditional delete, so GC
writes a tombstone and then deletes unconditionally, and a folded record
is kept forever as a tombstone because a late unconditional delete could
erase a successor's record. The table has conditional delete, and a
delete conditioned on `v` cannot remove a record a successor rewrote. So:

- A dead record with no log is removed with one `DeleteItem` conditioned
  on the `v` that GC read and judged dead.
- A dead, sealed record keeps today's terminal state, a tombstone with
  `expires_ms = 0`. The dead-leader sweep still needs it to find sealed
  sessions and GC their bundles. Removing it is a separate change.

Every loop that listed `nodes/` and read each lease (the capacity scan,
node-log `maintain`, `sweep_dead_leaders`, dead-node GC, the ready gate,
the wake-format stop check, `fleet::node_lease_ids`) becomes one `Query`
per lease shard. A node keeps one fleet view, refreshed at most once per
`CELLD_FLEET_VIEW_MS` (default 5000), and every loop reads that view
instead of its own scan. A loop that needs a fresher view than the cache
holds, such as recovery about to judge a lease expired, forces a refresh.

### Load and placement

A node writes its load item with an unconditional `PutItem` on every
renewal. It carries no authority, so a lost or late write is harmless.
Placement, rebalance, the format gate and container `max_instances` read
load with one eventually consistent `Query` on `pk = load`.

This removes the capacity sample and its refresh claim. The sample exists
so that one node scans the fleet and the rest read one object; a single
query over small items is cheaper than that object's read would be in the
table, where reads are billed per 4 KB. It also removes the sample's size
problem: the sample embeds every lease, and at about 1.5 KB per node it
passes DynamoDB's 400 KB item limit between 200 and 400 nodes.

### Fleet singletons

The waker role and the drain token keep their protocols: read, then
conditional write on `v`. The drain token is still released by writing an
expired record, because readers treat "absent" and "expired" alike and
the release path does not need to change.

### Deploy pointers

`celld deploy` today writes modules and manifests, then compare-and-swaps
the queue attachments, then the named pointer, then the fleet pointer
(`deploy.rs`). A crash between the pointer writes leaves them
inconsistent until the next deploy.

In table mode the attachments and both pointers switch in one
`TransactWriteItems`, each conditioned on the `v` the deploy read. The
blobs, modules and manifests are still written to the bucket first. A
node polls the fleet pointer every `CELLD_DEPLOY_POLL_S` as today; a
stream-driven push is left for later.

The managed control-plane client (`control_plane.rs`) writes the same
pointers and uses the same interface.

## Ordering across two stores

Nothing in celld writes coordination state and data atomically. Safety
comes from order:

1. Takeover: conditional write of the owner record at `epoch + 1`, then
   list the LTX epochs, then write under `e<epoch+1>/`.
2. Bucket-proof acknowledgement: LTX PUT completes, then the owner record
   is read and must still name this node at this epoch.
3. Bundle credit: bundle PUT completes, then the lease is read and its
   log must still be open at the shipper's epoch.
4. Recovery: claim the log, list and read bundles, write per-cell LTX and
   loss records, then seal the log.
5. Log reconfiguration: tier the open fragment to the bucket, then change
   the ensemble in the lease.
6. Wake retirement: prove durability, read the owner record, then write
   the retirement record.

Each of these is a completed operation on one store followed by an
operation on the other. S3 and a DynamoDB table read with
`ConsistentRead` are each linearizable, and a system of linearizable
objects is linearizable, so an operation that completes before another
begins is observed by it whichever store holds each. The orderings hold
without change provided that (a) every authority read is consistent and
(b) no step returns before the store has acknowledged. Both are rules of
[The store contract](#the-store-contract), and the probe checks (a).

The epoch in the LTX key remains the fence. A stale owner's writes land
in a superseded prefix whichever store holds the owner record.

## Code structure

A new module, `control_store.rs`, holds one enum, in the style of the
existing `Ownership` adapter (`actor.rs:194`):

```rust
pub enum ControlStore {
    Bucket(BucketControl),
    Dynamo(DynamoControl),
}
```

Its operations are typed, not keyed: `read_owner`, `cas_owner`,
`release_owner`, `read_lease`, `cas_lease`, `delete_dead_lease`,
`fleet_leases`, `publish_load`, `fleet_load`, `read_singleton`,
`cas_singleton`, `read_pointer`, `switch_pointers`. `BucketControl` is
the code that exists today, moved behind these methods. `Ownership`
keeps its `Memory` variant for the single-node development mode, and its
`Bucket` variant takes a `ControlStore`.

These modules build coordination keys or call the bucket's conditional
write for them today, and move behind the interface:

- `ownership_store.rs`: owners, leases, the capacity sample,
  `fleet_class_instances`
- `node_log.rs`: dead-session lease writes, the `nodes/` listing in the
  dead-leader sweep
- `dead_node_gc.rs`, `drain_token.rs`, `wake.rs` (waker role and owner
  reads), `wake_format.rs` (the stop check), `fleet.rs` (lease lookups,
  pointer reads), `deploy.rs`, `control_plane.rs` (pointer writes), and
  the ready gate in `main.rs`

The refactor is behavior-neutral for bucket fleets and lands first, on
its own. A lint test rejects `"nodes/"`, `"own.json"`, `"drain/"`,
`"wake/waker"` and `"deploy/current"` outside the two backends.

**The DynamoDB client.** `dynamo.rs` implements the eight operations the
table needs (`GetItem`, `PutItem`, `UpdateItem`, `DeleteItem`, `Query`,
`TransactWriteItems`, `DescribeTable`, `DescribeContinuousBackups`) as
JSON over the `reqwest` client celld already has. It does not depend on
the AWS SDK. Requests are signed with `object_store::aws::AwsAuthorizer`,
and credentials come from `AmazonS3::credentials()` on the fleet's own S3
client, so the table uses exactly the credential chain the bucket does:
environment, web identity, container and instance metadata. A DynamoDB
fleet therefore requires an `s3://` bucket.

## The startup probe

The bucket probe (`Bucket::probe_cas_steps`, `bucket.rs`) provokes the
two rejections a conforming store must produce. A table-mode node runs the
same four steps against a `probe` item, then checks:

- `DescribeTable`: key schema `pk`/`sk` strings, no global replicas, no
  secondary indexes, and a TTL attribute that is absent or `gc_at`.
- The meta item exists and its `fleet` matches the bucket marker.
- `DescribeContinuousBackups`: point-in-time recovery is on. A warning,
  not a refusal.

A violation stops the node, as a bucket violation does. `celld diagnose`
runs the same checks and prints the table's billing mode and any
throttling seen since boot.

## Partition limits and scan cost

A DynamoDB partition serves up to 1,000 write units and 3,000 read units
per second. All node leases share one partition key per shard.

- **Writes.** A lease without load telemetry is about 600 bytes, one
  write unit, renewed every TTL/3. One shard carries renewals for about
  3,000 nodes.
- **Reads.** Consistent lease scans are the binding limit. With one fleet
  view per node refreshed every 5 s, N nodes read N leases each, so read
  units grow with N²: about 300 read units per second at 100 nodes,
  2,700 at 300, and 30,000 at 1,000.

Revision 1 targets fleets up to 300 nodes. One shard serves about 200
nodes at the default view interval; above that, set `lease_shards` to 2
or more, which spreads the scan across partitions but does not reduce its
total cost. A fleet beyond 300 nodes should raise `CELLD_FLEET_VIEW_MS`
as well. A design
that reads only leases that changed is future work, and the N² term is
inherited from the bucket, where the same scans cost more.

Owner items are keyed by cell, so activation traffic spreads across
partitions without configuration.

## Latency

Estimates, not measurements: same region, small items, S3 Standard,
on-demand table. Phase 0 of the [rollout](#rollout) replaces them.

| Operation | Bucket p50 / p99 | Table p50 / p99 |
|---|---|---|
| Lease renewal | 40 / 300 ms | 6 / 25 ms |
| Cold activation, control-plane part (owner read, placement read, owner write) | 90 / 400 ms | 15 / 60 ms |
| Cold activation including the LTX epoch listing, which stays in the bucket | 130 / 550 ms | 50 / 230 ms |
| Bucket-proof acknowledgement (LTX PUT, then owner read) | 60 / 400 ms | 45 / 320 ms |
| Fleet lease scan, 100 nodes | 450 ms / 1.3 s | 20 / 60 ms |

Default fleet-durability writes and warm requests do not touch
coordination state and do not change. The largest effect is on the tail
of lease renewal, where a slow conditional write spends self-fence
margin.

## Cost

At list prices for us-east-1, which should be checked before relying on
them: S3 writes and lists cost $5.00 per million and reads $0.40; the
table costs $0.625 per million write units and $0.125 per million read
units.

| Workload | Bucket per month | Table per month |
|---|---|---|
| 100-node fleet overhead (renewals, scans, singletons, pointer polls) | about $1,400 | about $200–350 |
| 10M cells, 3 activation cycles per cell per day | about $10,100 | about $1,350 |
| Unchanged: LTX epoch listing per activation | about $4,500 | about $4,500 |

The saving comes from per-cell traffic, not from the fleet size. Large
fan-out reads cost more in the table than in the bucket, which is why the
load items are small and the capacity sample is not carried over.

## Failure modes

- **Either store unavailable stops the fleet.** Today one regional
  dependency can stop the fleet; in table mode there are two. A table
  outage stops lease renewal, and every node self-fences within one TTL.
  A bucket outage still stops restore and bucket-proof writes. Fleets
  that choose the table accept this in exchange for the latency and cost
  above; it is the main reason the bucket stays the default.
- **Throttling.** An on-demand table throttles traffic that more than
  doubles its previous peak. A throttled renewal is not committed and
  retries with its token inside its remaining authority, but a sustained
  throttle self-fences nodes. Operators should pre-warm the table for the
  expected peak or use provisioned capacity with headroom. `celld
  diagnose` reports throttled requests, and a metric counts them.
- **Clock skew.** Unchanged in kind. Lease expiry compares the writer's
  `expires_ms` with the reader's clock, as it does today.
- **Marker loss.** If `fleet/control.json` is deleted from a table
  fleet, a bucket-configured node could create a bucket marker and start
  beside the table fleet, because it sees no leases in `nodes/`. Every
  table-mode node therefore re-reads the marker on each fleet-view
  refresh and recreates it with a conditional create when it is missing,
  so the window lasts one refresh while any table node runs. A table node
  that finds a bucket marker in its place self-fences. The marker joins
  the reserved prefixes and the security guide.
- **A table restored from a backup.** Point-in-time recovery restores
  owner epochs that can be lower than the epochs already written in the
  bucket. Restore refuses to proceed when the newest non-empty epoch is
  at or above the claimed one, so a rolled-back epoch cannot overwrite
  data, but cells then cannot activate. Restoring the table requires the
  epoch floor repair described in [Migration](#migration).

## Security

The bucket is documented as the fleet's root of authority. In table mode
authority is split: ownership and leases are in the table, and the
peer-auth secret, deployments and data are in the bucket. A principal
that can write either can disrupt the fleet. The table needs:

```
dynamodb:GetItem, PutItem, UpdateItem, DeleteItem, Query,
TransactWriteItems, ConditionCheckItem, DescribeTable,
DescribeContinuousBackups
```

on the one table ARN. CLI principals that only deploy need `GetItem`,
`TransactWriteItems` and `ConditionCheckItem`. Encryption at rest uses
the table's KMS setting.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `CELLD_CONTROL` | `bucket` | `bucket`, or `dynamodb://TABLE` |
| `CELLD_CONTROL_REGION` | `AWS_REGION` | The table's region |
| `CELLD_CONTROL_ENDPOINT` | none | An endpoint override, for DynamoDB Local in tests |
| `CELLD_FLEET_VIEW_MS` | 5000 | The shared lease view's maximum age |

`celld control init --table NAME` creates the table with the schema,
on-demand billing, point-in-time recovery, and deletion protection, and
writes the meta item. Operators who manage tables with their own tooling
create it to the same schema and run `celld control init --adopt`, which
checks the table and writes only the meta item.

## Testing

- **One contract suite, three backends.** The existing ownership and
  lease tests run against `Memory`, `Bucket` (local store), and `Dynamo`
  through one parameterized suite.
- **DynamoDB Local in CI.** A job runs the celld integration tests with
  `CELLD_CONTROL=dynamodb://…` against DynamoDB Local.
- **A fault-injecting fake.** An in-process table implements the eight
  operations and can commit a write and then time out, throttle, or
  return 500. It drives the ambiguity tests: every ambiguous write must
  resolve through its `v` token, and a throttled renewal must retry
  without a readback.
- **Cross-store ordering tests.** The node-log double-loss, incarnation
  and recovery-witness tests (`node_log/*_tests.rs`) run in table mode,
  with the fake delaying table responses relative to bucket responses.
- **Release qualification against real DynamoDB**, as release tests run
  against R2 today.

## Migration

A fleet moves from the bucket to the table in one short coordinated stop
and a lazy copy of owner records. Copying ten million owner records up
front would take hours of listing; the lazy copy needs no downtime for
them.

1. Stop every node. `celld control migrate --to dynamodb://TABLE`
   refuses while any lease is unexpired.
2. The command copies, preserving every field: every `nodes/*.json`
   (including tombstones and folded logs, which recovery needs), the
   drain token, the waker lease, the deploy pointers and queue
   attachments. It writes the marker with `"migrating": true`.
3. Start nodes configured for the table. While the marker says
   `migrating`, an owner read that finds no item reads
   `cells/<cell>/own.json` from the bucket and, if present, creates the
   item with the same node and epoch under `attribute_not_exists`. The
   bucket record is frozen, because no bucket-mode node can start, so
   every copier writes the same value and one wins.
4. A background task walks `cells/` and copies the remaining owner
   records the same way. When it completes, it clears `migrating`, and
   owner reads stop consulting the bucket.

Moving back runs the same steps in the other direction, copying items
to objects.

The same epoch-floor rule repairs a table restored from a backup: `celld
control repair-epochs` walks `cells/`, and for each cell whose newest
non-empty LTX epoch is at or above the owner item's epoch, writes an
unowned item at that epoch.

## Rollout

- **Phase 0, measure.** Per-record-class latency histograms and request
  counters on today's bucket calls. This sizes the benefit before the
  work starts, and several findings are useful for bucket fleets
  regardless: sharing one lease view across loops removes most of the
  bucket's largest request line, the repeated `nodes/` scans.
- **Phase 1, the interface.** `ControlStore` with the bucket backend
  only, every caller moved behind it, the lint test, and the shared fleet
  view. No behavior change.
- **Phase 2, the table.** `DynamoControl`, the client, the probe, the
  marker, `celld control init`, the contract suite in CI, and updates to
  [guarantees.md](../guarantees.md), [security.md](../security.md) and
  [limitations.md](../limitations.md).
- **Phase 3, migration.** `celld control migrate`, the lazy owner copy,
  `repair-epochs`, and a qualification run that migrates a loaded fleet
  both ways.
- **Phase 4, the wake index**, if phase 0 shows alarm arming or the due
  scan matter.

## Later: the wake index

The wake index stays in the bucket in this revision. Its protocol assumes
immutable entry names: a late PUT can only recreate an obsolete name, and
a retirement watermark tells the collector which names are obsolete. That
protocol would work in the table unchanged, but moving it only pays off
if the scans get cheaper too, and the table cannot enumerate "minutes that
have entries" the way a delimiter listing does.

A sketch for a later revision:

- Entries as items under `pk = wake#<minute>#<shard>`, with the shard a
  hash of the cell, so one busy minute does not load one partition.
- A minute directory item written with each arm, and deleted only for
  minutes older than a grace period once every shard is empty. Arms clamp
  their discovery minute to no earlier than the current minute, so no arm
  can target a directory entry the collector is about to delete. The
  minute is only a discovery hint; SQLite holds the deadline.
- Arm as a transaction with a condition check on the cell's retirement
  item, so a late arm cannot recreate a retired entry and the collector no
  longer has to chase ghosts. The eager delete then has to use the
  published key rather than recomputing it from `at_ms`, because of the
  clamp.

## Decisions

- **The bucket stays the default and the only required store.** The
  table adds an availability dependency and is AWS-only; fleets on R2,
  GCS, Azure and Tigris are unaffected.
- **One table, not one per record type.** One set of permissions, one
  probe, one backup policy.
- **No AWS SDK.** Signing and credentials come from `object_store`, which
  celld already uses for S3, and the client is small.
- **Random version tokens rather than counters.** A token the writer
  generated resolves an ambiguous write exactly on readback.
- **Load leaves the lease.** It is advisory, rewritten on every renewal,
  and makes every authority scan three times more expensive.
- **The capacity sample is not ported.** It exists to save bucket reads.
  In the table it would cost more than the query it replaces, and it does
  not fit an item past a few hundred nodes.
- **The wake index waits.** Its protocol is the most intricate in the
  bucket, and the benefit is unmeasured.
- **The LTX epoch chain stays a listing.** An index would create a new
  write-then-index ordering.

## Open questions

- Do facet scopes (`facet_streams.rs`, which calls `delete_streams`) have
  owner records of their own, and should deleting a facet delete its
  owner item?
- Should the dead-leader sweep learn to read sealed sessions from another
  record, so a sealed, dead lease can be deleted outright in table mode
  instead of kept as a tombstone?
- Is 300 nodes the right target for revision 1, or does the reference
  deployment need an incremental lease view now?
- Should the deploy pointer poll become a push from DynamoDB Streams, and
  is a second consumer of the table worth that dependency?
- Should the table require the bucket and the table to share a region, or
  only warn?
