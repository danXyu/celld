# DynamoDB control plane: an optional home for fleet coordination

Status: revision 2, 2026-09-29. The table, its selection, the routing of
every coordination record, and `celld control init|show` are
implemented. [Not built yet](#not-built-yet) lists what revision 1
proposed and this revision leaves for later; the
[Decisions](#decisions) section records what changed and why.

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
- [Routing](#routing)
- [The table](#the-table)
- [The store contract](#the-store-contract)
- [Records](#records)
- [Ordering across two stores](#ordering-across-two-stores)
- [The startup checks](#the-startup-checks)
- [Partition limits and scan cost](#partition-limits-and-scan-cost)
- [Latency](#latency)
- [Cost](#cost)
- [Failure modes](#failure-modes)
- [Security](#security)
- [Configuration](#configuration)
- [Testing](#testing)
- [Not built yet](#not-built-yet)
- [Decisions](#decisions)
- [Open questions](#open-questions)

## Goals and non-goals

Goals:

- A fleet can choose DynamoDB for coordination state with one setting.
  Nothing changes for a fleet that does not: a bucket fleet issues exactly
  the store requests it issued before.
- Both guarantees in [guarantees.md](../guarantees.md) hold unchanged in
  either mode: at most one node owns a cell, and an acknowledged write
  survives any single-node loss.
- No call site can reach the wrong copy of a coordination record, including
  call sites written after this change.

Non-goals:

- Moving LTX files, node-log bundles, loss records, deployments, blobs,
  or export output out of the bucket.
- A general "metadata database". The table holds only records that are
  compare-and-swapped or scanned as a set.
- Other databases. Nothing here prevents one, but only DynamoDB is built.
- Multi-region. Global tables are refused (see
  [The store contract](#the-store-contract)).

## What moves and what stays

| Record | Bucket key | Home on a table fleet |
|---|---|---|
| Cell ownership | `cells/<cell>/own.json` | **table** |
| Node lease, with the folded node-log record and load | `nodes/<node>.json` | **table** |
| Drain token | `drain/token.json` | **table** |
| Waker role lease | `wake/waker.json` | **table** |
| Fleet deploy pointer | `deploy/current.json` | **table** |
| Named deploy pointer | `deploy/<script>/current.json` | **table** |
| Queue attachment | `deploy/queues/<queue>/consumer.json` | **table** |
| Backend marker (new) | `fleet/control.json` | bucket |
| Fleet capacity sample | `fleet/capacity-v1.json` | bucket |
| Wake index | `wake/entries/`, `wake/retired/`, `wake/format.json` | bucket |
| Peer-auth secret | `fleet/peer-auth.json` | bucket |
| LTX data | `cells/<cell>/ltx/` | bucket |
| Node-log bundles, recovery checkpoints, loss records | `log/` | bucket |
| Deploy modules, manifests, assets | `deploy/<script>/<version>/`, `deploy-blobs/` | bucket |
| Everything else | | bucket |

The LTX epoch chain stays a bucket listing. Restore derives it from
`cells/<cell>/ltx/` after the ownership write, and an index of it in the
table would add an ordering problem between the LTX PUT and the index
write that the listing does not have.

## Selecting the backend

`CELLD_CONTROL` names the backend: unset or `bucket` for the bucket,
`dynamodb://TABLE` for a table. The choice belongs to the fleet, not to
one node, so the bucket records it in `fleet/control.json`, created once
with a conditional create:

```json
{"format":1,"backend":"dynamodb","table":"celld-prod","region":"us-east-1","fleet":"b6f1…"}
```

- `celld control init` creates the table if needed, claims it, and writes
  the marker. It is the recommended first step for a table fleet.
- A serving node resolves the marker at startup, before it reads any
  record (`control::resolve`, role `Node`). If the marker is absent the
  node records its own `CELLD_CONTROL`, so a bucket fleet gets
  `{"format":1,"backend":"bucket"}` on the first start of this release.
- A node whose `CELLD_CONTROL` disagrees with the marker refuses to start
  and names both values. Two nodes therefore never coordinate one fleet
  through two stores.
- Creating a `dynamodb` marker requires that the bucket holds no unexpired
  node lease. A running bucket fleet cannot be joined by a table node.
- The `fleet` value is random. The table's meta item holds the fleet that
  claimed it, so a second fleet pointed at the same table is refused.
- The node's lease lane is a second bucket client with its own connection
  pool. It resolves second (role `Lease`), follows the marker, and opens
  its own table client, so lease traffic keeps its isolated pool.
- Operator commands resolve read-only (role `Operator`) and follow the
  marker. `celld deploy`, `celld cell`, `celld queue` and the rest need no
  new flag.

Releases before this one do not read the marker, so a table fleet must not
run an older binary. That is the same constraint the wake-format change
carried.

## Routing

`Bucket` routes the coordination records itself. `ControlKey::parse`
(`crates/celld/control.rs`) recognizes exactly the seven key shapes in the
table above. When the bucket client's route is a table, `get`, `head`,
`put`, `put_cas`, `delete` and the new `delete_if_token` send a recognized
key to the table, and `list` and `objects_page` answer a listing that
covers `nodes/`, `drain/`, `wake/` or `deploy/` with the table's records
merged in (and any bucket object under a record's key left out). Every
other key goes to the bucket as before.

The route is resolved once per opened client and shared by every clone of
it. A bucket fleet's route is the bucket, and a bucket fleet takes none
of the new branches: the check is a string match with no I/O, so the
sequence of store requests is unchanged. A client that is never resolved
behaves as a bucket fleet, unless the process is configured for a table,
in which case it refuses every coordination record. That makes a missed
resolution loud instead of letting it read an empty bucket.

This replaces the typed `ControlStore` interface of revision 1; see
[Decisions](#decisions).

## The table

One table. String partition key `pk`, string sort key `sk`. No secondary
indexes, no streams, no time-to-live. On-demand capacity, point-in-time
recovery and deletion protection when `celld control init` creates it.

| Item | `pk` | `sk` |
|---|---|---|
| Fleet meta | `meta` | `fleet` |
| Cell owner | `cell#<cell>` | `own` |
| Node lease | `nodes` | `<node>` |
| Drain token | `fleet` | `drain` |
| Waker role | `fleet` | `waker` |
| Fleet pointer | `deploy` | `current` |
| Named pointer | `deploy` | `script#<name>` |
| Queue attachment | `deploy` | `queue#<queue>` |
| Probe (transient) | `probe` | `<random>` |

Every item carries the same three attributes: `doc`, the record's JSON
body exactly as the bucket would hold it; `v`, the version token; and
`updated_ms`, the writer's wall clock. Keeping the body verbatim means every
reader and writer, including one that preserves fields a newer release
added (`CapacitySample` keeps raw lease bodies for that reason), works
unchanged.

## The store contract

The guarantees need four properties from the bucket: conditional create,
conditional overwrite, read-after-write, and exact ranged reads. The table
must provide the first three for every record it holds.

**Version tokens.** `v` is a random 128-bit hex string that the writer
generates for each write. It travels through `CasGuard::Match` like an
etag; the core never parses a token.

- A create is `attribute_not_exists(pk)`.
- An overwrite is `v = :expected`.

Because the writer generated the token, a readback after an ambiguous
write is exact: the item holds this writer's token or it does not.

**Error classes.** Each failure maps onto the classes the bucket lane
already uses (`LeaseCasError`, and `bucket::cas_write_did_not_commit`):

| Response | Class |
|---|---|
| `ConditionalCheckFailedException` | clean rejection (`Ok(None)`) |
| Any other 4xx, including throttling, validation, access denied, and resource not found | not committed |
| A connection that never opened | not committed |
| 5xx, a timeout, a reset connection, an unreadable success response | may have committed |

DynamoDB authenticates, validates and admits a request before it applies
it, so a 4xx answer means nothing changed. Throttling is in that class,
which lets a throttled lease renewal retry with the token it already
holds instead of spending a readback.

**No repeated writes.** The bucket's conditional client retries zero times
(`bucket.rs`, `cas_retry`), because a repeat of a write that already
committed answers as a lost race. The table client repeats reads, up to
twice, and never repeats a write.

**Consistency.** Every `GetItem` and `Query` sets `ConsistentRead`. The
startup checks refuse a global table, a secondary index, and
time-to-live.

**Clocks.** `capacity_record_is_recent` filters leases by the bucket's
`Last-Modified`. A listed table record reports `updated_ms` in that field,
the writer's clock rather than the store's; the filter's three-TTL window
already tolerates that skew.

**Timeouts.** The table client uses the bucket's bounds, connect 3 s and
request 15 s, because they are part of the self-fence arithmetic.

## Records

### Cell ownership

`read_owner`, `cas_owner` and `release_owner` in `ownership_store.rs` are
unchanged; their `get` and `put_cas` reach the table. The epoch rule is
unchanged: every acquire writes `epoch + 1`, and a release writes an empty
`node` and keeps the epoch. `Effect::VerifyOwnership` stays one read.

Two behaviors change on a table fleet:

- `delete_streams` (`ltx_repl.rs`) deletes the bucket objects under
  `cells/<cell>`, which on a bucket fleet includes `own.json`. On a table
  fleet the owner item survives, so the epoch stays monotonic across the
  delete, which the fence already assumes.
- `celld cell list` enumerates `cells/` prefixes. A cell that was acquired
  but never wrote an LTX file has a prefix on a bucket fleet because of
  `own.json`, and none on a table fleet. Such a cell holds no data.

### Node leases and the folded log

Lease renewal, dead-session recovery (`node_log::write_dead_record`), and
every lease reader reach the table through the same routed calls. The
folded node-log record rides in the lease item.

The rule "a folded record is never deleted, and an absent record proves
the bucket is complete" is unchanged. Dead-node GC (`dead_node_gc.rs`)
writes a tombstone and then deletes. It now deletes with
`delete_if_token`, passing the tombstone's token: on the bucket that is
the unconditional delete it always was, and on the table the delete is
conditioned on the token, so a delete that lands late cannot remove a
record a successor wrote.

Every loop that lists `nodes/` and reads each lease (the capacity scan,
node-log maintenance, the dead-leader sweep, dead-node GC, the ready gate,
the wake-format stop check, `fleet::node_lease_ids`) gets its listing from
one `Query` on the `nodes` partition and then reads each lease as before.

### Fleet singletons and deploy pointers

The waker role, the drain token, the deploy pointers and the queue
attachments keep their protocols unchanged, through the routed calls.
`control_plane::deployment_exists` lists `deploy/` to find a pointer; the
merged listing returns the table's pointers alongside the bucket's
deployments.

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
[The store contract](#the-store-contract), and the startup checks enforce
(a).

The epoch in the LTX key remains the fence. A stale owner's writes land
in a superseded prefix whichever store holds the owner record.

The end-to-end run in [Testing](#testing) exercised orderings 1 to 4: a
node killed with `SIGKILL` right after acknowledging a write was
recovered and sealed by its peer from the table's copy of its lease, and
the peer served the next write on top of the acknowledged one.

## The startup checks

A table node, and `celld control init`, check before serving:

- `DescribeTable`: the table is active, its keys are `pk`/`sk` strings,
  it has no secondary indexes and no replicas.
- `DescribeTimeToLive`: time-to-live is disabled.
- The meta item names this fleet, or is absent and is then claimed.
- The probe: create an absent item, fail to create it again, update it
  with the current token, fail to update it with a stale token, and read
  back the last write. A failure stops the node, as the bucket probe's
  does.

The bucket probe still runs too: the wake index and the marker are
bucket records written with conditional writes.

`celld diagnose` prints which store holds the records. `celld control
show` prints the marker, the table's shape check, its fleet, whether
point-in-time recovery is on, and the number of node leases.

## Partition limits and scan cost

A DynamoDB partition serves up to 1,000 write units and 3,000 read units
per second. All node leases share the `nodes` partition.

- **Writes.** A lease with its load telemetry is about 1.5 KB, two write
  units, renewed every TTL/3. The partition carries renewals for roughly
  1,500 nodes.
- **Reads.** Every lease scan is a `Query` on that partition followed by a
  `GetItem` per lease, and several loops on every node scan, so reads
  grow with N². The bucket has the same shape, and the table answers it
  faster and more cheaply, but past a few hundred nodes the scans need the
  work under [Not built yet](#not-built-yet).

Owner items are keyed by cell, so activation traffic spreads across
partitions without configuration.

## Latency

Estimates, not measurements: same region, small items, S3 Standard,
on-demand table.

| Operation | Bucket p50 / p99 | Table p50 / p99 |
|---|---|---|
| Lease renewal | 40 / 300 ms | 6 / 25 ms |
| Cold activation, control-plane part (owner read, placement read, owner write) | 90 / 400 ms | 15 / 60 ms |
| Cold activation including the LTX epoch listing, which stays in the bucket | 130 / 550 ms | 50 / 230 ms |
| Bucket-proof acknowledgement (LTX PUT, then owner read) | 60 / 400 ms | 45 / 320 ms |

Default fleet-durability writes and warm requests do not touch
coordination state and do not change. The largest effect is on the tail
of lease renewal, where a slow conditional write spends self-fence
margin. Against DynamoDB Local on one machine, renewals completed in 4 to
17 ms.

## Cost

At list prices for us-east-1, which should be checked before relying on
them: S3 writes and lists cost $5.00 per million and reads $0.40; the
table costs $0.625 per million write units and $0.125 per million read
units.

| Workload | Bucket per month | Table per month |
|---|---|---|
| 10M cells, 3 activation cycles per cell per day | about $10,100 | about $1,350 |
| Unchanged: LTX epoch listing per activation | about $4,500 | about $4,500 |

The saving comes from per-cell traffic, not from the fleet size.

## Failure modes

- **Either store unavailable stops the fleet.** Today one regional
  dependency can stop the fleet; on a table fleet there are two. A table
  outage stops lease renewal, and every node self-fences within one TTL.
  A bucket outage still stops restore and bucket-proof writes. This is
  the main reason the bucket stays the default.
- **Throttling.** An on-demand table throttles traffic that more than
  doubles its previous peak. A throttled renewal is not committed and
  retries with its token inside its remaining authority, but a sustained
  throttle self-fences nodes. Pre-warm the table for the expected peak, or
  use provisioned capacity with headroom.
- **Clock skew.** Unchanged in kind. Lease expiry compares the writer's
  `expires_ms` with the reader's clock, as it does today.
- **Marker loss.** If `fleet/control.json` is deleted while a table fleet
  runs, a bucket-configured node could create a bucket marker and start
  beside it, because the bucket shows no live lease. The marker is under
  the reserved `fleet/` prefix. Detecting its loss from a running table
  node is an open question.
- **A table restored from a backup.** Point-in-time recovery restores
  owner epochs that can be lower than the epochs already written in the
  bucket. Restore refuses to proceed when the newest non-empty epoch is
  at or above the claimed one, so a rolled-back epoch cannot overwrite
  data, but those cells then cannot activate until their epochs are
  repaired.

## Security

The bucket is documented as the fleet's root of authority. On a table
fleet authority is split: ownership and leases are in the table, and the
peer-auth secret, deployments, data and the marker are in the bucket. A
principal that can write either can disrupt the fleet. A node needs
`GetItem`, `PutItem`, `DeleteItem`, `Query`, `DescribeTable` and
`DescribeTimeToLive` on the one table. `celld control init` also needs
`CreateTable`, `UpdateContinuousBackups` and `DescribeContinuousBackups`.

The table client signs with `object_store`'s SigV4 signer and the S3
client's own credential chain, so the table authenticates exactly as the
bucket does. celld links no AWS SDK.

## Configuration

| Variable | Default | Meaning |
|---|---|---|
| `CELLD_CONTROL` | follow the marker; `bucket` for a new fleet | `bucket`, or `dynamodb://TABLE` |
| `CELLD_CONTROL_REGION` | the bucket's region | The table's region, when the marker does not name one |
| `CELLD_CONTROL_ENDPOINT` | none | An endpoint override, for DynamoDB Local |

```
celld control init --table NAME [--table-region REGION] [--no-create] --bucket s3://NAME
celld control show --bucket s3://NAME [--json]
```

## Testing

- **A fake table** (`control/tests.rs`) implements the operations celld
  sends and injects a throttle, or a write that applies and then answers
  500. The tests cover the key mapping, listing plans, the marker rules,
  table-shape refusals, the fleet claim, conditional deletes, the error
  classes, and that a write is attempted once.
- **DynamoDB Local.** `a_live_table_honors_the_contract` runs the create,
  shape check, probe, claim, conditional writes, paging and conditional
  deletes against a real endpoint when `CELLD_TEST_DYNAMODB_ENDPOINT` is
  set. CI starts DynamoDB Local and runs it.
- **The existing suites** run unchanged on bucket fleets, whose routes are
  fixed to the bucket.
- **End to end**, by hand, with MinIO for the bucket and DynamoDB Local
  for the table: `celld control init`, `celld deploy`, two nodes, a
  Durable Object counter written through both, and `SIGKILL` of the
  owner. The survivor recovered and sealed the dead node's log from its
  table lease and continued the counter without losing an acknowledged
  increment. The bucket held only the marker and data; the table held the
  owner, lease, meta and pointer items.

## Not built yet

These were proposed in revision 1 and are left for later:

- **Migration.** `celld control migrate`, with a lazy copy of owner
  records, in either direction. Today a fleet chooses its store when it
  starts, and a bucket fleet with live leases cannot switch.
- **Repairing epochs after a table restore**, `celld control
  repair-epochs`.
- **Splitting load telemetry out of the lease** into its own small item,
  which would cut the cost of every consistent lease scan by about two
  thirds.
- **Replacing the capacity sample with a query.** The sample stays in the
  bucket; a table item could not hold it past a few hundred nodes.
- **One shared lease view per node**, so the several loops that scan the
  leases share one read, and lease shards past a few hundred nodes.
- **Switching the deploy pointers in one transaction.**
- **The wake index**, which keeps its bucket protocol of immutable entry
  names and retirement watermarks.
- **Release qualification against real DynamoDB**, beside the R2 release
  tests.

## Decisions

- **The bucket stays the default and the only required store.** The
  table adds an availability dependency and is AWS-only; fleets on R2,
  GCS, Azure and Tigris are unaffected.
- **Route by key inside `Bucket`, not through a typed interface.**
  Revision 1 proposed a `ControlStore` enum and moving every caller behind
  it. More than a dozen modules and several operator commands build these
  keys, and a missed one would silently read an empty copy of a record
  and split the fleet. Routing at the one client every caller already
  holds covers them all, including future ones, leaves bucket fleets
  byte-for-byte unchanged, and keeps the change small enough to review.
- **Store the JSON body verbatim.** The records keep their wire formats,
  so every reader works unchanged, mixed-version field preservation
  survives, and a future migration is a byte copy.
- **Random version tokens rather than counters.** A token the writer
  generated resolves an ambiguous write exactly on readback.
- **No AWS SDK.** Signing and credentials come from `object_store`, which
  celld already uses for S3.
- **A 4xx is not committed.** DynamoDB applies nothing it refuses, which
  keeps a throttled renewal from costing a readback.
- **The capacity sample, wake index and peer-auth secret stay in the
  bucket.** None is on a latency-critical path that the table improves
  today, and each has its own reason to stay: the sample's size, the wake
  index's protocol, and the secret's write-once use.

## Open questions

- Do facet scopes have owner records of their own, and should deleting a
  facet delete its owner item?
- How should a running table node notice that `fleet/control.json` was
  removed?
- Should the table require the bucket and the table to share a region, or
  only warn?
- Is a strongly consistent `Query` plus a `GetItem` per lease worth
  collapsing into the `Query`'s own bodies for the scans that do not judge
  lease expiry?
