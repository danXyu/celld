# Application previews

A preview is a private copy of your application, running on its own fleet
with its own storage, at a stable URL. Use one to try a pull request, to
show a change to someone, or to reproduce a bug. A preview lives for a day
by default and then cleans itself up.

A preview starts empty. To reproduce a problem that depends on real data,
**seed** the preview: copy a few Durable Objects, with their SQLite and KV
data, from an approved source fleet such as production. The copies are
independent. Nothing the preview does reaches the source.

This guide covers creating, updating and deleting previews, seeding them,
and the one-time platform setup that previews and seeding need.

## How previews work

Previews run on Kubernetes under the
[celld operator](https://github.com/ewhauser/celld-operator). Four resources
take part:

- **Parent fleet.** An existing `CelldFleet` that a platform administrator
  has enabled for previews. It names the bucket that previews store their
  data in, and the source fleets that previews may seed from.
- **Preview.** A `CelldPreview` resource. It is the only resource a
  developer creates. `celld preview` writes it for you.
- **Child fleet.** The operator creates a fleet for each preview, under its
  own prefix in the parent's preview bucket, and routes the preview URL to
  it. The preview owns it, so deleting the preview removes it.
- **Seed executor.** A platform process, `celld preview seed --watch`, that
  copies the selected objects from the source fleet into a new child fleet
  before the child starts serving.

When you run `celld preview`, the command:

1. Builds your Wrangler project.
2. Creates the `CelldPreview`, or updates it if it already exists.
3. Waits for seeding to finish, if you asked for it.
4. Publishes the build to the child fleet's storage.
5. Waits until the operator reports that every node of the child fleet
   runs exactly this build, and the URL answers without a server error.
6. Prints the URL.

Kubernetes reporting the preview `Ready` does not mean your code is live.
The command waits for the operator to observe the version it published,
so when the URL prints, the URL serves your build.

## Before you start

You need:

- `kubectl` on your `PATH`, signed in to the cluster. `celld preview`
  always takes an explicit `--context` and never changes your current
  context.
- A parent fleet with previews enabled. Ask your platform administrator for
  its name and namespace. If previews are not enabled, the command fails
  with `parent fleet has not enabled previews`.
- Kubernetes permission to get, create, update and delete `CelldPreview`
  resources in the namespace, and to read the parent and child fleets.
- Object-store credentials, from the standard AWS credential chain, that can
  write the parent's preview bucket. The command publishes your build there
  directly.

To seed a preview you also need the name of an approved source (for
example `production`) and a platform administrator running the seed
executor; see [Setting up previews](#setting-up-previews).

## Quick start

Create a preview of the project in the current directory:

```sh
celld preview pr-42 \
  --context development --namespace previews --fleet development
```

The command prints the preview URL when the build is live. The first run
takes longest, because the operator starts the child fleet.

Push a new commit and run the same command again to update the preview. The
URL and the data stay the same:

```sh
celld preview pr-42 \
  --context development --namespace previews --fleet development \
  --revision "$(git rev-parse --short HEAD)"
```

Check on a preview, or delete it early:

```sh
celld preview status pr-42 --context development --namespace previews
celld preview delete pr-42 --context development --namespace previews
```

`status` prints the name, phase and URL; `--json` prints the whole
`CelldPreview` resource. `delete` asks the operator to shut the preview down
and clean up. It does not delete fleet objects itself.

## Creating and updating a preview

The preview name is a Kubernetes resource name: lowercase letters, digits
and hyphens. A good name says what the preview is for: `pr-42`,
`reproduce-cart`, `demo-checkout`.

Running `celld preview NAME` creates the preview if it does not exist and
updates it if it does. An update deploys the new build and keeps everything
else: the URL, the stored data, the lifetime and the seed. The built
content decides the deployment version, so running the command twice
without a change deploys nothing new. `--revision` is a label that shows up
on the resource; it does not choose what gets deployed.

Some settings are fixed when the preview is created:

- **Lifetime.** A preview expires 24 hours after it was created, or after
  `--ttl-seconds`, from 60 seconds to 7 days. Redeploying does not extend
  it. To keep working after it expires, create a new preview.
- **Seed.** A preview can be seeded only when it is created, and only once.
  Its seed source, objects and alarm policy never change.

On an update, leave `--ttl-seconds` and the seed options out, or repeat
their original values. A different value fails with an `immutable` error.

If the wait runs past `--timeout-seconds` (10 minutes by default), the
command fails but leaves the preview in place. Run `status` to see where it
is, or run the same deploy command again to resume waiting.

### Options

| Option | Default | Meaning |
|---|---|---|
| `--context CONTEXT` | required | The `kubectl` context to use. |
| `--namespace NS` | `default` | The namespace of the preview and parent fleet. |
| `--fleet FLEET` | required | The parent fleet. |
| `--config PATH` | current directory | Wrangler project directory or config file. |
| `--source TEXT` | preview name | A label for where the code came from. |
| `--revision TEXT` | none | A label for the code revision. |
| `--ttl-seconds N` | `86400` | Lifetime from creation, 60 to 604800. Fixed at creation. |
| `--seed-from ALIAS` | none | Seed from this approved source. Creation only. |
| `--object CLASS:ID` | none | An object to seed. Repeat for up to 100. |
| `--alarms Clear\|Preserve` | `Clear` | Whether seeded objects keep their scheduled alarms. |
| `--timeout-seconds N` | `600` | How long to wait for the preview to serve the build. |
| `--dry-run` | | Print the `CelldPreview` and stop. |
| `--json` | | Print the result as JSON. |

`--dry-run` builds nothing, changes nothing in the cluster and touches no
storage. It prints the resource the command would create:

```sh
celld preview reproduce-cart --dry-run \
  --context development --namespace previews --fleet development \
  --seed-from production \
  --object Cart:8a1f...e2 --object Customer:41c9...07
```

```yaml
apiVersion: celld.eric.dev/v1alpha1
kind: CelldPreview
metadata:
  name: reproduce-cart
  namespace: previews
spec:
  fleetRef:
    name: development
  source: reproduce-cart
  ttlSeconds: 86400
  seed:
    source: production
    alarms: Clear
    objects:
      - class: Cart
        id: 8a1f...e2
      - class: Customer
        id: 41c9...07
```

The output is JSON, which is also valid YAML, so you can save it and
`kubectl apply` it yourself.

## Seeding a preview

Seeding copies selected Durable Objects from a source fleet into a new
preview before the preview serves its first request. Use it to reproduce a
bug against the exact state that caused it.

### Pick the objects

Name each object as `Class:ID`, where `Class` is the Durable Object class
name and `ID` is the object's **canonical ID**: the 64-character hex string
that `id.toString()` returns. It is not the name you pass to `idFromName`.
To find IDs, list the cells of a class in the source fleet's bucket:

```sh
celld cell list Cart --bucket s3://acme-production
```

Or log `id.toString()` from your application where it resolves the object.

Select every object the bug needs, up to 100. If a cart refers to a
customer, seed both; a reference to an object you did not seed resolves to
an empty object in the preview.

Deploy the same code, or at least the same Worker name and the same
class-to-binding mapping, as the source. The seeded data belongs to a
class name; a preview that renames the class cannot find it.

### Create the seeded preview

```sh
celld preview reproduce-cart \
  --context development --namespace previews --fleet development \
  --seed-from production \
  --object Cart:8a1f...e2 --object Customer:41c9...07
```

The command waits while the executor copies the objects, then deploys and
prints the URL as usual. If seeding fails or is canceled, the command stops
and prints the preview's status. The preview cannot be reseeded; delete it
and create another.

### What gets copied

Each object comes over with:

- its application SQLite tables,
- its KV storage,
- the facets it embeds,
- its alarms, if you pass `--alarms Preserve`.

The default, `--alarms Clear`, removes every alarm, including those of
facets, so a seeded object does not run scheduled work on its own. Choose
`Preserve` when the bug involves an alarm.

Nothing else is copied: no leases, deployment configuration, credentials,
node logs, or objects you did not select. The source fleet is only read.

**Seeded data is real data.** It can contain customer records, tokens and
anything else your application stores. The administrator who approves a
source alias grants access to that data to everyone who can create
previews.

### How current the copies are

Each object is copied from its latest **persisted checkpoint** in the
source bucket, one object at a time. So:

- A write that the source node has acknowledged but not yet flushed to the
  bucket may be missing.
- Two objects can come from slightly different moments. The selection is
  not one atomic snapshot across objects.
- An object with no checkpoint in the bucket fails the seed. The executor
  never creates an empty object in its place.

This is a debugging tool, not a backup. Do not use it for backup or
disaster recovery.

### Limits

- 1 to 100 objects per preview, each named once.
- Each object's checkpoint, and its expanded SQLite image, can be at most
  256 MiB.
- System classes cannot be seeded.
- The source fleet and the preview must use different buckets.

## Setting up previews

This section is for the platform administrator who enables previews on a
fleet and runs the seed executor.

### Requirements

- The operator with the `CelldPreview` API from
  [celld-operator PR #52](https://github.com/ewhauser/celld-operator/pull/52).
- A celld runtime image that includes seeded-preview restore on every node
  of every preview fleet. An older image ignores the seed and serves empty
  objects instead; see [Under the hood](#under-the-hood).

### Enable previews on a parent fleet

Configure preview storage and routing in `spec.previews` of an existing
`CelldFleet`. The operator gives each preview its own prefix,
`p-<preview UID>`, in the preview bucket. `celld preview` reads the bucket,
prefix, region and endpoint only from these operator resources. It ignores
`CELLD_BUCKET`, `S3_ENDPOINT` and AWS endpoint overrides in the developer's
environment, so a developer's shell pointed at production cannot redirect a
preview deploy.

### Approve seed sources

To allow seeding, set the executor and list the source fleets on the parent:

```yaml
spec:
  previews:
    seeding:
      executor: celld-snapshot-v1
      sources:
        - name: production        # the alias developers pass to --seed-from
          fleetRef:
            namespace: prod
            name: production
            uid: <the production CelldFleet's UID>
```

The UID pins the approval to that fleet. If the source fleet is deleted and
recreated, seeding refuses until you approve the new one.

Treat each source alias as a data-access grant: anyone who can create a
preview on this parent can copy any object of that source.

### Run the seed executor

Run one supervised process per namespace that holds previews:

```sh
celld preview seed --watch --context development --namespace previews
```

Every ten seconds it looks for seed requests in that namespace, claims one,
and processes claimed requests one at a time. Running more than one watcher
is safe: Kubernetes lets only one of them claim each request. To process
one request by hand instead, give its reservation name:

```sh
celld preview seed s3-scope-<hash> --context development
```

The executor needs:

- **Kubernetes.** On `CelldStorageReservation`: get, list and watch, and get
  and patch on the status. On `CelldFleet` and `CelldPreview`: get, in the
  preview namespace and in each source namespace. The operator repository
  has an example role. Developers need none of these permissions.
- **Object storage.** From the standard AWS credential chain: read on the
  source bucket, and read and create on the preview bucket. The executor
  never reads Kubernetes Secrets.
- **Network** access to both object-store endpoints.

### What the executor checks

Before it copies anything, and again between objects, the executor checks
that the preview, child fleet, parent fleet, source fleet and reservation
are the same resources the request named; that the source is still
approved; that the child fleet has not started; and that the preview has
not expired or been canceled. If any check fails, it stops.

It copies in two passes. First it captures a snapshot of every selected
object and records the complete list, with a digest of each, in the
reservation. Only then does it import the snapshots into the preview's
storage. It checks for cancellation only between complete object
transfers, never in the middle of a storage write.

### When a seed gets stuck

If the executor crashes, or a storage or Kubernetes call fails with an
uncertain result, the reservation stays `Running`. Nothing retries it
automatically: a restarted executor, or a second watcher, will not take
over a `Running` claim, because the first writer might still be writing.

To recover, make sure the old executor process is gone and no storage
request it made can still land. There is no force-resume command. Delete
the preview and have the developer create a new one.

Snapshots and reservations are kept after a seed, whether it succeeded or
not. Nothing garbage-collects them yet.

## Under the hood

This section describes how a seed reaches the preview runtime. You do not
need it to use previews.

The executor drives `celld::preview_seed`, which does the storage work:

1. **Check the destination is unopened.** The preview's prefix must hold no
   fleet or application objects, and the operator keeps the runtime from
   starting.
2. **Capture.** For each selected object, restore its epoch chain from the
   source's persisted LTX files, as of the latest checkpoint, and clean the
   SQLite image. Replication control tables and wake epochs are removed;
   alarms are removed if the policy is `Clear`. The result is written as an
   immutable snapshot under `preview-snapshots/<reservation UID>/` in the
   preview bucket. Each entry records the class, the ID, the snapshot ID,
   the source version (`ltx:eN:txid:N`) and the SHA-256 digest of the
   payload.
3. **Pin.** The executor records the full manifest in the reservation
   before importing anything.
4. **Import.** Each pinned snapshot is written into the preview's prefix as
   a complete LTX snapshot at the reserved **epoch zero**, TXID one.
   Importing the same bytes twice is harmless.

Epoch zero is never a writer epoch. When the preview fleet first activates
a cell, it looks for this bootstrap snapshot. If it finds one, it restores
the cell from it through the normal epoch-chain path, and the first writer
then takes epoch one as usual. From there on, ownership and recovery work
as in any fleet. The lookup adds one object-store `HEAD` to each fresh
activation. A storage error fails the activation instead of serving an
empty cell. Before serving, the fleet's normal wake-index initialization
finds the seeded cells, including their preserved alarms.

A runtime image without this change does not look for the bootstrap
snapshot, so it would serve seeded cells as empty. Every preview fleet must
run an image that includes it.

The storage API itself takes no Kubernetes authority and fences no other
writer; that is the executor's job. Never call it on a serving fleet, and
never retry an operation that ended uncertainly. A capture that failed may
already have written its snapshot, and cannot be captured again under the
same operation.

## Testing

Rust tests cover object
selection, immutable settings, storage and ownership binding, waiting for
the loaded version, claim exclusion, and the capture, import and restore
path, including KV and SQL data, both alarm policies, digest checks and a
fresh runtime activation restoring a seed. `cargo test -p celld --lib
preview_seed` runs the storage tests against an in-memory object store.

A local integration run in Kind, with MinIO for storage, exercised real CRD
admission and status updates, seeding two objects into a separate prefix,
restore by the native runtime, both alarm policies, source isolation, the
background executor, and `create`, `update`, `status` and `delete`. It also
checked that a redeploy waits for the new version to be observed, and that
misleading bucket and endpoint variables in the shell are ignored.

Not yet qualified: public Ingress or Gateway routing, DNS and TLS, cloud
IAM, and a full rollout of the operator and CLI.
