# Fork builds

The operator's strict disk-removal contract requires this fork. Stock celld
v0.5.1 does not provide it. The first fork build identifies itself as
`0.5.1-ewhauser.1`, including in `celld --version`.

The release workflow builds native Linux x86_64, Linux aarch64 and macOS aarch64
binaries. Each workflow artifact contains the compressed executable and a JSON
record of its source repository, commit, version, target and SHA-256 digest.
After all native builds and the container smoke build pass, the workflow creates
a draft prerelease with the same artifacts, `SHA256SUMS`, and build attestations.
A release tag that already identifies a different commit is rejected.

Run the candidate build from an exact reviewed branch or tag:

```sh
gh workflow run release.yml --repo ewhauser/celld --ref <reviewed-ref>
```

Download the workflow artifacts with `gh run download`, or the draft assets
with `gh release download v0.5.1-ewhauser.1 --repo ewhauser/celld`. Verify the
checksums and source commit before using them. The macOS binary is a command-line
build without Apple signing or notarization.

Publishing the verified draft starts the existing container phase. It builds
and tests Linux amd64 and arm64 images at the release tag and publishes them to
`ghcr.io/ewhauser/celld`, including a multi-architecture manifest and provenance.
Fork prereleases never update `latest`. The operator must pin the verified
manifest digest; per-platform digests can be used for platform-specific tests.
Do not substitute an upstream image or infer a digest from a tag name.

All members and potential recovery processes must use a compatible fork build
before removing a last follower: recovery must understand the `bucket_complete`
proof. Artifact publication alone does not qualify EKS/EBS removal. Validate the
strict API and launcher handshake against the exact binary/image being used.

## 0.5.1-ewhauser.3: unavailable recovery witnesses

Fleet recovery no longer treats an unreachable follower as conclusively lost
because its lease expired more than three lease lifetimes ago. This could seal
a predecessor log and declare permanent loss while acknowledged writes still
existed on a retained disk whose process was starting later than its peer.

A missing address or failed seal request now keeps that member undecided.
Without another complete witness or an existing `bucket_complete` proof,
recovery refuses to seal, write a loss record, or replace the predecessor lease.
Startup keeps serving authenticated follower seal/tail requests during its
existing bounded retry ladder. A witness that returns within that ladder can
complete recovery; a permanently unreachable witness prevents startup instead
of turning an unknown disk state into data loss. Retry counts and deadlines
are unchanged.

The existing explicit-loss policy for reachable members reporting missing or
incomplete fragments is unchanged. This fix does not repair a predecessor
already sealed with a loss record by an older build. All potential recovering
members must run the corrected build before relying on the new behavior.

## 0.5.1-ewhauser.5

### Node-log state in `/state`

The internal `GET /state` response gains a `node_log` object, documented in
the [README](README.md#shut-down-and-roll-out-a-node). It reports this node's
durability posture, its log session and folded log, whether its shipper is
healthy, and the result of the last dead-leader sweep. The sweep result lists
every unsealed log whose lease has expired, and, for each ensemble member, the
leader sessions whose current epoch still needs that member's fragment. An
operator can gate a voluntary disruption on this fleet state without a bucket
client of its own.

The sweep already listed and read every node record; it now keeps its last
result in memory. The change adds no bucket requests, and `/state` makes none.
The bucket posture runs no sweep, so it reports `fleet: null`. A pass that
cannot list or read a record reports `complete: false`.

### Member and disk binding on recovery seal and tail

Before this build, a process that answered at a recovered member's address
with an empty follower store reported fragment epoch 0. Recovery treated that
answer as conclusive. With no complete witness and every member conclusive, it
wrote `log/<session>.e<epoch>.loss.json` and sealed the log. A replacement
machine that reused the node's name, and so its stable address, with a fresh
disk could therefore declare loss on the word of a disk that never held the
fragment, while the disk that did hold it was still retained.

Each follower store now keeps a random disk incarnation in
`<data>/peerlog/incarnation`, created and fsynced (file and directories) the
first time a process needs it, and read back after a restart. Each node
publishes it in its lease as the optional `disk_incarnation` field. Recovery
sends the member name and that incarnation with every seal and tail request
(`member` and `incarnation`, both optional). A follower with another name or
another incarnation refuses the request before it writes a seal mark, and
recovery counts the refusal as an undecided member, like an unreachable one.
The same binding applies to the tail reads that fold a quietly stranded cell.

0.5.1-ewhauser.7 narrows this refusal to another member name: the member's own
name on another incarnation answers from its replacement disk. See that section
for the deadlock the refusal caused.

The check protects only while the member's lease names a disk other than the
one answering, for example while a replacement is still recovering its own
predecessor. Once the replacement installs its lease, its disk is the member's
disk of record and the explicit-loss policy applies to its answer as before.
This is intentional. A disk that no longer exists cannot be recovered, so a
session whose only complete copy was on it gets a bounded loss record instead of
blocking recovery forever. Incarnations are therefore not recorded at
recruitment.
A startup whose incarnation file cannot be read fails instead of creating a
new identity.

Rollout: every field is additive and optional. Older requests and older lease
records deserialize with the fields absent and keep the unchecked behavior. The
check protects a recovery only when the recovering node, the answering
follower and the member lease that names the disk all come from this build.
Run it on every node that can recover or answer for this fleet before relying
on it.

## 0.5.1-ewhauser.6

### An idle leader moves off a departed follower

A leader that received no writes never left a follower that had gone away,
for example a pod deleted after a scale-in. The idle probe, an empty append
sent to each quiet member every 2 seconds, ignored a transport failure. It also
recorded the failed attempt as a completed append, so the gray-follower ledger
read a refused connection as a fast, healthy sample. Nothing degraded the
shipper. Maintenance therefore never drained the epoch to `bucket_complete` or
opened a new one without the member. The leader's current epoch kept naming the
departed member, and `/state.node_log.fleet.obligations` kept it as an
obligation, so a disk-removal gate never released that member's disk. Under
write load the first failed append degraded the shipper and the obligation
cleared, which is why only idle fleets showed the bug.

A failed probe now records a failure, not a latency sample. Three failed probes
in a row, with no answer between them, degrade the shipper the same way a failed
write does: acknowledgements wait for bucket proofs, and maintenance drains the
epoch to `bucket_complete` and opens a new one from the members whose leases are
live. An answer resets the count. The count tolerates a brief network fault
without opening a new epoch on a quiet fleet. A probe carries no
acknowledgement, so the tolerance never delays a degrade a write needs: a write
still degrades on its own first failure. The departed member leaves the
obligations about four seconds after its first failed probe, or after its lease
expires and a maintenance pass runs if the ensemble recruits it again before
then.

Safety does not change. Degrading only moves acknowledgements to the bucket
proof and starts the existing reconfiguration path.

## 0.5.1-ewhauser.7

### A replacement disk under the member's name answers recovery

0.5.1-ewhauser.5 made a follower refuse any recovery seal or tail whose
`incarnation` differed from its own disk, and recovery counted the refusal as
an undecided member. That deadlocked a fleet whose members lost their disks at
the same time. In a two-member fleet that lost both disks, each replacement
must recover its predecessor session before it installs its own lease. The
only ensemble member of that session is the other replacement, and that
member's lease still names its lost disk, because the other replacement is
blocked in the same step. Each follower refused the other's request with
`400 Bad Request`, each recovery failed with `no complete true witness ... 1
member(s) undecided`, and startup exited with `refusing to install a lease
over an unrecovered predecessor log`. No lease was replaced and no loss was
recorded, so the fleet never recovered.

A node name identifies exactly one disk at a time, and a new incarnation under
that name exists only because the named disk was replaced. The follower now
applies this rule:

- Another `member` name is refused, as before, and recovery counts the member
  as undecided.
- The same `member` name with another `incarnation` answers from the
  follower's own store and logs a warning that names the superseded
  incarnation. For an empty replacement disk the answer is a conclusive "no
  fragment". With no complete copy and every member conclusive, recovery
  writes `log/<session>.e<epoch>.loss.json` and seals, as it did before
  0.5.1-ewhauser.5.
- An `incarnation` without a `member` is still refused on a mismatch, and a
  request without an `incarnation` keeps the unchecked answer.

Tail reads, including the fold of a quietly stranded cell, follow the same
rule. The strict disk-removal path is unchanged. A three-member fleet that
loses two disks recovers each lost session from the survivor when it holds a
complete copy, and records the loss when it does not.

This reverses part of the 0.5.1-ewhauser.5 protection: a node name that runs on
a new disk now declares its old disk lost, even if that disk still exists
elsewhere. Reuse a node name on another disk only when the previous disk is
gone for good. `disk_incarnation` stays in the lease so that a follower can
report which disk it supersedes.

Rollout: no wire or record changes. A follower on an older build still refuses
a replacement's answer, so upgrade every node before relying on the fix.

## 0.6.0-ewhauser.1 (unreleased)

Based on upstream v0.6.0. It keeps every fork change through
0.5.1-ewhauser.7.

### Upgrading from 0.5.1-ewhauser builds needs a full stop

Upstream's rule for v0.5.1 to v0.6.0 applies. A fleet with `fleet` durability
must not roll from any 0.5.1-ewhauser build to this one. Stop every member,
then start every member on this build. A 0.6.0 node recovers its previous log
session only from a follower that returns the ranged tail format, and a 0.5.1
follower returns only the entries-only format, so a restarted member waits
for a witness the old members cannot give. A fleet with `bucket` durability
has no followers and can roll.

### How the fork changes sit on upstream v0.6.0

- An unreachable recovery witness stays undecided however long its lease has
  been expired (0.5.1-ewhauser.3). Upstream v0.6.0 still counts a member that
  is unreachable with a long-expired lease as conclusive; this build does not.
  Upstream's own rules also apply: an HTTP error, a legacy entries-only tail
  or a tail without complete range evidence leaves the member undecided.
- Seal and tail requests still carry the member name and disk incarnation
  (0.5.1-ewhauser.5 and .7). The ranged tail endpoint applies the same
  addressee check as the seal.
- Folding a quietly stranded cell before it is activated again now follows
  upstream: an epoch that may have fleet acknowledgements needs a complete
  follower tail. A log record the bucket already covers (`bucket_complete`)
  needs none, as before. Each tail request names the member's disk.
- Disk-removal coverage (`bucket_complete`) now waits for upstream's
  `all_fragment_rows_tiered`. It follows each stopped cell's retained tail
  until its uploads land, and replaces the fork's flag that stayed set once
  any cell had ended with an untiered tail.
