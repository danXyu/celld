---
title: celld fork documentation
description: Bug fixes, additional features, and release guidance for ewhauser/celld.
tableOfContents: false
hero:
  title: celld, with fork-specific fixes and features
  tagline: Recovery safeguards, Kubernetes previews, and OTLP metrics for the ewhauser/celld fork.
  actions:
    - text: Install the fork
      link: ./install/
      icon: right-arrow
    - text: Upstream documentation
      link: https://celld.dev/docs/
      variant: secondary
---

This site documents **what changes in [ewhauser/celld](https://github.com/ewhauser/celld)**.
For getting started with celld, Workers APIs, storage configuration, and ordinary
fleet operation, use the [main celld documentation](https://celld.dev/docs/).

## What the fork adds

| Area | Changes | Documentation |
| --- | --- | --- |
| Recovery | Keep unavailable witnesses undecided; bind recovery requests to members and disks; handle replacement disks. | [Recovery fixes](./fixes/#recovery-witnesses) |
| Idle fleets | Detect failed follower probes and reconfigure the ensemble without waiting for writes. | [Idle-follower fix](./fixes/#idle-follower-failures) |
| Actor storage | Roll back transactions left open by an aborted actor. | [Transaction fix](./fixes/#aborted-actor-transactions) |
| Development previews | Deploy isolated previews and seed them from approved persisted object checkpoints. | [Preview workflow](./fork/previews/) |
| Observability | Export node gauges and cell CPU and heap distributions through OTLP. | [Metrics](./fork/metrics/) |
| Change export | SQLite change records and warehouse delivery components under development. | [Implementation status](./fork/export/) |

## Releases and compatibility

The documented release baseline is **v0.6.0-ewhauser.2**, based on upstream
**v0.6.0**. Fork builds are published as prereleases. Development pages can describe
work on main that is not yet in a release; change export is one such feature.
Check the [release notes](./fork/releases/) and the
[published artifacts](https://github.com/ewhauser/celld/releases) before upgrading.

The move from 0.5.1 fork builds to 0.6.0 requires a full stop for fleets using
peer-disk durability. [Install and upgrade](./install/) covers the boundary.

## Companion projects

The [celld operator](https://ewhauser.github.io/celld-operator/) manages Kubernetes
fleets and preview resources. [celld-tck](https://ewhauser.github.io/celld-tck/)
publishes compatibility and recovery test results. Check the exact version and
commit behind a result when evaluating a release.
