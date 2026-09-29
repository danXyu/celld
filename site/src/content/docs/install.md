---
title: Install and upgrade
description: Download and verify fork artifacts, select a container image, and plan a compatible upgrade.
---

Use artifacts from **ewhauser/celld**. The installer linked by the upstream site
installs upstream celld; it does not select this fork.

The examples below pin **v0.6.0-ewhauser.2**, a fork prerelease based on upstream
v0.6.0. Check [fork releases](https://github.com/ewhauser/celld/releases) for the
artifact you intend to deploy.

## Native binary

Published targets are Linux x86_64, Linux aarch64, and macOS aarch64. This example
downloads the Linux x86_64 binary with the GitHub CLI:

```sh
mkdir -p celld-fork-download
cd celld-fork-download

gh release download v0.6.0-ewhauser.2 --repo ewhauser/celld \
  --pattern 'celld-x86_64-unknown-linux-gnu.*' --pattern SHA256SUMS

# Verify the compressed asset before extracting it.
gh attestation verify celld-x86_64-unknown-linux-gnu.gz --repo ewhauser/celld
grep 'celld-x86_64-unknown-linux-gnu.gz$' SHA256SUMS | shasum -a 256 -c -

gunzip -c celld-x86_64-unknown-linux-gnu.gz > celld
chmod +x celld
./celld --version
```

Use `aarch64-unknown-linux-gnu` for Linux ARM64 or `aarch64-apple-darwin` for Apple
Silicon in place of `x86_64-unknown-linux-gnu`. The adjacent `.build.json` records
the source repository, commit, version, target, and SHA-256 digest. The macOS
binary is not signed or notarized by Apple.

Move the verified executable into a directory on your `PATH`. Worker projects
also need [esbuild](https://esbuild.github.io/) on `PATH`. Continue with the
[upstream configuration and deployment guide](https://celld.dev/docs/).

## Container image

```sh
docker run --rm ghcr.io/ewhauser/celld:0.6.0-ewhauser.2 --version
```

Fork prereleases do **not** update `latest`. The image is published for Linux
amd64 and arm64 after the verified draft release is published. Native artifacts
and container publication are separate phases.

For Kubernetes, pin the verified manifest digest in the operator's `runtimeImage`.
Use the [operator compatibility guide](https://ewhauser.github.io/celld-operator/reference/compatibility/)
to select a supported runtime/operator combination. Follow the upstream runtime
guide for bucket credentials, listeners, and persistent local storage.

## Upgrade boundaries

| Upgrade | Required action |
| --- | --- |
| 0.5.1-ewhauser builds → 0.6.0-ewhauser builds with `fleet` durability | Stop every member, then start every member on the new build. A mixed fleet cannot supply the ranged recovery tail required by 0.6.0. |
| 0.5.1 → 0.6.0 with `bucket` durability | A rolling upgrade is supported; this mode has no follower recovery dependency. |
| 0.6.0-ewhauser.1 → 0.6.0-ewhauser.2 | Nodes can roll; there is no record or storage format change. |

All potential recovery nodes must run compatible fork builds before you rely on
the recovery fixes. Reuse a node name on a replacement disk only when the prior
disk is permanently lost: the replacement can declare that old disk lost.

## Removed APIs

In 0.6.0-ewhauser.2, strict disk-removal shutdown and `/state.node_log` reporting
were removed. Shutdown requests carrying a `mode` parameter return HTTP 400
without stopping the process. Ordinary and preserve shutdown remain supported.

The `/state.shutdown` identity envelope retains `schema_version: 1` and
`runtime_generation`, and reports `strict_disk_removal: false`. Use the current
operator lifecycle guidance instead of procedures for the retired API. See the
[release history](../fork/releases/) for details.
