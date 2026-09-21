<!--
SPDX-FileCopyrightText: The RamenDR authors
SPDX-License-Identifier: Apache-2.0
-->

# rtalur dev-loop notes

My personal, living notes for working fast in this repo. Not authoritative project
docs — see `CLAUDE.md` and the official docs for that. This file tracks **how I
iterate**: the tools, the inner loop, and the checkpoint philosophy behind them.

Maintained as work lands. Last updated: 2026-06-07.

## Philosophy: many cheap checkpoints

A fast dev loop is not only about shorter runs — it is about **many checkpoints**,
so re-running work is cheap, idempotent, and resumable. Prefer tools and stages
that:

- **Check reality first**, act only if reality is wrong, then verify.
- Are safe to re-run at any time (reconcile, not replay).
- Resume by simply running again — no manual "undo" or cleanup between attempts.

This is the design principle behind `drenv-go` (below) and the lens I apply to
every other part of the loop.

## The inner loop

The four areas I iterate in, fastest to slowest feedback:

| Area | Tool | What it gives me |
|------|------|------------------|
| Unit tests | `make test-*` (Ginkgo focus) + envtest | Logic correctness, seconds–minutes |
| Run controllers locally | `localrun` (`go run ./localrun ...`) | Real reconcilers vs. a live env, ~seconds to rebuild |
| Test environment | `drenv` (Python, today) / `drenv-go` (in progress) | Multi-cluster env to run against |
| End-to-end | `e2e/` module (`make e2e-rdr`) | Full failover/relocate behavior |

### Test environment — drenv / drenv-go

- **Today:** `cd test && drenv start envs/regional-dr.yaml` (Python).
- **In progress:** `drenv-go`, a parallel Go rewrite using the ensure model.
  Reuses the same envfiles (`test/envs/*.yaml`). Re-running `start` is cheap and
  only fixes what's missing. See
  `docs/superpowers/specs/2026-06-07-drenv-go-design.md`.

### Run controllers locally — localrun

- One native process per (cluster, reconciler) pair; ~4s edit→run loop, no images.
- `go run ./localrun configure --envfile <f>` once, then `go run ./localrun run`.
- Narrow to what I'm changing: `--reconcilers vrg,pvrgl`.
- `go run ./localrun refresh` = kill + clear logs + rebuild + restart.
- Logs in `localrun/logs/<cluster>-<reconciler>.log`.

### Unit tests

- Focused suites: `make test-vrg`, `make test-drpc`, `make test-drcluster`, …
- Single spec: `go test ./internal/controller -ginkgo.focus "<desc>"`.

### End-to-end

- `make e2e-rdr` (needs 3 standing clusters with operators deployed).
- Single scenario: `cd e2e && ./run.sh -test.run 'TestDR/<name>'`.

## drenv-go parity status

Parallel Go implementation of `drenv`. Reflects state as of Milestone 4D.
(✅ done and unit-tested · 🚧 unit-tested, NOT cluster-validated · ⬜ not started / out-of-scope)

### Core infrastructure

| Piece | Status | Notes |
|-------|--------|-------|
| Module skeleton (mage, tools.mod, cobra, ensure pkg, envfile parser) | ✅ | |
| envfile: parse profiles including `external: true` | ✅ | `Profile.External bool` added M4D |
| minikube provider + cluster lifecycle (start / stop / delete / status) | 🚧 | `start`/`status` cluster-validated (hub+dr1+dr2 on vfkit/containerd); stop/delete still FakeRunner-only. `status` now isolates the JSON object from minikube's interleaved stderr and treats `Kubeconfig: Misconfigured` (interrupted start, context missing from kubeconfig) as not-Running so `start` reconciles it instead of leaving every addon failing with `context "<name>" does not exist` |
| minikube `start` flag parity (container_runtime, extra_disks, disk_size, cni, nodes, extra_config, feature_gates, service_cluster_ip_range, rosetta, wait-timeout, dns-servers) | ✅ | `Profile`/`Template` embed `MinikubeSpec`; flags emitted in Python's order. Fixes minikube defaulting to docker instead of `container_runtime: containerd` |
| minikube DNS bypass on managed Macs (`--dns-servers`, #2786) | 🚧 | `minikube_dns.go` ports `dns.servers()` (auto/static/host modes, VM-driver gate → public 8.8.8.8/1.1.1.1) and `is_managed_mac()`/`systemextensionsctl` parsing. `MinikubeProvider.DNSMode` defaults to `auto`; detection runs only in auto mode and never shells out off Darwin. Unit-tested (mode selection, extension parsing, detection, emitted flag) |
| `$vm`/`$container`/`$network` placeholder resolution | ✅ | `resolvePlatform` maps placeholders to per-host driver/network (vfkit/vmnet-shared on macOS, kvm2/default on linux-amd64), matching Python's `_PLATFORM_DEFAULTS` |
| per-node containerd config (`ContainerdConfigStep`) | ✅ cluster-validated | post-start `minikube cp`+TOML-merge+`ssh restart containerd`; idempotent Done; mirrors `_configure_containerd` (minus the registry cache). cp/ssh round-trip validated on vfkit dr1/dr2 |
| external provider (ExternalProvider) | 🚧 | No-ops for lifecycle; Status probes `/readyz` via kubectl; unit-tested M4D |
| Per-profile provider selection (`provider.For`, `build.ProviderSelector`) | ✅ | External profiles get ExternalProvider; normal profiles get MinikubeProvider |
| suspend / resume / load-image | 🚧 | Unit-tested; delegates to provider per profile |
| `cli.Kubectl` wrapper (Apply, ApplyKustomizeDir, WaitFor, WaitCondition, GetRaw, ClusterInfoDump, …) | ✅ | All methods argv-tested |
| Addon-execution framework (registry, ensure integration, parallel workers) | ✅ | |
| Reality-gated addon re-runs (`ensure.NewGatedGroup` + per-addon readiness probe) | ✅ | A satisfied addon reports `addon/X: skipped, already satisfied` and runs nothing, instead of replaying every apply/wait/rollout. **All workload addons are gated**, including the multi-phase ones with thorough multi-condition probes: rook-cluster (CephCluster Ready + CSI components + CSIAddonsNodes Connected), rook-pool (pool Ready + peer token), rook-cephfs (filesystems Ready), ocm-cluster (ManagedCluster Available + addon deployments), argocd (server Available + cluster secrets), submariner (broker + member deployments), rbd-mirror (CephRBDMirror Ready + mirroring healthy). Only recipe stays ungated (apply-only, already cheap). Probes are conservative — any uncertainty reports not-ready, and a post-Do latch means a gate can never wrongly fail or skip needed work. |

### Commands

| Command | Status | Notes |
|---------|--------|-------|
| `start` | ✅ | **Full regional-dr cluster-validated** (hub+dr1+dr2 on vfkit/containerd): brings the whole environment up end-to-end (rook, ocm hub+cluster join, submariner, rbd-mirror, volsync, …), exit 0; re-run skips all satisfied addons via the reality gates |
| `stop` | 🚧 | Parallel ensure; unit-tested |
| `delete` | 🚧 | Parallel ensure; unit-tested |
| `status` | ✅ | Cluster-validated against the real minikube binary (reads running/stopped/not-found per cluster) |
| `load` | 🚧 | Per-profile; unit-tested |
| `suspend` | 🚧 | Per-profile; unit-tested |
| `resume` | 🚧 | Per-profile; unit-tested |
| `dump` | ✅ | YAML marshal of expanded env; trivially correct |
| `gather` | ✅ | Uses `kubectl cluster-info dump --all-namespaces --output=yaml` per profile; argv-tested |
| `cache` | ⬜ | Out of scope: drenv-go applies kustomizations via `kubectl -k` directly, so the Python kustomize-build cache (pre-downloading manifests) is unnecessary. Not reimplemented. |

### Not reimplemented (honest)

| Feature | Decision |
|---------|----------|
| lima provider | **Dropped**: explicitly out of scope for drenv-go. Only minikube + external. |
| odf-external-snapshotter addon | **Dropped**: removed upstream (commit 2bfa5daf) when the external snapshotter switched to the public VolumeGroupSnapshot API. The ported Go builder was deleted as dead code. |
| registry-cache / host-setup / cleanup | **Out of scope**: these are drenv host-infra features (local Docker registry, host `/etc/hosts`, DNS). drenv-go applies kustomizations directly; the kustomize-build cache is unnecessary. Host-infra setup is intentionally not reimplemented. |

### Addon parity sub-table

Every regional-dr addon has been ported and unit-tested (argv-level). The full
regional-dr stack is now validated end-to-end on the real 3-cluster env
(hub+dr1+dr2, vfkit/containerd): `start` brought every addon below to ✓ and a
re-run skipped all 18 gated addons as "already satisfied", exit 0.

**Cluster validation environment:** vfkit works on this host after all (the
earlier qemu2 fallback notes are obsolete). The whole regional-dr env comes up
host-routable, so the NodePort/Service addons (minio `mc`, velero, argocd) that
qemu-builtin couldn't reach now complete.

| Addon | Registered name | Status |
|-------|----------------|--------|
| external-snapshotter | `external-snapshotter` | ✅ cluster-validated (regional-dr) |
| olm | `olm` | ✅ cluster-validated (regional-dr) |
| recipe | `recipe` | ✅ cluster-validated (regional-dr) |
| csi-addons | `csi-addons` | ✅ cluster-validated (regional-dr) |
| ocm-controller | `ocm-controller` | ✅ cluster-validated (regional-dr) |
| minio | `minio` | ✅ cluster-validated (regional-dr): apply + rollout + `mc` bucket setup all complete (vfkit is host-routable) |
| velero | `velero` | ✅ cluster-validated (regional-dr) |
| volsync | `volsync` | ✅ cluster-validated (regional-dr): helm install + rollout on dr1/dr2 |
| ocm-hub | `ocm-hub` | ✅ cluster-validated (regional-dr): clusteradm init + hub deployments |
| ocm-cluster | `ocm-cluster` | ✅ cluster-validated (regional-dr): managed-cluster join to hub |
| submariner | `submariner` | ✅ cluster-validated (regional-dr): broker deploy + dr1/dr2 join |
| argocd | `argocd` | ✅ cluster-validated (regional-dr). **Fix:** apply now uses `--server-side=true --force-conflicts=true` — client-side apply overflowed the 262144-byte annotation limit on the `applicationsets.argoproj.io` CRD, and force-conflicts lets the server-side apply take over field ownership left by any prior client-side apply |
| rook-operator | `rook-operator` | ✅ cluster-validated (regional-dr). **Fix:** now applies the two `start-data/{deps,operator}` kustomizations (the `operator/` dir has no kustomization) and waits the CSI CRDs established + `ceph-csi-controller-manager` rollout between them, mirroring Python |
| rook-cluster | `rook-cluster` | ✅ cluster-validated (regional-dr). **Fix:** rewrote the CSI waits for the Rook 1.20 CSI-operator layout — poll the `*-ctrlplugin` deployments for ceph monitors (via `exec -c <plugin>`) and wait the correctly-named `CSIAddonsNode` resources (with retry), replacing the stale `daemonset/csi-rbdplugin` rollout that never existed |
| rook-toolbox | `rook-toolbox` | ✅ cluster-validated (regional-dr) |
| rook-pool | `rook-pool` | ✅ cluster-validated (regional-dr) |
| rook-cephfs | `rook-cephfs` | ✅ cluster-validated (regional-dr). Applies a `VolumeGroupSnapshotClass` after the snapshot class (consistency-group parity, #2739) |
| rbd-mirror | `rbd-mirror` | ✅ cluster-validated (regional-dr): CephRBDMirror Ready + pool mirroring healthy on dr1/dr2. Creates a `VolumeGroupReplicationClass` per interval alongside the VRC loop (consistency-group parity, #2739) |

### Known cluster-validation TODOs (from code review)

All of the items below have now been exercised by a real regional-dr run
(hub+dr1+dr2, vfkit/containerd) and are ✅ resolved. Kept here as a record of
what the code review flagged and how each held up on a live cluster:

1. **submariner: broker-info CWD** — ✅ resolved. `subctl deploy-broker` writes `broker-info.subm` into the working directory; the Go port runs deploy-broker then `os.Rename`s it to the deterministic path. The CWD-write + rename round-trip worked on the live run.

2. **argocd: temp-kubeconfig** — ✅ resolved. The temp-kubeconfig creation and `argocd cluster add` (with exit-20/"NOAUTH" suppression via the `OutputEnv` runner seam) completed against the live argocd. Separately, the apply itself needed `--server-side=true --force-conflicts=true` (see the addon table above).

3. **rbd-mirror: daemon-restart-on-timeout** — ✅ resolved. `waitRBDMirroringHealthy` reached healthy mirroring on dr1/dr2 without needing the retry/restart path this run; the retry logic (restart `deploy/rook-ceph-rbd-mirror-a` between attempts) remains as a safety net and is unit-tested.

4. **ocm: namespace-create waits** — ✅ resolved. The ocm-hub / ocm-cluster namespace-create waits (`open-cluster-management`, `open-cluster-management-hub`) completed within their timeouts; the managed clusters joined the hub.

5. **per-node `containerd` plugin config** — ✅ resolved. `provider.ContainerdConfigStep` (`minikube cp` config.toml out → TOML deep-merge of the profile block → copy back → `minikube ssh sudo systemctl restart containerd`) ran on vfkit dr1/dr2; the `device_ownership_from_security_context: true` setting rook needs was present and rook came up HEALTH_OK. The step is idempotent (Done skips when the block is already present).

## Staying in sync with Python drenv (drift check)

The Python `drenv` and `drenv-go` will live side by side for a long time, and
fixes or enhancements from other teams often land **only** in the Python
project. To make sure a lag in drenv-go is impossible to miss:

- `parity.lock` freezes a sha256 of every upstream (`../drenv`) file this port
  mirrors — derived automatically from the `.py` paths the Go source cites, plus
  every asset under each ported addon's directory, plus a small explicit
  supplement (`parityExtraSources` in `parity.go`) for modules named only in
  prose.
- `make parity` fails and names exactly what changed: **DRIFTED** (upstream file
  edited since last sync), **NEW**/**REMOVED** (tracked set changed), and
  **UNPORTED** (a new upstream addon with no drenv-go reference). Pure file
  hashing — no network or cluster.
- `make parity-update` re-baselines `parity.lock`. Run it **only after**
  reconciling drenv-go with the upstream change — it is the explicit "I reviewed
  this" acknowledgement, and shows up as a reviewable diff.
- CI (`.github/workflows/drenv-go.yaml`) runs `make parity` (plus lint/test/
  build) on any push/PR touching `test/drenv/**` or `test/drenv-go/**`, so a
  Python-side change that isn't reconciled here fails the check.

## Conventions I follow here

- Branch: `main`, push to `rtalur-github`. No PRs.
- Commits: small, modular, one logical change each. `git commit -s`.
  End messages with `Assisted-by: Claude Code/<model-id>`.
- New Go tooling: thin `Makefile` → `magefile.go`, pinned tools in `tools.mod`.
