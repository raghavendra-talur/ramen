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
| minikube provider + cluster lifecycle (start / stop / delete / status) | 🚧 | Unit-tested via FakeRunner; clusters could not boot on this host (vfkit) |
| minikube `start` flag parity (container_runtime, extra_disks, disk_size, cni, nodes, extra_config, feature_gates, service_cluster_ip_range, rosetta, wait-timeout) | ✅ | `Profile`/`Template` embed `MinikubeSpec`; flags emitted in Python's order. Fixes minikube defaulting to docker instead of `container_runtime: containerd` |
| `$vm`/`$container`/`$network` placeholder resolution | ✅ | `resolvePlatform` maps placeholders to per-host driver/network (vfkit/vmnet-shared on macOS, kvm2/default on linux-amd64), matching Python's `_PLATFORM_DEFAULTS` |
| per-node containerd config (`ContainerdConfigStep`) | ✅ (unit-tested; cp/ssh needs real cluster) | post-start `minikube cp`+TOML-merge+`ssh restart containerd`; idempotent Done; mirrors `_configure_containerd` (minus the registry cache) |
| external provider (ExternalProvider) | 🚧 | No-ops for lifecycle; Status probes `/readyz` via kubectl; unit-tested M4D |
| Per-profile provider selection (`provider.For`, `build.ProviderSelector`) | ✅ | External profiles get ExternalProvider; normal profiles get MinikubeProvider |
| suspend / resume / load-image | 🚧 | Unit-tested; delegates to provider per profile |
| `cli.Kubectl` wrapper (Apply, ApplyKustomizeDir, WaitFor, WaitCondition, GetRaw, ClusterInfoDump, …) | ✅ | All methods argv-tested |
| Addon-execution framework (registry, ensure integration, parallel workers) | ✅ | |
| Reality-gated addon re-runs (`ensure.NewGatedGroup` + per-addon readiness probe) | ✅ | A satisfied addon reports `addon/X: skipped, already satisfied` and runs nothing, instead of replaying every apply/wait/rollout. **All workload addons are gated**, including the multi-phase ones with thorough multi-condition probes: rook-cluster (CephCluster Ready + CSI components + CSIAddonsNodes Connected), rook-pool (pool Ready + peer token), rook-cephfs (filesystems Ready), ocm-cluster (ManagedCluster Available + addon deployments), argocd (server Available + cluster secrets), submariner (broker + member deployments), rbd-mirror (CephRBDMirror Ready + mirroring healthy). Only recipe and odf-external-snapshotter stay ungated (apply-`-k`-only, already cheap). Probes are conservative — any uncertainty reports not-ready, and a post-Do latch means a gate can never wrongly fail or skip needed work. |

### Commands

| Command | Status | Notes |
|---------|--------|-------|
| `start` | ✅ | Cluster-validated (c1/qemu2): creates the cluster, skips it on re-run (reality-as-checkpoint), runs the worker/addon tree |
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
| registry-cache / host-setup / cleanup | **Out of scope**: these are drenv host-infra features (local Docker registry, host `/etc/hosts`, DNS). drenv-go applies kustomizations directly; the kustomize-build cache is unnecessary. Host-infra setup is intentionally not reimplemented. |

### Addon parity sub-table

Every regional-dr addon has been ported and unit-tested (argv-level). A subset
is now also validated end-to-end on a real single-node cluster (`c1`).

**Cluster validation environment:** this host's vfkit driver is broken (SSH
timeouts), but drenv-go's `minikube start` auto-fell-back to the **qemu2** driver
and `c1` came up. qemu's builtin (user-mode) network is NOT host-routable, so
addons that need the host to reach a cluster NodePort/Service (minio `mc`,
velero, argocd) cannot complete here — a vmnet/vfkit limitation, not a drenv-go
bug (Python drenv hits the same on qemu-builtin). kubectl-path addons validate
fully.

| Addon | Registered name | Status |
|-------|----------------|--------|
| external-snapshotter | `external-snapshotter` | ✅ cluster-validated (c1/qemu2): CRDs applied, established-wait, controller rolled out |
| odf-external-snapshotter | `odf-external-snapshotter` | 🚧 ported, argv-tested (kubectl-only; expected to pass like external-snapshotter) |
| olm | `olm` | 🚧 ported, argv-tested (kubectl-only; expected to pass like external-snapshotter) |
| recipe | `recipe` | ✅ cluster-validated (c1/qemu2): CRD applied |
| csi-addons | `csi-addons` | 🚧 ported, argv-tested (kubectl-only; expected to pass like external-snapshotter) |
| ocm-controller | `ocm-controller` | 🚧 ported, argv-tested, NOT cluster-validated |
| minio | `minio` | ⚠️ partial (c1/qemu2): apply + rollout validated; `mc` alias/bucket blocked by qemu-builtin NodePort not being host-routable (env, not code) |
| velero | `velero` | 🚧 ported, argv-tested, NOT cluster-validated |
| volsync | `volsync` | 🚧 ported, argv-tested, NOT cluster-validated |
| ocm-hub | `ocm-hub` | 🚧 ported, argv-tested, NOT cluster-validated |
| ocm-cluster | `ocm-cluster` | 🚧 ported, argv-tested, NOT cluster-validated |
| submariner | `submariner` | 🚧 ported, argv-tested, NOT cluster-validated |
| argocd | `argocd` | 🚧 ported, argv-tested, NOT cluster-validated |
| rook-operator | `rook-operator` | 🚧 ported, argv-tested, NOT cluster-validated |
| rook-cluster | `rook-cluster` | 🚧 ported, argv-tested, NOT cluster-validated |
| rook-toolbox | `rook-toolbox` | 🚧 ported, argv-tested, NOT cluster-validated |
| rook-pool | `rook-pool` | 🚧 ported, argv-tested, NOT cluster-validated |
| rook-cephfs | `rook-cephfs` | 🚧 ported, argv-tested, NOT cluster-validated |
| rbd-mirror | `rbd-mirror` | 🚧 ported, argv-tested, NOT cluster-validated |

### Known cluster-validation TODOs (from code review)

These issues will surface when a real cluster run is possible:

1. **submariner: broker-info CWD** — `subctl deploy-broker` has no output-path flag; it always writes `broker-info.subm` into the working directory. The Go port mirrors Python exactly: run deploy-broker, then `os.Rename` the file to the deterministic path. Needs a real subctl run to confirm the CWD-write + rename round-trip.

2. **argocd: temp-kubeconfig** — **NOAUTH handling fixed:** the Go port now suppresses `argocd cluster add` failures only when the error is exit-20 AND the output contains "NOAUTH" (matching Python), via the new `OutputEnv` runner seam. The temp-kubeconfig creation still needs validation against a live argocd.

3. **rbd-mirror: daemon-restart-on-timeout** — **Implemented:** `waitRBDMirroringHealthy` now retries up to 3 attempts, restarting `deploy/rook-ceph-rbd-mirror-a` (`Kubectl.RolloutRestart` + rollout status) between attempts on a health timeout, mirroring Python. The retry/restart logic is unit-tested; the wait intervals still need validation against real Rook output.

4. **ocm: namespace-create waits** — The ocm-hub / ocm-cluster addons wait for `namespace/open-cluster-management` and `namespace/open-cluster-management-hub` to be created. The wait duration may need tuning against a real cluster.

5. **per-node `containerd` plugin config** — regional-dr.yaml sets a `containerd:` block (`device_ownership_from_security_context: true`, needed by rook). **Implemented:** after the cluster starts, `provider.ContainerdConfigStep` does `minikube cp` of `/etc/containerd/config.toml` out, deep-merges the profile block (TOML), copies it back, and `minikube ssh sudo systemctl restart containerd` — mirroring Python's `_configure_containerd`. The step is idempotent (Done skips when the block is already present, so re-runs don't restart a healthy containerd). The registry-mirror part of Python's helper stays out of scope with the registry cache. The merge/decision logic is unit-tested; the cp/ssh round-trip needs real-cluster validation.

## Conventions I follow here

- Branch: `main`, push to `rtalur-github`. No PRs.
- Commits: small, modular, one logical change each. `git commit -s`.
  End messages with `Assisted-by: Claude Code/<model-id>`.
- New Go tooling: thin `Makefile` → `magefile.go`, pinned tools in `tools.mod`.
