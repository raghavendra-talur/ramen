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
| external provider (ExternalProvider) | 🚧 | No-ops for lifecycle; Status probes `/readyz` via kubectl; unit-tested M4D |
| Per-profile provider selection (`provider.For`, `build.ProviderSelector`) | ✅ | External profiles get ExternalProvider; normal profiles get MinikubeProvider |
| suspend / resume / load-image | 🚧 | Unit-tested; delegates to provider per profile |
| `cli.Kubectl` wrapper (Apply, ApplyKustomizeDir, WaitFor, WaitCondition, GetRaw, ClusterInfoDump, …) | ✅ | All methods argv-tested |
| Addon-execution framework (registry, ensure integration, parallel workers) | ✅ | |

### Commands

| Command | Status | Notes |
|---------|--------|-------|
| `start` | 🚧 | Builds full ensure tree; unit-tested; no real cluster run |
| `stop` | 🚧 | Parallel ensure; unit-tested |
| `delete` | 🚧 | Parallel ensure; unit-tested |
| `status` | 🚧 | Per-profile provider; unit-tested |
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

Every regional-dr addon has been ported and unit-tested (argv-level). None has been validated against a live cluster because the vfkit clusters could not boot on this machine.

| Addon | Registered name | Status |
|-------|----------------|--------|
| external-snapshotter | `external-snapshotter` | 🚧 ported, argv-tested, NOT cluster-validated |
| odf-external-snapshotter | `odf-external-snapshotter` | 🚧 ported, argv-tested, NOT cluster-validated |
| olm | `olm` | 🚧 ported, argv-tested, NOT cluster-validated |
| recipe | `recipe` | 🚧 ported, argv-tested, NOT cluster-validated |
| csi-addons | `csi-addons` | 🚧 ported, argv-tested, NOT cluster-validated |
| ocm-controller | `ocm/controller` | 🚧 ported, argv-tested, NOT cluster-validated |
| minio | `minio` | 🚧 ported, argv-tested, NOT cluster-validated |
| velero | `velero` | 🚧 ported, argv-tested, NOT cluster-validated |
| volsync | `volsync` | 🚧 ported, argv-tested, NOT cluster-validated |
| ocm-hub | `ocm/hub` | 🚧 ported, argv-tested, NOT cluster-validated |
| ocm-cluster | `ocm/cluster` | 🚧 ported, argv-tested, NOT cluster-validated |
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

1. **submariner: broker-info CWD** — `subctl deploy-broker` writes `broker-info.subm` into its working directory. The Go port changes to a temp directory (`os.Chdir`) before calling subctl and renames the file afterward. Needs a real subctl run to confirm the rename path is correct.

2. **argocd: NOAUTH / temp-kubeconfig** — After `argocd cluster add`, argocd sometimes returns exit code 20 (NOAUTH transient error). The Go port suppresses it. The temp-kubeconfig creation also needs to be validated against a live argocd.

3. **rbd-mirror: daemon-restart-on-timeout + CSIAddonsNode retry** — The Python rbd-mirror addon restarts the Ceph rbd-mirror daemon if mirroring setup times out, and retries until all CSIAddonsNodes report "Connected". The Go port mirrors this logic but the wait intervals and retry counts have not been validated against real Rook output.

4. **ocm: namespace-create waits** — The ocm-hub / ocm-cluster addons wait for `namespace/open-cluster-management` and `namespace/open-cluster-management-hub` to be created. The wait duration may need tuning against a real cluster.

## Conventions I follow here

- Branch: `main`, push to `rtalur-github`. No PRs.
- Commits: small, modular, one logical change each. `git commit -s`.
  End messages with `Assisted-by: Claude Code/<model-id>`.
- New Go tooling: thin `Makefile` → `magefile.go`, pinned tools in `tools.mod`.
