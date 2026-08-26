# simtest

A state-space e2e framework for Ramen: three in-process envtest control planes
(hub, dr1, dr2), the **unmodified ramen binary** running as subprocesses against
them, and simulated external actors (VolumeReplication and VolSync fulfillers,
snapshotter, OCM work/view agents, governance policy agent, job runner, PVC
binder, in-process S3) that can inject any status, ordering, delay, or failure
at any state — milliseconds per event. Design:
`../docs/superpowers/specs/2026-07-02-simtest-statespace-e2e-design.md`.

## Quick start

```sh
make simtest        # build ../bin/manager, install envtest assets, run all
```

Or directly (always pass `-count=1`; set `-timeout` — the go test default of 10m
kills a matrix run mid-flight and skips teardown):

```sh
go test ./tests/ -v -count=1 -timeout 90m
```

## The app lifecycle

Scenarios are ordered lists of six stages:

```
create → enroll → failover → relocate → unenroll → delete
```

Every matrix combination runs the full lifecycle with one fault injected at one
(stage, checkpoint); after delete, a per-app leak check asserts nothing remains
(DRPC, VRGs, PVCs, hub-side ManifestWorks/ManagedClusterViews, VolSync
replication pairs and snapshots). Checkpoints are discovered from a seed run of
the same world, so they track what this ramen build actually does. Complex
journeys (repeated failover/relocate) are just longer stage lists.

## The pvcspec axis

Each managed cluster carries two storage stories, mirroring e2e's PVCSpec:

- **rbd** (`mock-rbd`): StorageClass + VolumeReplicationClass with matching
  storageid/replicationid labels — PVCs route through **VolRep**.
- **cephfs** (`mock-cephfs`): StorageClass with a storageid label only, plus a
  VolumeSnapshotClass — no replication class, so PVCs route through **VolSync**.

Two consistency-group stories layer on top, each in its own storageid universe
(so the plain peers stay ungrouped), with the DRPC carrying ramen's
`is-cg-enabled` annotation:

- **rbd-cg** (`mock-rbd-cg`): adds `groupreplicationid` plus a
  VolumeGroupReplicationClass — grouped PVCs replicate through one
  **VolumeGroupReplication** (fulfilled by the vgr actor, which also maintains
  the member-PVC list and re-adopts restored contents).
- **cephfs-cg** (`mock-cephfs-cg`): adds a public-v1 VolumeGroupSnapshotClass —
  grouped PVCs replicate through **ReplicationGroupSource/Destination** over
  **VolumeGroupSnapshot** (the vgs actor creates owner-referenced member
  snapshots; the snapshotter, job-runner, and volsync actors do the rest).

`TestBaselines` runs the full happy-path lifecycle once per pvcspec. The VolSync
path exercises the whole real chain: PSK-secret propagation via a governance
Policy (enforced by the policy-agent actor), the PVC mount job (completed by the
job-runner actor), ReplicationSource/Destination fulfillment, latestImage
snapshot restore on failover, and the manual-trigger final sync on relocate.

## Selecting tests

```sh
go test ./tests/ -run TestBaselines -v -count=1                     # ~7m, all four pvcspecs
go test ./tests/ -run 'TestBaselines/(rbd|cephfs)$' -v -count=1     # ~1m, plain stories only
go test ./tests/ -run TestMatrix -v -count=1 -timeout 3h            # ~180 combos
go test ./tests/ -run 'TestMatrix/rbd/relocate' -v -count=1 -timeout 20m
go test ./tests/ -run 'TestMatrix/rbd/failover/at=wrr/fault=s3-down' -v -count=1
```

Subtests address as
`TestMatrix/<pvcspec>/<stage>/at=<checkpoint>/fault=<fault>`; stages taking
fault injection are `failover` and `relocate`, checkpoints come from each
pvcspec's own seed run (`wrr`, `clean`, `wuc`, and relocate-only `pfs`). Each
pvcspec carries the faults that can touch its data path — the full grid for rbd,
volrep faults for rbd-cg, volsync/snap/jobs/polagent faults for the cephfs
stories — and cephfs-cg's grid is bounded to the `wrr`/`pfs` checkpoints because
its lifecycle converges on ramen's minute-scale requeues (~5min per combo). Tune
both in `matrixSpecs()`.

## Environment knobs

| Variable                              | Effect                                                                 |
| ------------------------------------- | ---------------------------------------------------------------------- |
| `SIMTEST_UI=1` (or `=127.0.0.1:8127`) | Live web UI; URL printed at world bring-up. See `ui/README.md`.        |
| `SIMTEST_UI_HOLD=1`                   | Keep the UI (and final state) up after the run until Ctrl-C.           |
| `SIMTEST_TIMEOUT_SCALE=2.5`           | Multiply observation timeouts for slow machines.                       |
| `SIMTEST_BACKEND=kind`                | Real kind clusters instead of envtest (see Backends below).            |
| `KUBEBUILDER_ASSETS`                  | Override envtest binaries (default: from `../testbin/testassets.txt`). |

## Artifacts

Every run writes `simtest/.artifacts/<TestName>-<timestamp>/`: per-manager logs
(`hub.log`, `dr1.log`, `dr2.log`), per-cluster admin kubeconfigs that work with
plain `kubectl` against the live world, `actors.log`, `progression-edges.log`,
and — with the UI on — `ui-events.jsonl`, the complete observed event stream.

## Backends

`SIMTEST_BACKEND` selects how the world's three clusters are provided:

- **envtest** (default): etcd + kube-apiserver per cluster — fast (~15s world
  boot) and deterministic, but nothing beyond the apiserver runs, so the
  **janitor** actor stands in for kube-controller-manager (protection
  finalizers, ownerRef garbage collection, foreground deletion).
- **kind**: one kind cluster per world cluster (requires `kind` plus a docker or
  podman runtime — with podman, start the machine first). A real
  kube-controller-manager runs and the janitor does not: its absence is itself a
  test, catching ramen bugs the sweeps would mask (GC-ordering assumptions,
  finalizer timing). Boot is ~90s and combos run slower under real-controller
  timing, so use it for baselines and periodic validation rather than the fault
  matrix:

```sh
podman machine start
SIMTEST_BACKEND=kind go test ./tests/ -run 'TestBaselines/(rbd|cephfs)$' -v -count=1 -timeout 40m
```
