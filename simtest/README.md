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

`TestBaselines` runs the full happy-path lifecycle once per pvcspec. The VolSync
path exercises the whole real chain: PSK-secret propagation via a governance
Policy (enforced by the policy-agent actor), the PVC mount job (completed by the
job-runner actor), ReplicationSource/Destination fulfillment, latestImage
snapshot restore on failover, and the manual-trigger final sync on relocate.

## Selecting tests

```sh
go test ./tests/ -run TestBaselines -v -count=1                # ~40s, rbd+cephfs
go test ./tests/ -run TestMatrix -v -count=1 -timeout 45m      # ~78 combos, ~22m
go test ./tests/ -run 'TestMatrix/relocate' -v -count=1 -timeout 20m
go test ./tests/ -run 'TestMatrix/failover/at=wrr/fault=s3-down' -v -count=1
```

Subtests address as `TestMatrix/<stage>/at=<checkpoint>/fault=<fault>`; stages
taking fault injection are `failover` and `relocate`, checkpoints come from the
seed (`wrr`, `clean`, `wuc`, and relocate-only `pfs`).

## Environment knobs

| Variable                              | Effect                                                                 |
| ------------------------------------- | ---------------------------------------------------------------------- |
| `SIMTEST_UI=1` (or `=127.0.0.1:8127`) | Live web UI; URL printed at world bring-up. See `ui/README.md`.        |
| `SIMTEST_UI_HOLD=1`                   | Keep the UI (and final state) up after the run until Ctrl-C.           |
| `SIMTEST_TIMEOUT_SCALE=2.5`           | Multiply observation timeouts for slow machines.                       |
| `KUBEBUILDER_ASSETS`                  | Override envtest binaries (default: from `../testbin/testassets.txt`). |

## Artifacts

Every run writes `simtest/.artifacts/<TestName>-<timestamp>/`: per-manager logs
(`hub.log`, `dr1.log`, `dr2.log`), per-cluster admin kubeconfigs that work with
plain `kubectl` against the live world, `actors.log`, `progression-edges.log`,
and — with the UI on — `ui-events.jsonl`, the complete observed event stream.
