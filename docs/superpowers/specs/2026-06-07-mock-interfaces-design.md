# Mock Interfaces for External Surfaces — Design

Date: 2026-06-07
Status: Approved (slice 1 scoped)

## Goal

Make Ramen easier to test *and* easier to play with locally by faking its
external surfaces. The same mock implementations serve two purposes:

1. Automated unit/integration tests.
2. Running Ramen against real `kind`/`minikube` clusters (via `drenv`) with
   **all external backends mocked**, so a developer can drive a real
   failover/relocate end-to-end without installing Ceph, csi-addons, VolSync,
   Velero, or object storage.

Inspiration: heketi's `executor: mock` model, where the server boots fully but
talks to a fake backend instead of real nodes. Ramen's twist: because it is a
Kubernetes operator, the most faithful fake for a CRD-backed backend is a
small *fulfillment controller* that satisfies the CRs Ramen creates, rather
than an in-process stub.

## Decisions (from brainstorming)

- **Run target:** real multi-cluster `kind`/`minikube` (hub + managed) brought
  up by `drenv`/`drenv-go`. The Kubernetes API and Ramen's own CRDs
  (DRPC/VRG/DRPolicy/DRCluster) are real; everything else is mocked.
- **Fidelity:** stateful. Mocks transition realistically so DR workflows run to
  completion (promote → reports Primary, etc.).
- **Strategy:** mock *fulfillment controllers* for CRD-backed backends
  (VolumeReplication, VolSync, Velero, VolumeSnapshot). In-process interface
  mocks are reserved for non-CRD surfaces (S3, pod-exec) and are **deferred** —
  see "Explicitly out of scope".
- **Observability is a first-class requirement:** every mock action must be
  visible — what it received and what it returned/set.
- **First slice:** mock **VolumeReplication** fulfillment controller. (S3 is
  intentionally *not* mocked — for real cross-cluster failover you just point
  Ramen's real S3 client at `drenv`'s MinIO; an S3 mock adds little value.)
- **Packaging:** a `--mock-controllers` mode on the **existing manager binary**
  (one image), not a separate binary.
- **Test refactor:** do **not** refactor existing VRG tests in slice 1. Build
  the mock against the same logic; dedup tests in a later follow-up.

## Background: what Ramen reads back from a VolumeReplication

From `internal/controller/vrg_volrep.go`, after Ramen creates/updates a
`VolumeReplication` CR (`spec.replicationState = primary|secondary`), it polls:

- `status.State` — `volrep.PrimaryState` / `volrep.SecondaryState`
  (`UnknownState` on failure).
- `status.ObservedGeneration` — must equal the object's `.Generation`
  (`vrg_volrep.go:1791`, `:2125`); stale status is ignored.
- `status.Conditions` — checked via `isVRConditionMet`:
  - `ConditionValidated` (True / `PrerequisiteMet`)
  - `ConditionCompleted` (True / `Promoted` for primary)
  - `ConditionDegraded` (False / `Healthy`)
  - `ConditionResyncing` (False / `NotResyncing`)
  Each condition's `ObservedGeneration` must match `.Generation`.
- `status.DestinationVolumeID` — non-empty for the primary so destination-info
  checks pass (`vrg_volrep.go:875`, `:879`).

The exact construction of this status already exists in test code:
`vrg_volrep_test.go:3155` (`promoteVolRepsAndDo`) and `:3209`
(`generateVRConditions`). The mock controller is that logic, lifted out of
`_test.go` into a shippable package and driven by a reconcile loop.

## Architecture

### Provider seam (the reusable pattern, recorded for later surfaces)

Each external surface has a Go interface. A factory selects real vs mock at
runtime. Mock implementations live in a **non-test** package so they compile
into the binary *and* are importable from `_test.go`. A generic logging
decorator wraps any provider to satisfy the observability requirement. Slice 1
exercises the CRD-controller variant of this pattern; the in-process variant
(factory keyed off `RamenConfig.Mock.<surface>`) is documented but unused until
a non-CRD surface is mocked.

### Slice 1 components

```
internal/controller/mock/                 (new, non-test package)
  volumereplication_fulfiller.go          VolumeReplicationFulfiller — pure status logic
  volumereplication_reconciler.go         MockVolumeReplicationReconciler — watch + apply + log + Event

cmd/main.go                               add --mock-controllers flag → runMockControllers()
```

**`VolumeReplicationFulfiller`** — pure, no client. Given a
`*volrep.VolumeReplication`, returns the `VolumeReplicationStatus` it should
have (State, ObservedGeneration, Message, Conditions, DestinationVolumeID)
based on `spec.replicationState`. Mirrors `generateVRConditions`. Handles both
directions:
- `primary` → `State=PrimaryState`, `Completed/Promoted`, `DestinationVolumeID`
  set.
- `secondary` → `State=SecondaryState`, `Completed/Demoted`, no
  `DestinationVolumeID`.
- Always: `Validated=True`, `Degraded=False`, `Resyncing=False`, all
  `ObservedGeneration = .Generation`.

**`MockVolumeReplicationReconciler`** — controller-runtime reconciler watching
`VolumeReplication`. On each event:
1. Compute desired status via the fulfiller.
2. If the live status already equals desired (state, observedGeneration,
   conditions), return without writing (idempotent — no hot loop).
3. Otherwise `Status().Update`, then emit observability (below).

### Observability

Each fulfillment emits **both**:
1. Structured log:
   `received: VR=<ns/name> spec.state=primary gen=N` then
   `set: status.state=Primary conditions=[Validated=T,Completed=T,Degraded=F,Resyncing=F] destID=mock-<ns>-<pvc>`.
2. A Kubernetes **Event** on the VolumeReplication object
   (`reason=MockFulfilled`, message summarizing the transition), so
   `kubectl describe volumereplication <x>` and `kubectl get events` show what
   the mock did and when.

### Run mode

`cmd/main.go` gains a `--mock-controllers` bool flag. When set,
`runMockControllers()` starts a controller-runtime manager that registers
**only** the mock fulfillment reconcilers (not the real Ramen reconcilers) and
runs. Deployed as its own Deployment on each managed cluster in mock
environments. Production manager run path is untouched (flag defaults false).

### drenv wiring

New addon `test/addons/mock-volrep/`:
- Applies the upstream `VolumeReplication` + `VolumeReplicationClass` **CRDs
  only** (no csi-addons operator, no rbd-mirror, no rook).
- Deploys the manager image with `--mock-controllers`.
- Applies a `VolumeReplicationClass` CR (inert; satisfies references).

A mock env profile substitutes `mock-volrep` for the
`csi-addons`/`rbd-mirror`/`rook-*` addons, so a managed cluster needs no real
storage backend to host a VolRep-based VRG.

## Testing

- **Unit:** `VolumeReplicationFulfiller` — primary→Primary+conditions,
  secondary→Secondary, generation propagation, DestinationVolumeID rules.
- **Envtest:** run `MockVolumeReplicationReconciler` against an envtest
  apiserver; create a `VolumeReplication`; assert the status converges to the
  same conditions the existing `waitForVolRepCondition` helpers expect;
  assert idempotency (no second write when nothing changed); assert an Event is
  recorded.
- Existing VRG tests are **not** modified in this slice.

## Explicitly out of scope (this slice)

- S3 / `ObjectStorer` mock — deliberately skipped; use `drenv` MinIO.
- In-process provider factory and `RamenConfig.Mock` field — deferred until a
  non-CRD surface is mocked.
- VolSync, Velero, VolumeSnapshot fulfillment controllers — follow-up specs,
  same pattern.
- Pod-exec mock — follow-up.
- Dedup of existing VRG test helpers onto the shared fulfiller — follow-up.

## Generalization (future specs, pattern fixed here)

Per CRD backend: shared fulfiller in `mock/` → mock reconciler → registered in
the `--mock-controllers` manager → `drenv` addon applying CRDs-only + the mock
controller → structured logs + Events. Targets in priority order: VolSync
`ReplicationSource`/`ReplicationDestination` (+ snapshot dance), Velero
`Backup`/`Restore`, `VolumeSnapshot`/`VolumeGroupSnapshot`.

## Success criteria

A developer can, on a `drenv` mock profile (no Ceph/csi-addons/VolSync/Velero
installed):
1. Apply a VRG (or DRPC driving one) that uses VolumeReplication.
2. Watch the mock controller fulfill each VolumeReplication, visible in both
   manager logs and `kubectl describe`/`kubectl get events`.
3. See the VRG reach `Primary` and report data-ready, i.e. a promotion that
   completes — proving the mock is stateful enough to advance the real Ramen
   state machine.
