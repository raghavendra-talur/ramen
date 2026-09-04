# simtest live UI

Read-only observability for a simtest run. Design:
`docs/superpowers/specs/2026-08-18-simtest-ui-design.md`.

## Usage

```
SIMTEST_UI=1 go test ./tests/ -run TestMatrix -v
```

The URL prints at world bring-up (`simtest ui: http://127.0.0.1:PORT`).
`SIMTEST_UI=:8090` picks a fixed port. Note that a bare port like `:8090` binds
all interfaces, unlike the default (`SIMTEST_UI=1` or unset host), which binds
loopback only. `SIMTEST_UI_HOLD=1` keeps the server (and final state) up after
the run until Ctrl-C.

Every run with the UI on also writes `ui-events.jsonl` into the run's artifacts
dir — the raw event stream, one JSON object per line.

## The stage

Beyond DRPC/VRG/PVC state, each cluster card carries data-plane chips for the
machinery replicating its PVCs: VolumeReplication and VolumeGroupReplication
state (with a `…` marker while the observed state lags the spec), VolSync
ReplicationSource/Destination last-sync and latest image, and
Volume(Group)Snapshot readiness. Hovering a PVC square shows the correlated
summary.

Clicking any stage object — the DRPC, a VRG, a PVC square, or a chip — opens a
drawer with its status conditions and full YAML (managed fields stripped),
served on demand by `/api/object` and kept live while open; Esc closes it. Ideas
ported from the retired ramenbooth experiment.

## Guarantees

- Read-only: the UI observes the world and cannot touch it.
- Never affects a test: a nil hub is a no-op, subscribers drop events when slow,
  and a UI launch failure logs and disables instead of failing the run. Each
  subscriber's event channel is buffered and written to with a non-blocking
  send: when a subscriber (e.g. a slow browser) can't keep up, the newly
  produced event is dropped for that subscriber (drop-newest); a reconnecting or
  refreshed client always recovers full state via the snapshot endpoint.
