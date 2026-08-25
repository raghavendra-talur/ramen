# simtest UI: live observability for matrix runs

Date: 2026-08-18 Status: Approved (layout and architecture validated
interactively)

## Problem

A simtest matrix run exercises hundreds of state × fault scenarios against three
control planes, five simulated actors, and three ramen manager subprocesses.
When a scenario stalls or fails, the only windows into the run are interleaved
text logs (`go test -v`, per-manager log files, `actors.log`,
`progression-edges.log`). Reconstructing "what was the world doing when scenario
mx10 stalled" from those files is slow, and impossible to do at a glance while
the run is live.

## Goal

A read-only web UI, served by the test process itself while a run executes, that
shows at a glance:

- progress through the matrix (done / running / queued, with failure reasons),
- the live state of the world (clusters, DRPC/VRG/PVC state, actors, faults,
  manager processes),
- a per-scenario state timeline (Grafana state-timeline style), and
- invariant violations the moment they happen.

Primary consumer: the developer debugging simtest or ramen, with a browser tab
open next to a running `go test`.

## Non-goals (v1)

- No control surface: the UI cannot pause scenarios, inject faults, or touch the
  world in any way. Read-only is a design guarantee, not a v1 shortcut.
- No post-run replay viewer. V1 persists the event stream to the artifacts dir
  (see Wire protocol) so a replay viewer becomes a later, small addition, but
  does not build one.
- No JS build toolchain, no external CDN assets, no new Go dependencies.
- No change to any existing ramen file outside `simtest/` (simtest's standing
  constraint), and no dependency on ramenbooth.

## Approved visual design (dashboard revision, 2026-08-25)

Dark mission-control aesthetic, monospace, laid out as a status dashboard. The
organizing rule: component state lives ON the component (manager LEDs and fault
badges on the cluster cards), while the header strip summarizes the run and
shouts about anything hurting.

```
+----------------------------------------------------------------------+
| SIMTEST · run  ▓▓▓▓░░ 12/78  11✓ 1✗ 0⚑  [fault chips] [DEAD]  2m08s |
+--------+-----------------------------------------+-------------------+
| TESTS  |               WORLD STAGE               | SELECTED          |
| rail,  |   hub card (DRPC, mgr LED, badges)      | status+elapsed,   |
| grouped|   dr1/dr2 cards (VRG, PVC grid,         | phase/progression,|
| by     |     mgr LED, fault badges)              | conditions, VRGs  |
| stage/ |   S3 node (outage flash), topology      +-------------------+
| checkpt|   edges between components              | EVENTS (tall      |
|        |                                         | column, colored)  |
+--------+-----------------------------------------+-------------------+
| STATE TIMELINE: colored bands + time axis + violation flags          |
| tracks: drpc-phase, drpc-progression, vrg-dr1, vrg-dr2, s3, invar.   |
+----------------------------------------------------------------------+
```

Key behaviors:

- **Header strip**: run name, progress bar, pass/fail/violation counters,
  elapsed clock; active faults and dead managers appear as red chips so trouble
  is visible regardless of where the user is looking.
- **TESTS rail**: every scenario, grouped by stage and injection point
  (`failover · at wrr`, `relocate · at pfs`, ...), short fault labels, per-row
  durations, spinner + live elapsed on the running row. Clicking a row pins the
  timeline and inspector to that scenario — including finished ones (replay from
  retained segments).
- **Stage**: cluster cards carry their own manager LED (blinking red + banner
  when the manager subprocess dies) and red badges for the faults currently
  injected against that cluster; PVCs as small squares whose color animates on
  state change; S3 flashes its outage; topology edges connect the components.
  Known DR states use fixed semantic colors (green healthy, blue
  secondary/ready, purple in-flight, amber cleanup, red trouble), with a
  deterministic hash fallback for unknown values.
- **Inspector**: selected scenario's status and elapsed, DRPC phase/progression,
  conditions, per-cluster VRG summary.
- **Events**: tall column of single-line entries — short clock, colored actor
  names, red violations — full text on hover, scroll position preserved across
  re-renders.
- **State timeline**: discrete colored bands per track on a shared clock with a
  time axis (tenths under 10s), invariant violations as flags. Per-scenario;
  retained after the scenario finishes. The s3 track reads down/up.

## Architecture

New package `simtest/ui`, pure addition, stdlib only (`net/http`, `embed`,
`encoding/json`).

### Activation and lifecycle

- Opt-in via env var: `SIMTEST_UI=1` (ephemeral port) or `SIMTEST_UI=:8090`.
- `world.build()` starts the server and prints the URL as the first line of test
  output; `world.Teardown` shuts it down.
- `SIMTEST_UI_HOLD=1` keeps the server (final snapshot included) alive after
  tests finish until Ctrl-C, for post-mortem inspection of a failed run.
- UI failures never fail a test: server start errors are logged and ignored.

### Status hub

One in-memory model behind a mutex (`ui.Hub`), the single source of truth for
both the snapshot endpoint and the event stream. Model:

- RunInfo: name, start time, matrix plan, progress counters.
- Tests: per scenario — group, name, status (queued/running/passed/failed),
  duration, failure reason.
- World: per cluster — manager process state, DRPC (phase, progression,
  conditions), VRG state, PVC states.
- Faults: active fault policies and parameters.
- Timelines: per scenario, per track — list of (value, start, end) segments,
  derived by the hub from state observations.
- Events: bounded ring buffer of recent actor/invariant events.

Producers and their hooks (each a one-line call into the hub):

| Producer                            | Hook                                                                                                                              |
| ----------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| test driver (`tests/`)              | `RunStart(plan)`, `ScenarioStart/End(id, result)`                                                                                 |
| cluster watchers                    | DRPC/VRG/PVC observations from watches on the three envtest clusters (reuses the observe/recorder machinery's scheme and configs) |
| actors evlog                        | tee of each event line                                                                                                            |
| fault policy store                  | fault set/clear notifications                                                                                                     |
| ManagerProcess / invariants checker | process state changes; violations                                                                                                 |

Hard rules:

- Every hub write is non-blocking: buffered channel, drop-newest on overflow. A
  slow or absent browser can never stall a scenario.
- A nil `*Hub` is a no-op receiver: hooks are called unconditionally; UI off
  costs nothing and no call site needs a nil check.

## Wire protocol

Three GET endpoints, localhost by default:

- `/` — the embedded page.
- `/api/snapshot` — the full hub model as one JSON document. A fresh or
  reconnecting client renders entirely from this.
- `/api/stream` — Server-Sent Events. Incremental JSON events, each with a
  monotonic sequence number: `test_started`, `test_finished`, `state_changed`,
  `actor_event`, `fault_changed`, `manager_changed`, `invariant_violated`. SSE
  over WebSocket deliberately: one-way matches read-only, stdlib on both ends,
  and `EventSource` reconnects natively — on reconnect the client re-fetches
  `/api/snapshot` and resumes.

Persistence: the hub tees the same JSON event lines into
`<artifacts>/ui-events.jsonl`. This is the future replay format; v1 writes it
and stops there.

## Frontend

One `index.html` embedded via `go:embed`: vanilla JS, no framework, no build
step (the approved mockup's HTML/CSS is the implementation skeleton).

- Client store: `/api/snapshot` fills it, `EventSource` events mutate it; each
  event type maps to a targeted DOM update. No virtual DOM, no full re-renders.
- Stage as inline SVG: cluster cards, edges as paths; actor events animate
  message dots along the matching edge (CSS `offset-path`); state changes swap
  CSS classes so color transitions animate.
- Timeline as flex bands per track; playhead driven by event timestamps;
  finished scenarios render from retained segments.
- Selection (pinned scenario, inspected object) lives in the client store.

**Implementation notes.** The shipped frontend deviates from the plan above in
scope, not in wire protocol:

- Rendering is coalesced full-snapshot refetch + full re-render, not per-event
  targeted DOM updates: each `EventSource` event (and reconnect, via `onopen`)
  schedules a debounced `GET /api/snapshot` (at most ~5/s) and the whole page
  re-renders from the result (scroll positions preserved). A 1s tick keeps
  clocks and open timeline bands advancing between events. No client-side
  incremental model.
- Shipped since the first cut: rail grouping with durations, timeline time axis
  and violation flags, semantic state colors, topology edge lines, manager LEDs
  and per-cluster fault badges, the events column, and the header
  fault/dead-manager chips.
- Still deferred to follow-ups: message-dot animation along the edges, a
  "waiting on" spotlight that highlights the component a Wait\* progression is
  blocked on, fault-severed edge rendering, and per-cluster actor glyphs that
  flash as agents act.

## Testing

1. Hub unit tests (the meat): feed observation sequences, assert model
   transitions and timeline-segment derivation, including out-of-order and
   dropped-event cases.
1. Server tests via `httptest`: snapshot shape, SSE framing,
   reconnect-with-snapshot semantics, and a slow-consumer test proving producers
   never block.
1. Smoke test: start a world with `SIMTEST_UI=1`, fetch `/` and `/api/snapshot`,
   assert the page serves and the snapshot reflects a running scenario. JS
   behavior is verified by eye during development.

## Placement

Implemented on the simtest branch as pure additions under `simtest/` (plus this
spec). Because nothing outside `simtest/` changes, the work is indifferent to
the pending rebase of the branch onto `origin/main` (validated 2026-08-18:
simtest runs on `origin/main` + PR #2552).
