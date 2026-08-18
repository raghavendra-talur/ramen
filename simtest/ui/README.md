# simtest live UI

Read-only observability for a simtest run. Design:
`docs/superpowers/specs/2026-08-18-simtest-ui-design.md`.

## Usage

    SIMTEST_UI=1 go test ./tests/ -run TestMatrix -v

The URL prints at world bring-up (`simtest ui: http://127.0.0.1:PORT`).
`SIMTEST_UI=:8090` picks a fixed port. `SIMTEST_UI_HOLD=1` keeps the
server (and final state) up after the run until Ctrl-C.

Every run with the UI on also writes `ui-events.jsonl` into the run's
artifacts dir — the raw event stream, one JSON object per line.

## Guarantees

- Read-only: the UI observes the world and cannot touch it.
- Never affects a test: a nil hub is a no-op, subscribers drop events
  when slow, and a UI launch failure logs and disables instead of
  failing the run.
