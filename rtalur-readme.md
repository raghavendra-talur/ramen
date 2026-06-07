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

Parallel Go implementation of `drenv`. Tracks which pieces work in Go yet.
(✅ done · 🚧 in progress · ⬜ not started)

| Piece | Status |
|-------|--------|
| Module skeleton (mage, tools.mod, cobra, ensure pkg, envfile parser) | ⬜ |
| minikube provider + cluster lifecycle (`start`/`delete`/`status`) | ⬜ |
| kubectl/kustomize wrappers + addon primitives | ⬜ |
| Addons (smallest env first) | ⬜ |
| Remaining commands (`stop`/`load`/`suspend`/`resume`/`gather`/`cache`) | ⬜ |
| lima + external providers | ⬜ |

## Conventions I follow here

- Branch: `main`, push to `rtalur-github`. No PRs.
- Commits: small, modular, one logical change each. `git commit -s`.
  End messages with `Assisted-by: Claude Code/<model-id>`.
- New Go tooling: thin `Makefile` → `magefile.go`, pinned tools in `tools.mod`.
