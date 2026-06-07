<!--
SPDX-FileCopyrightText: The RamenDR authors
SPDX-License-Identifier: Apache-2.0
-->

# drenv-go: a parallel, ensure-model rewrite of drenv in Go

**Status:** Design approved — 2026-06-07
**Author:** Raghavendra Talur (with Claude Code)
**Scope:** New, parallel implementation. Does not replace or modify the existing Python `drenv`.

## Motivation

Faster dev loops are not only about reducing run time — they are about having
**many checkpoints**, so that re-running work is cheap, idempotent, and resumable.

The existing Python `drenv` (`test/drenv/`) provisions multi-cluster test
environments. It works, but its `start` flow is largely procedural: a failed or
interrupted run is recovered by ad-hoc restart logic, and there is no uniform
notion of "is this stage already satisfied?" applied to every unit of work.

`drenv-go` is a ground-up Go implementation built around a single idempotent
**ensure model**: every unit of work checks reality, acts only if reality is
wrong, then verifies. Re-running is always safe and skips whatever is already
correct. This gives the dense, reliable checkpointing that makes the inner dev
loop fast.

This is a **parallel** implementation. The Python `drenv` stays exactly as it is.
We build `drenv-go` incrementally, diffing its behavior against the Python tool
at every step, until it reaches parity.

## Goals

- Full parity with Python `drenv` over time: providers, lifecycle, and all addons,
  all implemented in Go.
- Every unit of work is an idempotent, independently-checkable checkpoint.
- Re-running `start` is cheap and converges (reconcile semantics).
- Reuse the existing envfile YAML format unchanged — drop-in, diffable.
- Each milestone is independently usable and verifiable; each addon port is a
  separate commit.

## Non-goals

- Replacing or modifying the Python `drenv`. The two coexist.
- Reimplementing minikube/lima/kubectl/kustomize. We wrap the same CLIs.
- A new config format. Existing `test/envs/*.yaml` are reused verbatim.
- Native client-go / typed API access (we wrap CLIs; this may be revisited later).

## Key decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Parity scope | Full parity incl. addons, in Go, incremental | End goal is a complete replacement-grade tool |
| Operation strategy | Wrap CLIs (kubectl / minikube / kustomize) | Fastest to parity; behavior trivially diffable against Python |
| Module layout | New module `test/drenv-go/` with mage + `tools.mod` | Matches the author's convention for new Go tools |
| Checkpoint model | Reality is the checkpoint (stateless) | Purely idempotent; never stale; resume = run again |
| Config | Reuse existing envfile YAML | Drop-in, no second schema to maintain |
| First provider | minikube | Primary on macOS + Linux CI |

## Architecture

### The ensure model (core abstraction)

The entire tool is built from one small interface. A `Step` is one idempotent
unit of work whose post-condition is also its pre-condition:

```go
type Step interface {
    Name() string
    // Done reports whether the desired state currently holds (cheap, read-only).
    // Used BOTH as the pre-check (skip if already true) and as verify.
    Done(ctx context.Context) (bool, error)
    // Do performs the operation needed to reach the desired state.
    Do(ctx context.Context) error
}
```

The runner is identical for every step — this is where checkpoints come from:

```go
func Ensure(ctx context.Context, s Step) Result {
    if ok, err := s.Done(ctx); err != nil {
        return Failed
    } else if ok {
        return Skipped              // checkpoint already satisfied; no work
    }
    if err := s.Do(ctx); err != nil {
        return Failed
    }
    return waitDone(ctx, s)         // verify: poll Done until true or timeout
}
```

Properties that follow:

- **Reality is the checkpoint.** Re-running `start` re-checks every `Done()`
  (cheap) and only `Do()`s what is actually missing. No local state file, so it
  can never go stale or drift.
- **Resume = run again.** An interrupted run is recovered by re-invoking; already
  satisfied steps are skipped.
- **Many checkpoints** = we decompose into many small steps, each independently
  checkable.

Steps where verification requires waiting (rollouts, conditions) are handled by
`waitDone` polling `Done` with a timeout. Steps whose `Do` already blocks until
ready (e.g. `kubectl wait`) simply return `Done == true` immediately afterward.

### Composition & concurrency

Steps compose into a tree mirroring the envfile, with parallelism matching the
existing Python `drenv` exactly:

- **Profiles run in parallel** (each cluster is independent).
- **Within a profile, workers run in parallel.**
- **Within a worker, addons run serially** (ordering matters: operator before
  cluster, CRDs before CRs).
- **Global workers run after profiles** (cross-cluster addons: rbd-mirror, volsync).

Concurrency is implemented with `errgroup`. A `Group` is itself a `Step`:

- `Group.Done` = all children done.
- `Group.Do` = ensure children under the group's policy (serial or parallel).

So the same `Ensure` logic and checkpoint reporting apply at every level: env →
profile → worker → addon → the sub-steps inside an addon (apply CRDs → wait
established → apply operator → wait rollout, each its own checkpoint).

### Checkpoint reporting

Each step emits a line as it is ensured:

```
✓ minikube/dr1 running            (skipped, already satisfied)
⟳ addon/rook-operator            acting…
✓ addon/rook-operator            done in 12.3s
✗ addon/velero                   failed: deployment not ready
```

A `status` command runs every `Done()` read-only and reports satisfied vs.
missing — a pure expression of the ensure model, with no side effects.

### Config

The existing envfile YAML (`test/envs/*.yaml`) is reused verbatim. Go structs
unmarshal the same schema (`name`, `ramen`, `templates`, `profiles`, `workers`,
template expansion of `$vm` / `$network` / `$name`, addon `args`, etc.). No new
format is introduced. This keeps `drenv-go` drop-in and lets us diff its behavior
against Python `drenv` on identical inputs.

### CLI surface

cobra-based, grown incrementally:

- MVP: `drenv-go start --envfile <f>`, `drenv-go delete --envfile <f>`,
  `drenv-go status --envfile <f>`.
- Later: `stop`, `load`, `suspend` / `resume`, `gather`, `cache` — parity with
  the Python subcommands.

### Providers

A `Provider` interface wraps the provisioning CLIs:

```go
type Provider interface {
    Exists(profile Profile) (bool, error)
    Start(ctx context.Context, profile Profile) error
    Stop(ctx context.Context, profile Profile) error
    Delete(ctx context.Context, profile Profile) error
    Status(ctx context.Context, profile Profile) (Status, error)
    LoadImage(ctx context.Context, profile Profile, image string) error
}
```

- **minikube first** (primary on macOS and Linux CI).
- lima and external providers follow, each a separate checkpoint.

Provider operations are themselves expressed as steps (e.g. "cluster running" =
`Done` checks `minikube status`, `Do` runs `minikube start`).

### Addons

Each addon is a Go package exposing its steps, wrapping `kubectl` / `kustomize`
through shared primitives:

- `apply(kustomization)` / `applyManifest(path)`
- `waitRollout(deployment)`
- `waitCondition(resource, condition)`
- `waitEstablished(crd)`

Addons are ported one at a time, diffed against the corresponding Python hook
(`test/drenv/addons/<name>/{start,test,stop}`). Until an addon exists in Go, the
envs that need it cannot fully start — so envs are sequenced smallest-first.

## Module layout

```
test/drenv-go/
  go.mod                 # new, 4th module
  tools.mod / tools.sum  # pinned workflow tools (mage, golangci-lint, ...)
  Makefile               # thin: delegates to mage
  magefile.go            # build / test / lint / clean workflow logic
  cmd/drenv-go/main.go   # cobra entry point
  internal/
    ensure/              # Step, Ensure, Group, Result, reporter
    envfile/             # YAML parse + template expansion (mirrors envfile.py)
    provider/            # Provider interface + minikube (then lima, external)
    cli/                 # exec wrappers: kubectl, kustomize, minikube
    addons/<name>/       # one package per addon, ported incrementally
```

The root `Makefile` gets thin targets delegating into the module (e.g.
`make drenv-go` → build). Existing `drenv` targets are untouched.

## Incremental rollout (each milestone is usable and committable)

1. **Skeleton** — `test/drenv-go/` (go.mod, mage, tools.mod, Makefile wiring),
   cobra, `ensure` package (`Step` / `Ensure` / `Group` / reporter), envfile
   parser. Verifiable: `drenv-go status` on a parsed env prints the step tree.
2. **minikube + lifecycle** — provider + cluster steps; `start` / `delete` /
   `status` stand up bare clusters for a trivial env.
3. **kubectl/kustomize wrappers + addon primitives** — generic apply / wait
   steps.
4. **Addons, smallest env first** — e.g. `minio.yaml` → `example.yaml` → … →
   `regional-dr.yaml`, one addon per commit, each diffed against its Python hook.
5. **Remaining commands & providers** — `stop`, `load`, `suspend`/`resume`,
   `gather`, `cache`; lima and external providers.

## Testing

- Unit tests for `ensure` (skip/act/verify/failure paths, group serial/parallel
  semantics) and `envfile` (parse + template expansion against existing
  `test/envs/*.yaml`), using table tests; no cluster required.
- CLI wrappers tested via a small command-runner seam that can be faked.
- Parity validation: run `drenv-go start` and Python `drenv start` on the same
  envfile and compare resulting cluster state — done per addon as it is ported.
- mage `test` target runs the module's unit tests; `lint` runs golangci-lint.

## Documentation

`rtalur-readme.md` at the repo root is the living dev-loop doc: the checkpoint
philosophy, how `drenv-go` fits alongside `localrun` / e2e / unit tests in the
inner loop, and a parity status table (which envs/addons work in Go yet). It is
updated as each milestone lands.

## Risks & open questions

- **Parity drift while incremental:** mitigated by diffing each ported addon
  against its Python hook and by smallest-env-first sequencing.
- **CLI output parsing fragility** (e.g. `minikube status` text): isolate parsing
  in the `cli` package behind typed results so it is testable and swappable.
- **macOS vs Linux provider differences** (vfkit/vmnet vs kvm2): the provider
  interface abstracts these; minikube-first keeps the matrix small initially.
