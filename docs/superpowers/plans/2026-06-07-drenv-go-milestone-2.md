<!--
SPDX-FileCopyrightText: The RamenDR authors
SPDX-License-Identifier: Apache-2.0
-->

# drenv-go Milestone 2: Provider + cluster lifecycle Implementation Plan

> **For agentic workers:** Use TDD. Each task is a small commit. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Make `drenv-go start`, `delete`, and `status` actually provision and tear down clusters via the minikube CLI, expressed as ensure Steps composed into the env tree.

**Architecture:** A `cli.Runner` seam wraps command execution (real `exec` impl + a fake for tests). A `provider.Provider` wraps minikube. Cluster operations are `ensure.Step`s (`Done` = minikube reports running; `Do` = minikube start). A builder turns an `envfile.Env` into the Group tree (profiles in parallel) and the commands ensure it with a `ConsoleReporter`.

**Tech Stack:** Go 1.25, builds on Milestone 1's `ensure` and `envfile` packages.

---

## File Structure

```
test/drenv-go/internal/
  cli/
    runner.go         # Runner interface, Exec (real), SPDX
    runner_test.go    # Exec runs a real trivial command (echo/true)
    fake.go           # FakeRunner records calls, returns scripted outputs/errors
    minikube.go       # Minikube wrapper: Status/Start/Stop/Delete/LoadImage over a Runner
    minikube_test.go  # table tests using FakeRunner
  provider/
    provider.go       # Provider interface, Profile->args mapping helpers, Status enum
    minikube.go       # MinikubeProvider implementing Provider over cli.Minikube
    minikube_test.go
    steps.go          # ClusterRunningStep / ClusterAbsentStep (ensure.Step)
    steps_test.go
  build/
    build.go          # Start(env,provider,opts) ensure.Step ; Delete(env,provider,opts) ensure.Step
    build_test.go
cmd/drenv-go/
  start.go            # `start` subcommand
  delete.go           # `delete` subcommand
  status.go           # MODIFY: Done()-driven status using the build tree
```

Shared types introduced (use consistently):
- `cli.Runner` — `Run(ctx, name string, args ...string) error`, `Output(ctx, name string, args ...string) (string, error)`
- `cli.Exec` (real), `cli.FakeRunner` (test)
- `cli.Minikube{ R Runner }` with `Status(ctx, profile string) (provider.Status, error)` — NO, avoid import cycle: put `Status` enum in `provider`, and have `cli.Minikube` return a raw struct; provider maps it. To keep it simple: `cli.Minikube.Status` returns `(cli.MinikubeStatus, error)` where `MinikubeStatus` has `Host, APIServer string`. Provider interprets.
- `provider.Status` — `StatusRunning`, `StatusStopped`, `StatusNotFound`, `StatusUnknown`
- `provider.Provider` — `Exists(ctx, profile string) (bool, error)`, `Start(ctx, p envfile.Profile) error`, `Stop(ctx, profile string) error`, `Delete(ctx, profile string) error`, `Status(ctx, profile string) (Status, error)`, `LoadImage(ctx, profile, image string) error`
- `provider.ClusterRunningStep(p Provider, prof envfile.Profile) ensure.Step`
- `provider.ClusterAbsentStep(p Provider, prof envfile.Profile) ensure.Step`
- `build.Start(e *envfile.Env, p provider.Provider, opts ensure.Options) ensure.Step`
- `build.Delete(e *envfile.Env, p provider.Provider, opts ensure.Options) ensure.Step`

SPDX headers + gci import order on every file.

---

## Task 1: cli.Runner seam + Exec + FakeRunner

**Files:** Create `internal/cli/runner.go`, `internal/cli/fake.go`, `internal/cli/runner_test.go`.

- [ ] TDD. `Runner` interface (`Run`, `Output`). `Exec` shells out via `os/exec` with `CommandContext`; `Run` wires stdout/stderr to the process's; `Output` returns trimmed combined-or-stdout output and wraps errors with the command line. `FakeRunner` records every `(name, args)` call and returns scripted results keyed by call index or by command name; expose `.Calls` for assertions.
- [ ] Tests: `Exec.Output(ctx, "echo", "hi")` returns `"hi"`; `Exec.Run(ctx, "true")` succeeds, `Exec.Run(ctx, "false")` errors. `FakeRunner` records calls and replays scripted output/err.
- [ ] `go test ./internal/cli/...`, `make lint`. Commit.

## Task 2: cli.Minikube wrapper

**Files:** Create `internal/cli/minikube.go`, `internal/cli/minikube_test.go`.

- [ ] `Minikube{ R Runner }`. Methods: `Status(ctx, profile) (MinikubeStatus, error)` runs `minikube status -p <profile> -o json` and parses JSON (`Host`, `APIServer`, `Name`); a non-zero exit with "not found"/profile-missing maps to a zero-value status with `Host=""` (caller interprets as not-found) rather than a hard error — detect via output. `Start(ctx, args...)`, `Stop(ctx, profile)`, `Delete(ctx, profile)`, `LoadImage(ctx, profile, image)` run the corresponding minikube subcommands via the Runner.
- [ ] Tests via FakeRunner: Status parses a JSON sample into `{Host:"Running",APIServer:"Running"}`; the "profile not found" output yields an empty Host without error; Start/Stop/Delete/LoadImage issue the expected `minikube` argv (assert against `FakeRunner.Calls`).
- [ ] `go test`, `make lint`. Commit.

## Task 3: provider.Provider + MinikubeProvider + Status enum

**Files:** Create `internal/provider/provider.go`, `internal/provider/minikube.go`, `internal/provider/minikube_test.go`.

- [ ] `Status` enum (`StatusRunning`/`StatusStopped`/`StatusNotFound`/`StatusUnknown`) with `String()`. `Provider` interface as listed above. `MinikubeProvider{ MK *cli.Minikube }` implements it:
  - `Status`: maps `cli.MinikubeStatus` → enum (Host=="Running" && APIServer=="Running" → Running; Host=="Stopped" → Stopped; empty/not-found → NotFound; else Unknown).
  - `Exists`: Status != NotFound.
  - `Start(ctx, prof)`: builds `minikube start` args from the profile — `-p <name>`, and when set: `--driver <driver>` (skip if it's an unresolved `$vm`/`$network` placeholder — treat a leading `$` as "let minikube default it", i.e. omit), `--cpus`, `--memory`, `--network`. Delegates to `cli.Minikube.Start`.
  - `Stop`/`Delete`/`LoadImage`: delegate.
- [ ] Tests via FakeRunner-backed `cli.Minikube`: Status mapping table; Start omits `$`-prefixed driver/network and includes resolved values + cpus/memory; Exists true/false.
- [ ] `go test`, `make lint`. Commit.

## Task 4: cluster lifecycle Steps

**Files:** Create `internal/provider/steps.go`, `internal/provider/steps_test.go`.

- [ ] `ClusterRunningStep(p Provider, prof envfile.Profile) ensure.Step`: `Name()` = `"cluster/" + prof.Name`; `Done` = `p.Status(...) == StatusRunning`; `Do` = `p.Start(ctx, prof)`. `ClusterAbsentStep(...)`: `Name()` = `"cluster/" + prof.Name + " absent"`; `Done` = `p.Status(...) == StatusNotFound`; `Do` = `p.Delete(...)`. Use small unexported structs implementing `ensure.Step`.
- [ ] Tests with a fake `provider.Provider` (in-memory state): a not-running cluster becomes running after `Ensure(ClusterRunningStep)`; an already-running one is Skipped (Start not called); `ClusterAbsentStep` deletes a present cluster and skips an absent one.
- [ ] `go test`, `make lint`. Commit.

## Task 5: build (env → Steps)

**Files:** Create `internal/build/build.go`, `internal/build/build_test.go`.

- [ ] `Start(e, p, opts)`: returns a serial top-level `ensure.Group` named `e.Name` whose first child is a parallel group `"clusters"` of `ClusterRunningStep` per profile. (Workers/addons are Milestone 3 — leave a clearly-named seam, e.g. a comment where the worker groups will be appended, but do NOT stub fake work.) `Delete(e, p, opts)`: a parallel group of `ClusterAbsentStep` per profile.
- [ ] Tests with the fake provider: `Ensure(Start(env,...))` starts all profiles' clusters (assert all running); `Ensure(Delete(env,...))` removes them. Use the sample env from a small inline `envfile.Env` value (don't depend on YAML here).
- [ ] `go test`, `make lint`. Commit.

## Task 6: wire start / delete / status commands

**Files:** Create `cmd/drenv-go/start.go`, `cmd/drenv-go/delete.go`; modify `cmd/drenv-go/status.go` and register the new commands in `main.go`.

- [ ] A small helper (in `cmd/drenv-go`) constructs a `provider.MinikubeProvider{ MK: &cli.Minikube{R: cli.Exec{}} }` and loads the env. `start`: load env, build `build.Start`, `ensure.Ensure(ctx, step, opts)` with `opts.Reporter = ensure.ConsoleReporter{W: os.Stdout}` and `ensure.DefaultOptions()` values; non-zero exit on error. `delete`: same with `build.Delete`. `status`: build the Start tree and, for each profile, call `provider.Status` and print running/stopped/not-found per cluster (Done()-driven) — keep the `--envfile` required check. Register `start`, `delete` in `main.go`.
- [ ] Build; `./bin/drenv-go --envfile ../envs/regional-dr.yaml status` runs without needing clusters (prints each cluster's status, likely "not found"). `go test ./...`, `make build`, `make lint` clean. Commit.

## Task 7: readme parity update

- [ ] Flip the "minikube provider + cluster lifecycle (`start`/`delete`/`status`)" row to ✅ (or 🚧 if clusters not yet smoke-tested) in `rtalur-readme.md`. Note in the file that cluster start is unit-tested via a fake runner; real-cluster smoke test pending. Commit.

---

## Self-Review
- Coverage: provider, lifecycle steps, env→steps, and the three commands are all present and unit-tested via the runner/provider fakes — no cluster required to pass tests.
- Scope: workers/addons deferred to Milestone 3 (only a commented seam in `build.Start`). No addon/kubectl logic here beyond what `start` needs.
- Consistency: `Status` enum lives in `provider` (no import cycle with `cli`); `cli.Minikube` returns a raw `MinikubeStatus`. SPDX + gci everywhere.
