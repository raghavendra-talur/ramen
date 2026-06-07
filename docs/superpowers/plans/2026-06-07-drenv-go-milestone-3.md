<!--
SPDX-FileCopyrightText: The RamenDR authors
SPDX-License-Identifier: Apache-2.0
-->

# drenv-go Milestone 3: Command surface + kubectl foundation

> TDD. Small commits. Builds on M1/M2.

**Goal:** Complete the cluster-lifecycle command surface (`stop`, `load`, `suspend`, `resume`, `dump`) to parity with the Python drenv, and add the `cli.Kubectl` wrapper that the addon layer (M4) will build on.

**Scope note:** This milestone intentionally does NOT build the addon-execution framework or port addons. drenv's addons are heterogeneous (kubectl/kustomize/helm/scripts/exec), so that abstraction is deferred to M4 where it can be designed against real addon definitions and validated on clusters. M3 finishes everything verifiable without per-addon guessing.

---

## Task 1: extend cli.Minikube + provider for pause/unpause/stop/load

**Files:** modify `internal/cli/minikube.go` (+test), `internal/provider/provider.go`, `internal/provider/minikube.go` (+test).

- [ ] Add `cli.Minikube.Pause(ctx, profile)` → `minikube pause -p <profile>` and `Unpause(ctx, profile)` → `minikube unpause -p <profile>`. (`Stop`, `Delete`, `LoadImage` already exist.)
- [ ] Extend `provider.Provider` with `Suspend(ctx, profile string) error` and `Resume(ctx, profile string) error`. Implement on `MinikubeProvider` delegating to `MK.Pause`/`MK.Unpause`. (`Stop`/`Delete`/`LoadImage` already on the interface — verify.)
- [ ] Tests via FakeRunner: Pause/Unpause issue correct argv; provider Suspend/Resume delegate.
- [ ] `go test ./...`, `make lint`. Commit.

## Task 2: cli.Kubectl wrapper

**Files:** create `internal/cli/kubectl.go` (+test).

- [ ] `Kubectl{ R Runner }`. Cluster selection is by `--context <name>` (minikube sets the kube context to the profile name). Methods:
  - `Apply(ctx, kubeContext string, args ...string) error` → `kubectl --context <ctx> apply <args...>`
  - `ApplyKustomization(ctx, kubeContext, dir string) error` → `kubectl --context <ctx> apply -k <dir>`
  - `WaitRollout(ctx, kubeContext, namespace, resource string, timeout time.Duration) error` → `kubectl --context <ctx> -n <ns> rollout status <resource> --timeout <Ns>`
  - `WaitCondition(ctx, kubeContext, namespace, resource, condition string, timeout time.Duration) error` → `kubectl --context <ctx> -n <ns> wait <resource> --for=condition=<cond> --timeout <Ns>`
  - `Get(ctx, kubeContext, namespace, args ...string) (string, error)` → `kubectl --context <ctx> -n <ns> get <args...> -o ...` (return Output)
  Format `timeout` as `<seconds>s`.
- [ ] Tests via FakeRunner: assert exact argv for each method (timeout formatting, context, namespace). No real cluster.
- [ ] `go test`, `make lint`. Commit.

## Task 3: wire stop / load / suspend / resume / dump commands

**Files:** create `cmd/drenv-go/stop.go`, `load.go`, `suspend.go`, `resume.go`, `dump.go`; register in `main.go`. Reuse `loadEnv`/`newMinikubeProvider` helpers.

- [ ] `stop`: for each profile (parallel via an ensure group of "cluster stopped" steps, OR simply iterate calling `prov.Stop`), stop the cluster. Use the ensure model for consistency: add `provider.ClusterStoppedStep` (Done = Status==StatusStopped || NotFound; Do = Stop) in `provider/steps.go` (+test), and a `build.Stop(env, p, opts)` parallel group. Wire the command with a ConsoleReporter.
- [ ] `load`: `--image <ref>` flag (required); for each profile call `prov.LoadImage(ctx, profile, image)`. (Loading is not naturally idempotent; a plain iteration with error aggregation is fine — no ensure Step needed. Document why.)
- [ ] `suspend`/`resume`: iterate profiles calling `prov.Suspend`/`prov.Resume`. (minikube pause/unpause; plain iteration.)
- [ ] `dump`: load env and print it as YAML to stdout (use the same yaml lib). Read-only.
- [ ] Register all in `main.go`. Build; `--help` for each works. `./bin/drenv-go --envfile ../envs/regional-dr.yaml dump` prints the expanded env. `go test ./...`, `make build`, `make lint` clean. Commit.

## Task 4: readme parity update

- [ ] In `rtalur-readme.md`, update the "Remaining commands" row: mark `stop`/`load`/`suspend`/`resume`/`dump` done, leaving `gather`/`cache` as the remainder. Add a row or note that the addon-execution framework + addon ports are the next milestone. Commit.

---

## Self-Review
- Command surface reaches parity except `gather`/`cache`/`setup`/`cleanup` (infra-heavy; deferred with a note).
- `cli.Kubectl` is unit-tested via FakeRunner and ready for M4 addons; not yet used by any command (no dead-code concern — it's the documented M4 foundation, mirroring how `ensure` preceded its consumers).
- No speculative addon abstraction built. Scope honest.
