<!--
SPDX-FileCopyrightText: The RamenDR authors
SPDX-License-Identifier: Apache-2.0
-->

# drenv-go Milestone 4: Addon execution framework + addon ports

> TDD for the framework (unit tests via FakeRunner). Addons validated on REAL clusters.

**Goal:** Run an environment's workers/addons to parity with Python drenv. Build the addon framework, then port every regional-dr addon, reusing the existing manifests under `test/drenv/addons/<name>/`.

**Validation:** unit tests for the framework + primitives; real-cluster `drenv-go start` for the addons (three minikube clusters dr1/dr2/hub exist; destroyable). lima/external out of scope per user. Mark each addon in `rtalur-readme.md` as ✅ cluster-validated or 🚧 ported-pending-validation honestly.

---

## Architecture

### Addon model
An **addon** is identified by name and produces an `ensure.Step` given its runtime deps, the target cluster (context), and its args. Most addons are a serial sequence of primitive ops. A **registry** maps addon name → builder:

```go
// internal/addon
type Deps struct {
    K        *cli.Kubectl
    MK       *cli.Minikube   // rarely needed by addons
    Helm     *cli.Helm
    Clusteradm *cli.Clusteradm
    Subctl   *cli.Subctl
    MC       *cli.MC
    Velero   *cli.Velero
    AddonsDir string         // path to test/drenv/addons
    Opts     ensure.Options
}

// Builder returns the Step that ensures the addon on `cluster` with `args`.
type Builder func(d Deps, cluster string, args []string) ensure.Step

func Register(name string, b Builder)
func Lookup(name string) (Builder, bool)
```

Each addon registers itself (in its own file) via `func init()` or an explicit registration list. Builders compose primitive Steps.

### Primitive Steps (internal/addon/primitives.go)
Small `ensure.Step` constructors built on `cli.Kubectl`:
- `ApplyKustomization(d, name, context, dir)` — Done: (cheap—re-apply is idempotent; Done returns false then Do applies; or check a sentinel). For idempotency without a reliable pre-check, model apply as: Done=false always is wrong (never skips). Instead, an addon's Done should be its WAIT condition. So prefer composing: an addon Step's `Done` = "all its readiness checks pass"; `Do` = apply + (the apply's own waits). Implement an addon as a serial Group of sub-steps where each sub-step is apply-or-wait with its own Done. For apply sub-steps, Done returns false (always applies — apply is idempotent and cheap) — acceptable. For wait sub-steps, Done = the condition. NOTE: to keep re-runs cheap, the FINAL readiness check gates the group: if a Group's children are all Done, the group is skipped. So design each addon group so its wait children's Done reflect real readiness; apply children Done=false means apply always runs, but that's cheap and idempotent. (This matches drenv: it re-applies on every start.)
- `WaitRollout`, `WaitCondition` (--for=condition=X / jsonpath / create), `Exec`, `Get`, `Patch`, `Annotate`, `Label` — thin Step or helper wrappers over Kubectl.
- `ApplyTemplate(d, name, context, file, vars)` — read file, substitute `$var`, apply via stdin.

Extend `cli.Kubectl` with the verbs the recipes need: `ApplyFile`, `ApplyKustomizeDir` (apply -k), `ApplyStdin`, `WaitFor` (generic --for), `RolloutStatus`, `Get` (jsonpath), `Exec`, `Patch`, `Annotate`, `Label`, and a `Watch`/poll helper. All accept `--context`. Unit-test argv via FakeRunner.

### New CLI wrappers (internal/cli), all over Runner, argv unit-tested:
- `Helm` — `RepoAdd(name,url)`, `UpgradeInstall(release, chart, kubeContext, flags...)`
- `Clusteradm` — `Init(...)`, `Get(what,...)`, `Join(...)`, `Addon(action,names,clusters,context)`, `Install(what,names,context)`
- `Subctl` — `DeployBroker(context, globalnet, brokerInfo, version)`, `Join(brokerInfo, context, clusterID, cableDriver, version)`
- `MC` — `SetAlias(name,url,key,secret)`, `MakeBucket(target, ignoreExisting)`
- `Velero` — `Install(flags...)`
- minio helper: `ServiceURL(k, context)` and `WaitForService(...)` (kubectl get hostIP + nodePort, HTTP poll).

### Composition (build.Start)
Wire the M4 seam: for each profile, after its cluster-running step, add a parallel group of its workers; each worker is a SERIAL group of its addon Steps (looked up via the registry, with `$name`-expanded args from envfile). After all profiles (serial barrier), add a parallel group of the env's GLOBAL workers (each serial over its addons). This matches drenv: profiles parallel; workers parallel within a profile; addons serial within a worker; globals after profiles.

`addonsDir` resolves relative to the envfile: `filepath.Join(dir(envfile), "..", "drenv", "addons")`, overridable by a `--addons-dir` flag.

---

## Tasks

### Task 1: extend cli.Kubectl + new CLI wrappers
Extend `Kubectl` with ApplyFile/ApplyKustomizeDir/ApplyStdin/WaitFor/RolloutStatus/Get/Exec/Patch/Annotate/Label/Watch (all `--context`). Add `Helm`, `Clusteradm`, `Subctl`, `MC`, `Velero` wrappers. Unit-test all argv via FakeRunner. Commit per wrapper or as one cohesive commit.

### Task 2: internal/addon framework
`Deps`, `Builder`, `Register`/`Lookup`, primitive Step constructors, `ApplyTemplate`, minio helper. Unit-test the registry and a representative primitive (e.g. ApplyTemplate substitution; a wait Step's Done via a fake Kubectl seam). Commit.

### Task 3: port the simple addons (kustomize/manifest)
Register: external-snapshotter, odf-external-snapshotter, olm (server-side CRDs), recipe, csi-addons, ocm-controller, minio (apply + mc alias/bucket). Each reuses `test/drenv/addons/<pkg>` dirs/files and the exact waits from the spec. Commit grouped.

### Task 4: wire build.Start worker/addon composition + status tree
Compose worker/addon groups per the architecture. Update `status` to render the full step tree (env→profile→worker→addon) using registry lookups, falling back to listing addon names. Unit-test composition with a fake registry. Commit.

### Task 5: port CLI-driven addons
velero, volsync (helm), ocm-hub / ocm-cluster (clusteradm, cross-cluster), submariner (subctl, cross-cluster, broker-info file under ~/.config/drenv/<env>/submariner), argocd (cluster add, cross-cluster). Commit grouped.

### Task 6: port the rook suite + rbd-mirror
rook operator/cluster/toolbox/pool/cephfs (templated pool/cephfs), rbd-mirror (ceph-via-exec, cross-cluster secret exchange, templated vrc/secret, mirroring health poll). Commit grouped.

### Task 7: real-cluster validation + parity table
`drenv-go start --envfile test/envs/<env>` against dr1/dr2/hub, smallest-first, fixing port bugs found. Record which addons reach Ready. Update `rtalur-readme.md` parity table honestly. Commit.

---

## Notes
- Reuse existing manifests; do NOT re-author addon YAML. kubectl `-k` replaces the Python cache+kustomize step.
- Cross-cluster addons (ocm-cluster, submariner, argocd, rbd-mirror, volsync) act on clusters beyond their worker's profile — the Step closes over the needed contexts from args.
- Idempotency: addons re-apply on every run (matches drenv); the wait sub-steps make re-runs converge and a fully-ready addon's waits pass quickly.
