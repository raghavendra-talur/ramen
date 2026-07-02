# simtest State-Space E2E Framework Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `simtest/`, a self-contained e2e framework that runs the unmodified `bin/manager` binary against three envtest control planes with framework-simulated external actors, plus a test collection (baselines, fault matrix, ordering, crash/recovery).

**Architecture:** A `world` package boots hub/dr1/dr2 envtest apiservers, seeds bootstrap objects, starts an in-process fake S3, and launches the real ramen hub and dr-cluster operators as subprocesses. An `actors` package runs framework-side controllers that fulfill every external contract (OCM work/view agent, csi-addons VolumeReplication, PVC binding, finalizer GC), each gated by a mutable fault-injection policy. `observe`, `user`, `invariants`, and `tests` drive scenarios and assert global safety.

**Tech Stack:** Go, controller-runtime + envtest (k8s 1.33.0 assets), `github.com/ramendr/ramen/api` types, `open-cluster-management.io/api`, `github.com/stolostron/multicloud-operators-foundation` (ManagedClusterView), `github.com/csi-addons/kubernetes-csi-addons` (VolumeReplication), `github.com/johannesboyne/gofakes3` (in-process S3).

## Global Constraints

- **Do not modify any existing file.** All new code lives under `simtest/`. The only files outside it are this plan and the spec (already committed).
- New Go module `github.com/ramendr/ramen/simtest`, `go 1.25.0`, with `replace github.com/ramendr/ramen/api => ../api`.
- Every Go file starts with the SPDX header:
  ```go
  // SPDX-FileCopyrightText: The RamenDR authors
  // SPDX-License-Identifier: Apache-2.0
  ```
- For shared dependencies (`sigs.k8s.io/controller-runtime`, `k8s.io/*`, `open-cluster-management.io/api`, `github.com/stolostron/multicloud-operators-foundation`, `github.com/csi-addons/kubernetes-csi-addons`), pin the same versions as the root `go.mod` (check with `grep <module> ../go.mod`), then `go mod tidy`.
- Format with `gofmt -w` before each commit.
- Commit after every task with `git commit -s`; end every commit message body with `Assisted-by: Claude Code/claude-fable-5`. No `Co-Authored-By` trailers. Work on branch `simtest-framework`.
- envtest binaries: kubernetes 1.33.0 via the existing `hack/install-setup-envtest.sh` (writes the assets path to `<repo>/testbin/testassets.txt`). Never edit that script.
- The ramen binary contract (verified against the code):
  - env `RAMEN_CONTROLLER_TYPE` = `dr-hub` | `dr-cluster` (mandatory), `POD_NAMESPACE` = `ramen-system` (must exist), `RAMEN_RECONCILERS` allow-list, `RAMEN_METRICS_BIND_ADDRESS=0`, `RAMEN_HEALTH_BIND_ADDRESS=0`.
  - `--kubeconfig=<path>` flag (registered by controller-runtime).
  - Config is read from ConfigMap `ramen-hub-operator-config` / `ramen-dr-cluster-operator-config` in `POD_NAMESPACE`, key `ramen_manager_config.yaml` (the `--config` flag is inert). Pre-create it with `leaderElect: false`.
- Scope guards (defer, do not build): VolSync/Velero/snapshot/fence actors, Metro-DR, consistency groups, recipes, `DropNext` policy, YAML scenario files. RamenConfig sets `volSync.disabled: true` and `kubeObjectProtection.disabled: true`, so those flows never run in v1.

## Deferred (documented, not in this plan)

VolSync, Velero, snapshotter, and NetworkFence actors (the spec's full actor table); drenv-backed World; randomized exploration. The `actors.Policy` and `actors.ClusterRef` interfaces are the extension points; porting recipes live in `internal/controller/mock/` (copy, never import — `internal/` is not importable across modules).

## File Structure

```
simtest/
├── go.mod, go.sum
├── Makefile                 # make simtest → build manager, ensure assets, go test
├── .gitignore               # .artifacts/
├── README.md
├── world/
│   ├── consts.go            # names, namespaces, labels, profiles
│   ├── paths.go             # RepoRoot(), EnsureAssets()
│   ├── scheme.go            # NewScheme() with all needed types
│   ├── cluster.go           # StartCluster: envtest + CRDs + kubeconfig file
│   ├── s3.go                # gofakes3 server with outage switch
│   ├── ramenconfig.go       # RamenConfig YAML + operator ConfigMap builders
│   ├── process.go           # ManagerProcess: start/kill/restart/logs
│   ├── bootstrap.go         # namespaces, ManagedClusters, classes, DRClusters, DRPolicy
│   └── world.go             # World assembly + teardown + shared test singleton
├── actors/
│   ├── policy.go            # Policy types + Store (fault injection)
│   ├── evlog.go             # per-world event log
│   ├── runtime.go           # one manager per cluster, actor registration
│   ├── pvbinder.go          # binds PVCs/PVs (no KCM in envtest)
│   ├── janitor.go           # strips pvc/pv-protection finalizers
│   ├── volrep.go            # csi-addons VolumeReplication fulfiller (ported)
│   └── ocmagent.go          # ManifestWork applier + ManagedClusterView fulfiller
├── observe/
│   ├── recorder.go          # watch-based DRPC progression/phase recorder + gates
│   └── ready.go             # WaitDRPCReady (Available+PeerReady+Completed+LastGroupSyncTime)
├── user/
│   └── actions.go           # CreateApp, EnableProtection, Failover, Relocate, DeleteApp, Disable
├── invariants/
│   └── checker.go           # single-primary invariant + progression edge recording
└── tests/
    ├── main_test.go         # TestMain: shared world
    ├── baseline_test.go     # T1 + reusable baseline drivers with hooks
    ├── matrix_test.go       # T2 fault matrix (+ T3 ordering variants)
    └── recovery_test.go     # T4 operator crash/recovery
```

Dependency direction: `tests → user/observe/invariants/world/actors`; `world → actors` (starts the runtime); `actors` imports nothing from simtest except itself. No package imports `tests`.

---

### Task 1: Module scaffold, Makefile, paths

**Files:**
- Create: `simtest/go.mod`, `simtest/Makefile`, `simtest/.gitignore`, `simtest/world/consts.go`, `simtest/world/paths.go`, `simtest/world/paths_test.go`

**Interfaces:**
- Produces: `world.RepoRoot() string`, `world.EnsureAssets(t *testing.T)`, and all shared constants (used by every later task).

- [ ] **Step 1: Create module and Makefile**

`simtest/go.mod` (versions for the `require` lines: copy from `../go.mod`; run `go mod tidy` afterwards to fill the rest):

```
module github.com/ramendr/ramen/simtest

go 1.25.0

replace github.com/ramendr/ramen/api => ../api

require (
	github.com/csi-addons/kubernetes-csi-addons v0.12.0
	github.com/johannesboyne/gofakes3 v0.0.0-20240701191259-edd0227ffc37
	github.com/ramendr/ramen/api v0.0.0-00010101000000-000000000000
	github.com/stolostron/multicloud-operators-foundation v1.0.0-2021-01-01-07-04-13.0.20220502062803-4cae02c2ff86
	k8s.io/api v0.33.0
	k8s.io/apimachinery v0.33.0
	k8s.io/client-go v0.33.0
	open-cluster-management.io/api v0.13.0
	sigs.k8s.io/controller-runtime v0.21.0
	sigs.k8s.io/yaml v1.4.0
)
```

(The versions above are indicative; the authoritative source is `../go.mod`. After writing, run `go mod tidy` and commit the resolved `go.sum`.)

`simtest/Makefile`:

```make
ROOT := ..

.PHONY: simtest manager assets

simtest: manager assets
	go test ./tests/... -v -timeout 90m

manager:
	$(MAKE) -C $(ROOT) build

assets:
	cd $(ROOT) && hack/install-setup-envtest.sh
```

`simtest/.gitignore`:

```
.artifacts/
```

- [ ] **Step 2: Write the failing test**

`simtest/world/paths_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRepoRoot(t *testing.T) {
	root := RepoRoot()
	for _, p := range []string{"config/crd/bases", "hack/test", "cmd/main.go"} {
		if _, err := os.Stat(filepath.Join(root, p)); err != nil {
			t.Fatalf("RepoRoot()=%q missing %s: %v", root, p, err)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `cd simtest && go test ./world/ -run TestRepoRoot -v`
Expected: FAIL (compile error: `RepoRoot` undefined)

- [ ] **Step 4: Implement paths and constants**

`simtest/world/paths.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// RepoRoot returns the ramen repository root (parent of simtest/).
func RepoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)

	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

// EnsureAssets sets KUBEBUILDER_ASSETS from <repo>/testbin/testassets.txt,
// mirroring internal/controller/suite_test.go. Skips the test with guidance
// if assets are missing.
func EnsureAssets(t *testing.T) {
	t.Helper()

	if _, set := os.LookupEnv("KUBEBUILDER_ASSETS"); set {
		return
	}

	content, err := os.ReadFile(filepath.Join(RepoRoot(), "testbin", "testassets.txt"))
	if err != nil {
		t.Skipf("envtest assets missing, run 'make assets' in simtest/: %v", err)
	}

	t.Setenv("KUBEBUILDER_ASSETS", strings.TrimSpace(string(content)))
}

// ManagerBin returns the path of the ramen manager binary.
func ManagerBin() string {
	return filepath.Join(RepoRoot(), "bin", "manager")
}
```

`simtest/world/consts.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

const (
	HubName = "hub"
	DR1Name = "dr1"
	DR2Name = "dr2"

	RamenSystemNS = "ramen-system" // POD_NAMESPACE for both operator types
	RamenOpsNS    = "ramen-ops"    // discovered-app management namespace

	DRPolicyName       = "dr-policy-1m"
	SchedulingInterval = "1m"

	StorageClassName = "mock-rbd"
	VRClassName      = "mock-rbd-vrc"
	Provisioner      = "mock.csi.ramen.io"
	ReplicationID    = "mock-replication-rbd"

	// Label keys ramen matches on (see internal/controller/volumereplicationgroup_controller.go).
	StorageIDLabel     = "ramendr.openshift.io/storageid"
	ReplicationIDLabel = "ramendr.openshift.io/replicationid"

	S3SecretName    = "ramen-s3-secret"
	S3AccessKey     = "simtest"
	S3SecretKey     = "simtest123"
	S3ProfilePrefix = "s3-" // profile name: s3-dr1, s3-dr2; bucket: bucket-dr1, ...

	HubConfigMapName = "ramen-hub-operator-config"
	DRConfigMapName  = "ramen-dr-cluster-operator-config"
	ConfigMapKey     = "ramen_manager_config.yaml"

	// OCM annotation that stops OCM from scheduling; ramen owns PlacementDecisions.
	OcmSchedulingDisable = "cluster.open-cluster-management.io/experimental-scheduling-disable"

	AppLabelKey = "appname" // pvcSelector label key, mirrors e2e
)

func StorageID(cluster string) string  { return "mock-rbd-" + cluster }
func S3Profile(cluster string) string  { return S3ProfilePrefix + cluster }
func S3Bucket(cluster string) string   { return "bucket-" + cluster }
```

- [ ] **Step 5: Run test to verify it passes**

Run: `cd simtest && go mod tidy && go test ./world/ -run TestRepoRoot -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add simtest/
git commit -s -m "simtest: module scaffold, paths, constants

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 2: Scheme and envtest cluster bring-up

**Files:**
- Create: `simtest/world/scheme.go`, `simtest/world/cluster.go`, `simtest/world/cluster_test.go`

**Interfaces:**
- Consumes: `RepoRoot`, `EnsureAssets` (Task 1).
- Produces: `world.NewScheme() *runtime.Scheme`; `world.StartCluster(name, dir string) (*Cluster, error)`; `type Cluster struct { Name string; Env *envtest.Environment; Cfg *rest.Config; Client client.Client; KubeconfigPath string }`; `(*Cluster).Stop() error`.

- [ ] **Step 1: Write the failing test**

`simtest/world/cluster_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"context"
	"os"
	"testing"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

func TestStartCluster(t *testing.T) {
	EnsureAssets(t)

	c, err := StartCluster("dr1", t.TempDir())
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })

	if _, err := os.Stat(c.KubeconfigPath); err != nil {
		t.Fatalf("kubeconfig not written: %v", err)
	}

	// Ramen CRDs must be installed and typed client usable.
	vrg := &rmn.VolumeReplicationGroup{}
	err = c.Client.Get(context.Background(), types.NamespacedName{Name: "nope", Namespace: "default"}, vrg)
	if err == nil || !isNotFound(err) {
		t.Fatalf("expected NotFound for missing VRG (CRD installed), got: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./world/ -run TestStartCluster -v`
Expected: FAIL (compile error: `StartCluster` undefined)

- [ ] **Step 3: Implement scheme and cluster**

`simtest/world/scheme.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	rmn "github.com/ramendr/ramen/api/v1alpha1"
	viewv1beta1 "github.com/stolostron/multicloud-operators-foundation/pkg/apis/view/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ocmv1 "open-cluster-management.io/api/cluster/v1"
	clrapiv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	ocmworkv1 "open-cluster-management.io/api/work/v1"
)

// NewScheme returns a scheme with every type the framework touches.
func NewScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(s))
	utilruntime.Must(rmn.AddToScheme(s))
	utilruntime.Must(ocmv1.Install(s))
	utilruntime.Must(clrapiv1beta1.Install(s))
	utilruntime.Must(ocmworkv1.Install(s))
	utilruntime.Must(viewv1beta1.AddToScheme(s))
	utilruntime.Must(volrep.AddToScheme(s))

	return s
}
```

(Note: check each Add/Install function name against the module — `ocmv1.Install`, `clrapiv1beta1.Install`, `ocmworkv1.Install`, `viewv1beta1.AddToScheme`, `volrep.AddToScheme`; the vendored packages differ in whether they export `Install` or `AddToScheme`.)

`simtest/world/cluster.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

type Cluster struct {
	Name           string
	Env            *envtest.Environment
	Cfg            *rest.Config
	Client         client.Client
	KubeconfigPath string
}

// StartCluster boots one envtest control plane with all ramen + third-party
// CRDs and writes an admin kubeconfig into dir.
func StartCluster(name, dir string) (*Cluster, error) {
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join(RepoRoot(), "config", "crd", "bases"),
			filepath.Join(RepoRoot(), "hack", "test"),
		},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		return nil, fmt.Errorf("cluster %s: envtest start: %w", name, err)
	}

	user, err := env.AddUser(envtest.User{Name: "simtest-admin", Groups: []string{"system:masters"}}, nil)
	if err != nil {
		_ = env.Stop()

		return nil, fmt.Errorf("cluster %s: add user: %w", name, err)
	}

	kc, err := user.KubeConfig()
	if err != nil {
		_ = env.Stop()

		return nil, fmt.Errorf("cluster %s: kubeconfig: %w", name, err)
	}

	kcPath := filepath.Join(dir, name+".kubeconfig")
	if err := os.WriteFile(kcPath, kc, 0o600); err != nil {
		_ = env.Stop()

		return nil, err
	}

	cl, err := client.New(cfg, client.Options{Scheme: NewScheme()})
	if err != nil {
		_ = env.Stop()

		return nil, err
	}

	return &Cluster{Name: name, Env: env, Cfg: cfg, Client: cl, KubeconfigPath: kcPath}, nil
}

func (c *Cluster) Stop() error { return c.Env.Stop() }

func isNotFound(err error) bool { return errors.IsNotFound(err) }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go mod tidy && go test ./world/ -run TestStartCluster -v`
Expected: PASS (takes ~5-15s for apiserver+etcd start)

- [ ] **Step 5: Commit**

```bash
git add simtest/world/
git commit -s -m "simtest: envtest cluster bring-up with full CRD set

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 3: In-process S3 with outage switch

**Files:**
- Create: `simtest/world/s3.go`, `simtest/world/s3_test.go`

**Interfaces:**
- Produces: `world.StartS3(buckets ...string) *S3Server`; `type S3Server struct { URL string }`; `(*S3Server).SetDown(down bool)`; `(*S3Server).Stop()`.

- [ ] **Step 1: Write the failing test**

`simtest/world/s3_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"net/http"
	"testing"
)

func TestS3Server(t *testing.T) {
	s := StartS3("bucket-dr1")
	t.Cleanup(s.Stop)

	resp, err := http.Get(s.URL + "/bucket-dr1?list-type=2")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("list bucket: err=%v status=%v", err, resp)
	}
	resp.Body.Close()

	s.SetDown(true)

	resp, err = http.Get(s.URL + "/bucket-dr1?list-type=2")
	if err != nil {
		t.Fatalf("get during outage: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 during outage, got %d", resp.StatusCode)
	}

	s.SetDown(false)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./world/ -run TestS3Server -v`
Expected: FAIL (compile error)

- [ ] **Step 3: Implement**

`simtest/world/s3.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	"github.com/johannesboyne/gofakes3"
	"github.com/johannesboyne/gofakes3/backend/s3mem"
)

// S3Server is an in-process S3 endpoint (path-style, no TLS) with an outage
// switch used by the fault matrix.
type S3Server struct {
	URL  string
	srv  *httptest.Server
	down atomic.Bool
}

func StartS3(buckets ...string) *S3Server {
	backend := s3mem.New()
	for _, b := range buckets {
		if err := backend.CreateBucket(b); err != nil {
			panic(err)
		}
	}

	s := &S3Server{}
	inner := gofakes3.New(backend).Server()
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.down.Load() {
			http.Error(w, "simtest s3 outage", http.StatusServiceUnavailable)

			return
		}
		inner.ServeHTTP(w, r)
	}))
	s.URL = s.srv.URL

	return s
}

func (s *S3Server) SetDown(down bool) { s.down.Store(down) }
func (s *S3Server) Stop()             { s.srv.Close() }
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go mod tidy && go test ./world/ -run TestS3Server -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simtest/world/
git commit -s -m "simtest: in-process S3 server with outage switch

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 4: RamenConfig generation

**Files:**
- Create: `simtest/world/ramenconfig.go`, `simtest/world/ramenconfig_test.go`

**Interfaces:**
- Produces: `world.RamenConfigYAML(controllerType, s3URL string) (string, error)` and `world.OperatorConfigMap(controllerType, s3URL string) (*corev1.ConfigMap, error)` (controllerType is `"dr-hub"` or `"dr-cluster"`; the ConfigMap is named per type, namespace `ramen-system`).

- [ ] **Step 1: Write the failing test**

`simtest/world/ramenconfig_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"testing"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"sigs.k8s.io/yaml"
)

func TestRamenConfigYAML(t *testing.T) {
	y, err := RamenConfigYAML("dr-hub", "http://127.0.0.1:9999")
	if err != nil {
		t.Fatal(err)
	}

	cfg := rmn.RamenConfig{}
	if err := yaml.Unmarshal([]byte(y), &cfg); err != nil {
		t.Fatalf("generated config does not unmarshal into RamenConfig: %v", err)
	}

	if cfg.LeaderElection == nil || cfg.LeaderElection.LeaderElect == nil || *cfg.LeaderElection.LeaderElect {
		t.Fatal("leader election must be disabled")
	}
	if cfg.Metrics.BindAddress != "0" || cfg.Health.HealthProbeBindAddress != "0" {
		t.Fatal("metrics and health must be disabled")
	}
	if len(cfg.S3StoreProfiles) != 2 {
		t.Fatalf("want 2 s3 profiles, got %d", len(cfg.S3StoreProfiles))
	}
	if !cfg.VolSync.Disabled || !cfg.KubeObjectProtection.Disabled {
		t.Fatal("volsync and kubeObjectProtection must be disabled in v1")
	}

	cm, err := OperatorConfigMap("dr-cluster", "http://127.0.0.1:9999")
	if err != nil {
		t.Fatal(err)
	}
	if cm.Name != DRConfigMapName || cm.Namespace != RamenSystemNS || cm.Data[ConfigMapKey] == "" {
		t.Fatalf("bad configmap: %+v", cm.ObjectMeta)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./world/ -run TestRamenConfigYAML -v`
Expected: FAIL (compile error)

- [ ] **Step 3: Implement**

`simtest/world/ramenconfig.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"fmt"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cfgv1alpha1 "k8s.io/component-base/config/v1alpha1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

// RamenConfigYAML renders the operator config stored in the operator
// ConfigMap. Both operator types get both clusters' S3 profiles: the
// dr-cluster operator uploads to all profiles in VRG.spec.s3Profiles and the
// hub validates DRCluster.spec.s3ProfileName against its own copy.
func RamenConfigYAML(controllerType, s3URL string) (string, error) {
	resourceName := "hub.ramendr.openshift.io"
	if controllerType == "dr-cluster" {
		resourceName = "dr-cluster.ramendr.openshift.io"
	}

	cfg := rmn.RamenConfig{
		TypeMeta: metav1.TypeMeta{APIVersion: "ramendr.openshift.io/v1alpha1", Kind: "RamenConfig"},
		LeaderElection: &cfgv1alpha1.LeaderElectionConfiguration{
			LeaderElect:  ptr.To(false),
			ResourceName: resourceName,
		},
		Metrics:                 rmn.ControllerMetrics{BindAddress: "0"},
		Health:                  rmn.ControllerHealth{HealthProbeBindAddress: "0"},
		RamenControllerType:     rmn.ControllerType(controllerType),
		MaxConcurrentReconciles: 50,
		RamenOpsNamespace:       RamenOpsNS,
		VolSync:                 rmn.VolSyncConfig{Disabled: true},
		KubeObjectProtection:    rmn.KubeObjectProtectionConfig{Disabled: true},
		DrClusterOperator: rmn.DrClusterOperatorConfig{
			DeploymentAutomationEnabled: false,
			S3SecretDistributionEnabled: false,
		},
		S3StoreProfiles: []rmn.S3StoreProfile{
			s3Profile(DR1Name, s3URL),
			s3Profile(DR2Name, s3URL),
		},
	}

	b, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal ramen config: %w", err)
	}

	return string(b), nil
}

func s3Profile(cluster, s3URL string) rmn.S3StoreProfile {
	return rmn.S3StoreProfile{
		S3ProfileName:        S3Profile(cluster),
		S3Bucket:             S3Bucket(cluster),
		S3CompatibleEndpoint: s3URL,
		S3Region:             "us-east-1",
		S3SecretRef:          corev1.SecretReference{Name: S3SecretName}, // resolved in POD_NAMESPACE
	}
}

func OperatorConfigMap(controllerType, s3URL string) (*corev1.ConfigMap, error) {
	name := HubConfigMapName
	if controllerType == "dr-cluster" {
		name = DRConfigMapName
	}

	y, err := RamenConfigYAML(controllerType, s3URL)
	if err != nil {
		return nil, err
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: RamenSystemNS},
		Data:       map[string]string{ConfigMapKey: y},
	}, nil
}
```

(Field names to verify against `api/v1alpha1/ramenconfig_types.go` at implementation time: `VolSyncConfig`, `KubeObjectProtectionConfig`, `DrClusterOperatorConfig` struct names, and whether the `KubeObjectProtection`/`VolSync` fields are values or pointers — adjust to the real types; the unit test locks the semantics either way.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go mod tidy && go test ./world/ -run TestRamenConfigYAML -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simtest/world/
git commit -s -m "simtest: RamenConfig and operator ConfigMap generation

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 5: Manager process control

**Files:**
- Create: `simtest/world/process.go`, `simtest/world/process_test.go`

**Interfaces:**
- Produces: `world.StartManager(o ManagerOpts) (*ManagerProcess, error)`; `type ManagerOpts struct { Name, Bin, Kubeconfig, LogDir string; ControllerType string; Reconcilers string }`; `(*ManagerProcess).Kill() error`, `(*ManagerProcess).Restart() error`, `(*ManagerProcess).Alive() bool`, `(*ManagerProcess).Stop()`.

- [ ] **Step 1: Write the failing test**

`simtest/world/process_test.go` — uses `/bin/sleep` as a stand-in binary so this unit test needs no manager build:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"testing"
	"time"
)

func TestManagerProcessLifecycle(t *testing.T) {
	p, err := StartManager(ManagerOpts{
		Name: "fake", Bin: "/bin/sleep", Kubeconfig: "600", LogDir: t.TempDir(),
		ControllerType: "dr-hub", Reconcilers: "drpc",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Stop)

	if !p.Alive() {
		t.Fatal("process should be alive")
	}
	if err := p.Kill(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for p.Alive() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if p.Alive() {
		t.Fatal("process should be dead after Kill")
	}

	if err := p.Restart(); err != nil {
		t.Fatal(err)
	}
	if !p.Alive() {
		t.Fatal("process should be alive after Restart")
	}
}
```

(Note the trick: for `/bin/sleep`, the kubeconfig value `600` becomes the sleep duration through the `--kubeconfig=600` argument — that fails for sleep. Instead build the command as `bin` + args where args for the real manager are `--kubeconfig=<path>`; for the test, pass Bin `/bin/sleep` and Kubeconfig `600` and have `StartManager` special-case nothing — the arg `--kubeconfig=600` makes sleep exit immediately with an error. To keep the stub honest, `StartManager` must place the kubeconfig arg only when the file exists; otherwise it passes `Kubeconfig` as a raw argument. Implement exactly that: `if _, err := os.Stat(o.Kubeconfig); err == nil { args = ["--kubeconfig="+o.Kubeconfig] } else { args = [o.Kubeconfig] }`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./world/ -run TestManagerProcessLifecycle -v`
Expected: FAIL (compile error)

- [ ] **Step 3: Implement**

`simtest/world/process.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
)

type ManagerOpts struct {
	Name           string // process label, used for the log file
	Bin            string
	Kubeconfig     string
	LogDir         string
	ControllerType string // dr-hub | dr-cluster
	Reconcilers    string // RAMEN_RECONCILERS value
}

// ManagerProcess runs one ramen operator as a subprocess. Kill/Restart enable
// crash-recovery scenarios.
type ManagerProcess struct {
	mu   sync.Mutex
	opts ManagerOpts
	cmd  *exec.Cmd
	log  *os.File
}

func StartManager(o ManagerOpts) (*ManagerProcess, error) {
	p := &ManagerProcess{opts: o}
	if err := p.start(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *ManagerProcess) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	logFile, err := os.OpenFile(
		filepath.Join(p.opts.LogDir, p.opts.Name+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	args := []string{p.opts.Kubeconfig}
	if _, err := os.Stat(p.opts.Kubeconfig); err == nil {
		args = []string{"--kubeconfig=" + p.opts.Kubeconfig}
	}

	cmd := exec.Command(p.opts.Bin, args...)
	cmd.Env = append(os.Environ(),
		"RAMEN_CONTROLLER_TYPE="+p.opts.ControllerType,
		"POD_NAMESPACE="+RamenSystemNS,
		"RAMEN_RECONCILERS="+p.opts.Reconcilers,
		"RAMEN_METRICS_BIND_ADDRESS=0",
		"RAMEN_HEALTH_BIND_ADDRESS=0",
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()

		return fmt.Errorf("start %s: %w", p.opts.Name, err)
	}

	go func() { _ = cmd.Wait() }() // reap; Alive() checks the result

	p.cmd, p.log = cmd, logFile

	return nil
}

func (p *ManagerProcess) Alive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.cmd != nil && p.cmd.ProcessState == nil
}

func (p *ManagerProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}

	return p.cmd.Process.Signal(syscall.SIGKILL)
}

func (p *ManagerProcess) Restart() error {
	_ = p.Kill()
	p.mu.Lock()
	if p.log != nil {
		p.log.Close()
	}
	p.mu.Unlock()

	return p.start()
}

// Stop terminates the process at teardown (SIGKILL is fine for tests).
func (p *ManagerProcess) Stop() {
	_ = p.Kill()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.log != nil {
		p.log.Close()
		p.log = nil
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go test ./world/ -run TestManagerProcessLifecycle -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simtest/world/
git commit -s -m "simtest: manager subprocess lifecycle control

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 6: Fault-injection policy store and event log

**Files:**
- Create: `simtest/actors/policy.go`, `simtest/actors/policy_test.go`, `simtest/actors/evlog.go`

**Interfaces:**
- Produces:
  - `type Key struct { Actor, Cluster string }` with `func (k Key) String() string` (`"volrep@dr1"`), and constructors `VolRep(cluster)`, `Work(cluster)`, `View(cluster)`, `Binder(cluster)`.
  - Policies: `Normal{}`, `Silent{}`, `Delayed{After time.Duration}`, `FailWith{Mode string}` all implementing `Policy` (marker `isPolicy()`).
  - `NewStore() *Store`; `(*Store).Set(k Key, p Policy)`; `(*Store).Decide(k Key, obj string) Decision` where `type Decision struct { Proceed bool; RequeueAfter time.Duration; Policy Policy }`.
  - `NewEvLog(path string) (*EvLog, error)`; `(*EvLog).Logf(format string, args ...any)`; `(*EvLog).Close()`.

- [ ] **Step 1: Write the failing test**

`simtest/actors/policy_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"testing"
	"time"
)

func TestPolicyStore(t *testing.T) {
	s := NewStore()
	k := VolRep("dr1")

	if d := s.Decide(k, "pvc-a"); !d.Proceed {
		t.Fatal("default policy must be Normal/proceed")
	}

	s.Set(k, Silent{})
	if d := s.Decide(k, "pvc-a"); d.Proceed || d.RequeueAfter <= 0 {
		t.Fatalf("silent must not proceed and must requeue, got %+v", d)
	}

	s.Set(k, Delayed{After: 80 * time.Millisecond})
	if d := s.Decide(k, "pvc-b"); d.Proceed {
		t.Fatal("delayed must hold before deadline")
	}
	time.Sleep(120 * time.Millisecond)
	if d := s.Decide(k, "pvc-b"); !d.Proceed {
		t.Fatal("delayed must proceed after deadline")
	}

	s.Set(k, FailWith{Mode: "degraded"})
	d := s.Decide(k, "pvc-a")
	if !d.Proceed {
		t.Fatal("failWith proceeds (with failure payload)")
	}
	if fw, ok := d.Policy.(FailWith); !ok || fw.Mode != "degraded" {
		t.Fatalf("decision must carry the policy, got %+v", d.Policy)
	}

	s.Set(k, Normal{})
	if d := s.Decide(k, "pvc-b"); !d.Proceed {
		t.Fatal("reset to normal must proceed")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./actors/ -run TestPolicyStore -v`
Expected: FAIL (compile error)

- [ ] **Step 3: Implement**

`simtest/actors/policy.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"sync"
	"time"
)

// Key identifies one actor instance on one cluster; policies are set per key.
type Key struct{ Actor, Cluster string }

func (k Key) String() string { return k.Actor + "@" + k.Cluster }

func VolRep(cluster string) Key { return Key{Actor: "volrep", Cluster: cluster} }
func Work(cluster string) Key   { return Key{Actor: "work", Cluster: cluster} }
func View(cluster string) Key   { return Key{Actor: "view", Cluster: cluster} }
func Binder(cluster string) Key { return Key{Actor: "binder", Cluster: cluster} }

type Policy interface{ isPolicy() }

// Normal fulfills immediately.
type Normal struct{}

// Silent never responds while set (external system down); actors poll so
// clearing the policy resumes fulfillment.
type Silent struct{}

// Delayed responds only after the object has been pending for After.
type Delayed struct{ After time.Duration }

// FailWith fulfills with an actor-specific failure payload. Modes are defined
// by each actor (volrep: "validated-false", "degraded").
type FailWith struct{ Mode string }

func (Normal) isPolicy()   {}
func (Silent) isPolicy()   {}
func (Delayed) isPolicy()  {}
func (FailWith) isPolicy() {}

const pollInterval = 300 * time.Millisecond

type Decision struct {
	Proceed      bool
	RequeueAfter time.Duration
	Policy       Policy
}

type Store struct {
	mu        sync.Mutex
	policies  map[Key]Policy
	firstSeen map[string]time.Time // key.String()+"/"+obj -> first Decide under Delayed
}

func NewStore() *Store {
	return &Store{policies: map[Key]Policy{}, firstSeen: map[string]time.Time{}}
}

func (s *Store) Set(k Key, p Policy) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.policies[k] = p

	if _, ok := p.(Delayed); !ok { // reset delay clocks on any policy change
		for key := range s.firstSeen {
			delete(s.firstSeen, key)
		}
	}
}

func (s *Store) Decide(k Key, obj string) Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	p, ok := s.policies[k]
	if !ok {
		p = Normal{}
	}

	switch pol := p.(type) {
	case Silent:
		return Decision{Proceed: false, RequeueAfter: pollInterval, Policy: pol}
	case Delayed:
		id := k.String() + "/" + obj
		t0, seen := s.firstSeen[id]
		if !seen {
			t0 = time.Now()
			s.firstSeen[id] = t0
		}
		if time.Since(t0) < pol.After {
			return Decision{Proceed: false, RequeueAfter: pollInterval, Policy: pol}
		}

		return Decision{Proceed: true, Policy: pol}
	default:
		return Decision{Proceed: true, Policy: p}
	}
}
```

`simtest/actors/evlog.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"fmt"
	"os"
	"sync"
	"time"
)

// EvLog is the per-world append-only actor event log, dumped on test failure.
type EvLog struct {
	mu sync.Mutex
	f  *os.File
}

func NewEvLog(path string) (*EvLog, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &EvLog{f: f}, nil
}

func (l *EvLog) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	fmt.Fprintf(l.f, "%s "+format+"\n", append([]any{time.Now().Format(time.RFC3339Nano)}, args...)...)
}

func (l *EvLog) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.f.Close()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go test ./actors/ -run TestPolicyStore -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simtest/actors/
git commit -s -m "simtest: fault-injection policy store and event log

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 7: Actor runtime, PVC binder, janitor

**Files:**
- Create: `simtest/actors/runtime.go`, `simtest/actors/pvbinder.go`, `simtest/actors/janitor.go`, `simtest/actors/pvbinder_test.go`

**Interfaces:**
- Consumes: `world.StartCluster` (test only), `Store`, `EvLog`.
- Produces:
  - `type ClusterRef struct { Name string; Cfg *rest.Config }`
  - `type Runtime struct { Store *Store; Log *EvLog }`
  - `Start(ctx context.Context, scheme *runtime.Scheme, hub ClusterRef, managed []ClusterRef, log *EvLog) (*Runtime, error)` — builds one controller-runtime manager per managed cluster (binder+janitor+volrep) and one for the hub (ocm agents; wired in Tasks 8-9), starts them on goroutines.
  - Binder behavior (used implicitly by every scenario): any Pending PVC gets a PV created+bound and both statuses set Bound; restored PVCs (with `spec.volumeName` already set) are adopted and bound.

- [ ] **Step 1: Write the failing test**

`simtest/actors/pvbinder_test.go` (integration, single envtest cluster, runtime with binder only — hub == the same cluster so `Start` is exercised whole):

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ramendr/ramen/simtest/actors"
	"github.com/ramendr/ramen/simtest/world"
)

func TestPVBinder(t *testing.T) {
	world.EnsureAssets(t)

	dir := t.TempDir()
	c, err := world.StartCluster("dr1", dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	log, err := actors.NewEvLog(filepath.Join(dir, "events.log"))
	if err != nil {
		t.Fatal(err)
	}
	ref := actors.ClusterRef{Name: c.Name, Cfg: c.Cfg}
	if _, err := actors.Start(ctx, world.NewScheme(), ref, []actors.ClusterRef{ref}, log); err != nil {
		t.Fatal(err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: ptr.To(world.StorageClassName),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if err := c.Client.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.Client.Get(ctx, types.NamespacedName{Name: "data", Namespace: "default"}, pvc)
		if pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("PVC never bound: phase=%s volumeName=%q", pvc.Status.Phase, pvc.Spec.VolumeName)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./actors/ -run TestPVBinder -v`
Expected: FAIL (compile error)

- [ ] **Step 3: Implement runtime, binder, janitor**

`simtest/actors/runtime.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

type ClusterRef struct {
	Name string
	Cfg  *rest.Config
}

type Runtime struct {
	Store *Store
	Log   *EvLog
}

// Start launches the framework-side external actors: per managed cluster a
// manager running pvbinder + janitor + volrep, and on the hub a manager
// running the OCM work/view agents (one pair per managed cluster).
func Start(ctx context.Context, scheme *runtime.Scheme, hub ClusterRef, managed []ClusterRef, log *EvLog,
) (*Runtime, error) {
	rt := &Runtime{Store: NewStore(), Log: log}

	managedClients := map[string]client.Client{}

	for _, m := range managed {
		mgr, err := newManager(m.Cfg, scheme)
		if err != nil {
			return nil, fmt.Errorf("actor manager %s: %w", m.Name, err)
		}

		managedClients[m.Name] = mgr.GetClient()

		if err := setupPVBinder(mgr, m.Name, rt); err != nil {
			return nil, err
		}
		if err := setupVolRep(mgr, m.Name, rt); err != nil {
			return nil, err
		}

		go runJanitor(ctx, mgr.GetClient(), m.Name, rt)
		go func(m manager.Manager, name string) {
			if err := m.Start(ctx); err != nil {
				rt.Log.Logf("actor-manager %s exited: %v", name, err)
			}
		}(mgr, m.Name)
	}

	hubMgr, err := newManager(hub.Cfg, scheme)
	if err != nil {
		return nil, fmt.Errorf("actor manager hub: %w", err)
	}

	for _, m := range managed {
		if err := setupOCMAgents(hubMgr, m.Name, managedClients[m.Name], rt); err != nil {
			return nil, err
		}
	}

	go func() {
		if err := hubMgr.Start(ctx); err != nil {
			rt.Log.Logf("actor-manager hub exited: %v", err)
		}
	}()

	return rt, nil
}

func newManager(cfg *rest.Config, scheme *runtime.Scheme) (manager.Manager, error) {
	return ctrl.NewManager(cfg, ctrl.Options{
		Scheme:         scheme,
		Metrics:        metricsserver.Options{BindAddress: "0"},
		LeaderElection: false,
	})
}
```

(`setupVolRep` and `setupOCMAgents` don't exist yet — add stubs so this task compiles, replaced in Tasks 8-9:)

```go
// in runtime.go for now; Tasks 8 and 9 move these to their own files.
func setupVolRep(mgr manager.Manager, cluster string, rt *Runtime) error { return nil }

func setupOCMAgents(mgr manager.Manager, cluster string, managedClient client.Client, rt *Runtime) error {
	return nil
}
```

`simtest/actors/pvbinder.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// pvBinder stands in for kube-controller-manager + a CSI provisioner: envtest
// runs no controllers, so nothing else would ever bind a PVC.
type pvBinder struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupPVBinder(mgr manager.Manager, cluster string, rt *Runtime) error {
	b := &pvBinder{client: mgr.GetClient(), cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		Complete(b)
}

func (b *pvBinder) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pvc := &corev1.PersistentVolumeClaim{}
	if err := b.client.Get(ctx, req.NamespacedName, pvc); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if pvc.GetDeletionTimestamp() != nil || pvc.Status.Phase == corev1.ClaimBound {
		return ctrl.Result{}, nil
	}

	d := b.rt.Store.Decide(Binder(b.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		pvName = "pv-" + string(pvc.UID)
	}

	if err := b.ensurePV(ctx, pvName, pvc); err != nil {
		return ctrl.Result{}, err
	}

	if pvc.Spec.VolumeName == "" {
		pvc.Spec.VolumeName = pvName
		if err := b.client.Update(ctx, pvc); err != nil {
			return ctrl.Result{}, err
		}
	}

	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.AccessModes = pvc.Spec.AccessModes
	pvc.Status.Capacity = pvc.Spec.Resources.Requests

	if err := b.client.Status().Update(ctx, pvc); err != nil {
		return ctrl.Result{}, err
	}

	b.rt.Log.Logf("binder@%s bound pvc %s -> pv %s", b.cluster, req.NamespacedName, pvName)

	return ctrl.Result{}, nil
}

func (b *pvBinder) ensurePV(ctx context.Context, pvName string, pvc *corev1.PersistentVolumeClaim) error {
	pv := &corev1.PersistentVolume{}

	err := b.client.Get(ctx, types.NamespacedName{Name: pvName}, pv)
	if errors.IsNotFound(err) {
		pv = &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:    pvc.Spec.Resources.Requests,
				AccessModes: pvc.Spec.AccessModes,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: "mock.csi.ramen.io", VolumeHandle: pvName},
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				StorageClassName:              deref(pvc.Spec.StorageClassName),
				ClaimRef: &corev1.ObjectReference{
					Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID,
				},
			},
		}
		if err := b.client.Create(ctx, pv); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// Adopt restored PVs whose ClaimRef points at a prior incarnation of this claim.
	if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == pvc.Name &&
		pv.Spec.ClaimRef.Namespace == pvc.Namespace && pv.Spec.ClaimRef.UID != pvc.UID {
		pv.Spec.ClaimRef.UID = pvc.UID
		if err := b.client.Update(ctx, pv); err != nil {
			return err
		}
	}

	if pv.Status.Phase != corev1.VolumeBound {
		pv.Status.Phase = corev1.VolumeBound
		if err := b.client.Status().Update(ctx, pv); err != nil {
			return err
		}
	}

	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
```

`simtest/actors/janitor.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// runJanitor emulates the kube-controller-manager protection controllers:
// envtest's apiserver adds kubernetes.io/pvc-protection and pv-protection
// finalizers, but nothing removes them, so deletes would hang forever.
func runJanitor(ctx context.Context, c client.Client, cluster string, rt *Runtime) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep(ctx, c, cluster, rt)
		}
	}
}

func sweep(ctx context.Context, c client.Client, cluster string, rt *Runtime) {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcs); err == nil {
		for i := range pvcs.Items {
			stripFinalizer(ctx, c, &pvcs.Items[i], "kubernetes.io/pvc-protection", cluster, rt)
		}
	}

	pvs := &corev1.PersistentVolumeList{}
	if err := c.List(ctx, pvs); err == nil {
		for i := range pvs.Items {
			stripFinalizer(ctx, c, &pvs.Items[i], "kubernetes.io/pv-protection", cluster, rt)
		}
	}
}

func stripFinalizer(ctx context.Context, c client.Client, obj client.Object, fin, cluster string, rt *Runtime) {
	if obj.GetDeletionTimestamp() == nil || !slices.Contains(obj.GetFinalizers(), fin) {
		return
	}

	obj.SetFinalizers(slices.DeleteFunc(obj.GetFinalizers(), func(s string) bool { return s == fin }))

	if err := c.Update(ctx, obj); err == nil {
		rt.Log.Logf("janitor@%s stripped %s from %s/%s", cluster, fin, obj.GetNamespace(), obj.GetName())
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go mod tidy && go test ./actors/ -run TestPVBinder -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simtest/actors/
git commit -s -m "simtest: actor runtime with PVC binder and finalizer janitor

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 8: VolumeReplication actor

**Files:**
- Create: `simtest/actors/volrep.go`, `simtest/actors/volrep_test.go`
- Modify: `simtest/actors/runtime.go` (delete the `setupVolRep` stub)

**Interfaces:**
- Consumes: `Store`/`Decision` (Task 6), runtime registration (Task 7).
- Produces: fulfillment of `VolumeReplication.status` exactly as `internal/controller/mock/volumereplication_fulfiller.go` does (ported, not imported). `FailWith` modes: `"validated-false"` (Validated=False, Completed=False — VR never completes), `"degraded"` (Completed=True, Degraded=True, Resyncing=True — secondary stuck resyncing / primary degraded).

- [ ] **Step 1: Write the failing test**

`simtest/actors/volrep_test.go` (unit-level: call the fulfiller function directly; the reconciler wiring is covered by T1):

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"testing"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newVR(state volrep.ReplicationState) *volrep.VolumeReplication {
	vr := &volrep.VolumeReplication{}
	vr.Name, vr.Namespace, vr.Generation = "pvc-a", "app-ns", 3
	vr.Spec.ReplicationState = state

	return vr
}

func findCond(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}

	return nil
}

func TestFulfillVolumeReplicationPrimary(t *testing.T) {
	st := fulfillVolumeReplication(newVR(volrep.Primary), Normal{})

	if st.State != volrep.PrimaryState || st.ObservedGeneration != 3 {
		t.Fatalf("bad state: %+v", st)
	}
	if st.LastSyncTime == nil || st.DestinationVolumeID == "" {
		t.Fatal("primary needs LastSyncTime and DestinationVolumeID")
	}
	for typ, want := range map[string]metav1.ConditionStatus{
		volrep.ConditionValidated: metav1.ConditionTrue,
		volrep.ConditionCompleted: metav1.ConditionTrue,
		volrep.ConditionDegraded:  metav1.ConditionFalse,
		volrep.ConditionResyncing: metav1.ConditionFalse,
	} {
		c := findCond(st.Conditions, typ)
		if c == nil || c.Status != want || c.ObservedGeneration != 3 {
			t.Fatalf("condition %s: got %+v want %s", typ, c, want)
		}
	}
}

func TestFulfillVolumeReplicationSecondary(t *testing.T) {
	st := fulfillVolumeReplication(newVR(volrep.Secondary), Normal{})

	if st.State != volrep.SecondaryState || st.DestinationVolumeID != "" {
		t.Fatalf("bad secondary status: %+v", st)
	}
	// VRG requires Completed=True, Degraded=False, Resyncing=False, State=Secondary
	// to consider a secondary protected (vrg_volrep.go checkResyncCompletionAsSecondary).
	if c := findCond(st.Conditions, volrep.ConditionCompleted); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatal("secondary Completed must be True")
	}
}

func TestFulfillVolumeReplicationFailureModes(t *testing.T) {
	st := fulfillVolumeReplication(newVR(volrep.Primary), FailWith{Mode: "validated-false"})
	if c := findCond(st.Conditions, volrep.ConditionValidated); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatal("validated-false mode must set Validated=False")
	}

	st = fulfillVolumeReplication(newVR(volrep.Secondary), FailWith{Mode: "degraded"})
	if c := findCond(st.Conditions, volrep.ConditionDegraded); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatal("degraded mode must set Degraded=True")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./actors/ -run TestFulfillVolumeReplication -v`
Expected: FAIL (compile error)

- [ ] **Step 3: Implement**

`simtest/actors/volrep.go` (fulfiller logic ported from `internal/controller/mock/volumereplication_fulfiller.go`; reconciler mirrors `volumereplication_controller.go` with the policy gate added):

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"time"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

type volRepActor struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupVolRep(mgr manager.Manager, cluster string, rt *Runtime) error {
	a := &volRepActor{client: mgr.GetClient(), cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&volrep.VolumeReplication{}).
		Complete(a)
}

func (a *volRepActor) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vr := &volrep.VolumeReplication{}
	if err := a.client.Get(ctx, req.NamespacedName, vr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if vr.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(VolRep(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	desired := fulfillVolumeReplication(vr, d.Policy)
	if vrStatusFulfilled(vr, desired) {
		return ctrl.Result{}, nil
	}

	vr.Status = desired
	if err := a.client.Status().Update(ctx, vr); err != nil {
		return ctrl.Result{}, err
	}

	a.rt.Log.Logf("volrep@%s fulfilled %s state=%s policy=%T", a.cluster, req.NamespacedName, desired.State, d.Policy)

	return ctrl.Result{}, nil
}

// fulfillVolumeReplication is the port of mock.FulfillVolumeReplication with
// failure-mode injection layered on top.
func fulfillVolumeReplication(vr *volrep.VolumeReplication, policy Policy) volrep.VolumeReplicationStatus {
	primary := vr.Spec.ReplicationState == volrep.Primary
	gen := vr.GetGeneration()
	now := metav1.Now()

	status := volrep.VolumeReplicationStatus{
		ObservedGeneration: gen,
		Conditions:         vrConditions(gen, primary, policy),
	}

	if primary {
		status.State = volrep.PrimaryState
		status.Message = "volume is marked primary"
		status.DestinationVolumeID = fmt.Sprintf("mock-%s-%s", vr.GetNamespace(), vr.GetName())
	} else {
		status.State = volrep.SecondaryState
		status.Message = "volume is marked secondary"
	}

	status.LastSyncTime = &now
	syncDuration := metav1.Duration{Duration: time.Second}
	status.LastSyncDuration = &syncDuration

	return status
}

func vrConditions(gen int64, primary bool, policy Policy) []metav1.Condition {
	validated, completed := metav1.ConditionTrue, metav1.ConditionTrue
	degraded, resyncing := metav1.ConditionFalse, metav1.ConditionFalse

	if fw, ok := policy.(FailWith); ok {
		switch fw.Mode {
		case "validated-false":
			validated, completed = metav1.ConditionFalse, metav1.ConditionFalse
		case "degraded":
			degraded, resyncing = metav1.ConditionTrue, metav1.ConditionTrue
		}
	}

	completedReason := volrep.Promoted
	if !primary {
		completedReason = volrep.Demoted
	}

	now := metav1.Now()
	mk := func(typ string, st metav1.ConditionStatus, reason, msg string) metav1.Condition {
		return metav1.Condition{
			Type: typ, Status: st, Reason: reason, Message: msg,
			ObservedGeneration: gen, LastTransitionTime: now,
		}
	}

	return []metav1.Condition{
		mk(volrep.ConditionValidated, validated, volrep.PrerequisiteMet, "volume is validated"),
		mk(volrep.ConditionCompleted, completed, string(completedReason), "replication state set"),
		mk(volrep.ConditionDegraded, degraded, volrep.Healthy, "volume health"),
		mk(volrep.ConditionResyncing, resyncing, volrep.NotResyncing, "resync state"),
	}
}

func vrStatusFulfilled(vr *volrep.VolumeReplication, desired volrep.VolumeReplicationStatus) bool {
	if vr.Status.State != desired.State ||
		vr.Status.ObservedGeneration != desired.ObservedGeneration ||
		vr.Status.DestinationVolumeID != desired.DestinationVolumeID {
		return false
	}

	for _, want := range desired.Conditions {
		got := findCondition(vr.Status.Conditions, want.Type)
		if got == nil || got.Status != want.Status || got.ObservedGeneration != want.ObservedGeneration {
			return false
		}
	}

	return true
}

func findCondition(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}

	return nil
}
```

Also delete the `setupVolRep` stub from `runtime.go`.

(Constant name checks at implementation time: `volrep.Promoted`/`volrep.Demoted` may be typed condition reasons — convert with `string(...)` as shown; `volrep.PrerequisiteMet`, `volrep.Healthy`, `volrep.NotResyncing` are the reasons the mock fulfiller uses. The VRG's secondary truth table needs Degraded=False AND Resyncing=False AND state Secondary — the Normal path above satisfies it; the "degraded" mode holds a secondary in the syncing state which VRG reports as DataReady=Replicating/DataProtected=False.)

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd simtest && go test ./actors/ -v`
Expected: PASS (policy, binder, volrep tests)

- [ ] **Step 5: Commit**

```bash
git add simtest/actors/
git commit -s -m "simtest: VolumeReplication actor with failure modes

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 9: OCM work/view agent

**Files:**
- Create: `simtest/actors/ocmagent.go`, `simtest/actors/ocmagent_test.go`
- Modify: `simtest/actors/runtime.go` (delete the `setupOCMAgents` stub)

**Interfaces:**
- Consumes: `Runtime`, `Store`, policies `Work(cluster)`, `View(cluster)`.
- Produces the OCM contract ramen depends on (verified against `internal/controller/util/mw_util.go` and `mcv_util.go`):
  - **Work agent** — for every `ManifestWork` in hub namespace `<cluster>`: apply each `spec.workload.manifests[].RawExtension` to the managed cluster (server-side apply, field owner `simtest-work-agent`), then set status conditions `Applied=True` and `Available=True` (observedGeneration = MW generation). On MW deletion: delete the applied objects, then remove the framework finalizer `simtest.ramendr.openshift.io/work-cleanup`.
  - **View agent** — for every `ManagedClusterView` in hub namespace `<cluster>`: read `spec.scope` (Kind/Group/Version/Name/Namespace) from the managed cluster and write status: exactly ONE condition of type `viewv1beta1.ConditionViewProcessing`; on success `Status=True, Reason="GetResourceProcessing"` and `status.result.raw` = resource JSON; on NotFound `Status=False, Reason=viewv1beta1.ReasonGetResourceFailed, Message` containing the apierror text (ramen's `parseErrorMessage` matches "not found"). Requeues every 1s so hub always sees fresh managed-cluster state.

- [ ] **Step 1: Write the failing test**

`simtest/actors/ocmagent_test.go` (two envtest clusters: hub + dr1; no ramen binary):

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	viewv1beta1 "github.com/stolostron/multicloud-operators-foundation/pkg/apis/view/v1beta1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ocmworkv1 "open-cluster-management.io/api/work/v1"

	"github.com/ramendr/ramen/simtest/actors"
	"github.com/ramendr/ramen/simtest/world"
)

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestOCMAgent(t *testing.T) {
	world.EnsureAssets(t)

	dir := t.TempDir()
	hub, err := world.StartCluster("hub", dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Stop() })

	dr1, err := world.StartCluster("dr1", dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dr1.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	log, _ := actors.NewEvLog(filepath.Join(dir, "events.log"))
	_, err = actors.Start(ctx, world.NewScheme(),
		actors.ClusterRef{Name: hub.Name, Cfg: hub.Cfg},
		[]actors.ClusterRef{{Name: dr1.Name, Cfg: dr1.Cfg}}, log)
	if err != nil {
		t.Fatal(err)
	}

	// MW namespace on hub = managed cluster name.
	if err := hub.Client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "dr1"}}); err != nil {
		t.Fatal(err)
	}

	// --- work agent: MW carrying a ConfigMap manifest ---
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "from-mw", Namespace: "default"},
		Data:       map[string]string{"k": "v"},
	}
	raw, _ := json.Marshal(cm)
	mw := &ocmworkv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{Name: "test-default-cm-mw", Namespace: "dr1"},
		Spec: ocmworkv1.ManifestWorkSpec{Workload: ocmworkv1.ManifestsTemplate{
			Manifests: []ocmworkv1.Manifest{{RawExtension: runtime.RawExtension{Raw: raw}}},
		}},
	}
	if err := hub.Client.Create(ctx, mw); err != nil {
		t.Fatal(err)
	}

	eventually(t, 30*time.Second, func() bool {
		got := &corev1.ConfigMap{}

		return dr1.Client.Get(ctx, types.NamespacedName{Name: "from-mw", Namespace: "default"}, got) == nil
	}, "manifest never applied to managed cluster")

	eventually(t, 30*time.Second, func() bool {
		_ = hub.Client.Get(ctx, types.NamespacedName{Name: mw.Name, Namespace: "dr1"}, mw)

		return meta.IsStatusConditionTrue(mw.Status.Conditions, ocmworkv1.WorkApplied) &&
			meta.IsStatusConditionTrue(mw.Status.Conditions, ocmworkv1.WorkAvailable)
	}, "MW status never Applied+Available")

	// --- view agent: read that ConfigMap back ---
	mcv := &viewv1beta1.ManagedClusterView{
		ObjectMeta: metav1.ObjectMeta{Name: "test-default-cm-mcv", Namespace: "dr1"},
		Spec: viewv1beta1.ViewSpec{Scope: viewv1beta1.ViewScope{
			Kind: "ConfigMap", Version: "v1", Name: "from-mw", Namespace: "default",
		}},
	}
	if err := hub.Client.Create(ctx, mcv); err != nil {
		t.Fatal(err)
	}

	eventually(t, 30*time.Second, func() bool {
		_ = hub.Client.Get(ctx, types.NamespacedName{Name: mcv.Name, Namespace: "dr1"}, mcv)
		if len(mcv.Status.Conditions) != 1 || mcv.Status.Conditions[0].Type != viewv1beta1.ConditionViewProcessing {
			return false
		}

		return mcv.Status.Conditions[0].Status == metav1.ConditionTrue && len(mcv.Status.Result.Raw) > 0
	}, "MCV never fulfilled")

	// --- MW deletion removes the applied object ---
	if err := hub.Client.Delete(ctx, mw); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() bool {
		got := &corev1.ConfigMap{}
		err := dr1.Client.Get(ctx, types.NamespacedName{Name: "from-mw", Namespace: "default"}, got)

		return err != nil
	}, "applied object not cleaned up on MW delete")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./actors/ -run TestOCMAgent -v`
Expected: FAIL (stub `setupOCMAgents` does nothing → timeouts)

- [ ] **Step 3: Implement**

`simtest/actors/ocmagent.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"encoding/json"
	"time"

	viewv1beta1 "github.com/stolostron/multicloud-operators-foundation/pkg/apis/view/v1beta1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ocmworkv1 "open-cluster-management.io/api/work/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	workFinalizer = "simtest.ramendr.openshift.io/work-cleanup"
	fieldOwner    = client.FieldOwner("simtest-work-agent")
	viewRefresh   = time.Second
)

func setupOCMAgents(mgr manager.Manager, cluster string, managedClient client.Client, rt *Runtime) error {
	inCluster := predicate.NewPredicateFuncs(func(o client.Object) bool { return o.GetNamespace() == cluster })

	wa := &workAgent{hub: mgr.GetClient(), managed: managedClient, cluster: cluster, rt: rt}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&ocmworkv1.ManifestWork{}, builder.WithPredicates(inCluster)).
		Named("work-agent-" + cluster).
		Complete(wa); err != nil {
		return err
	}

	va := &viewAgent{hub: mgr.GetClient(), managed: managedClient, cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&viewv1beta1.ManagedClusterView{}, builder.WithPredicates(inCluster)).
		Named("view-agent-" + cluster).
		Complete(va)
}

// ---- work agent ----

type workAgent struct {
	hub, managed client.Client
	cluster      string
	rt           *Runtime
}

func (a *workAgent) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	mw := &ocmworkv1.ManifestWork{}
	if err := a.hub.Get(ctx, req.NamespacedName, mw); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	d := a.rt.Store.Decide(Work(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	if mw.GetDeletionTimestamp() != nil {
		if err := a.deleteManifests(ctx, mw); err != nil {
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(mw, workFinalizer)

		return ctrl.Result{}, a.hub.Update(ctx, mw)
	}

	if controllerutil.AddFinalizer(mw, workFinalizer) {
		if err := a.hub.Update(ctx, mw); err != nil {
			return ctrl.Result{}, err
		}
	}

	for i := range mw.Spec.Workload.Manifests {
		obj, err := decodeManifest(mw.Spec.Workload.Manifests[i].Raw)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := a.managed.Patch(ctx, obj, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
			return ctrl.Result{}, err
		}
	}

	appliedChanged := setMWCondition(mw, ocmworkv1.WorkApplied, "AppliedManifestWorkComplete")
	availableChanged := setMWCondition(mw, ocmworkv1.WorkAvailable, "ResourcesAvailable")
	if appliedChanged || availableChanged {
		if err := a.hub.Status().Update(ctx, mw); err != nil {
			return ctrl.Result{}, err
		}
		a.rt.Log.Logf("work@%s applied %s (%d manifests)", a.cluster, req.NamespacedName, len(mw.Spec.Workload.Manifests))
	}

	return ctrl.Result{}, nil
}

func (a *workAgent) deleteManifests(ctx context.Context, mw *ocmworkv1.ManifestWork) error {
	for i := range mw.Spec.Workload.Manifests {
		obj, err := decodeManifest(mw.Spec.Workload.Manifests[i].Raw)
		if err != nil {
			continue
		}
		if err := a.managed.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	a.rt.Log.Logf("work@%s cleaned up %s/%s", a.cluster, mw.Namespace, mw.Name)

	return nil
}

func decodeManifest(raw []byte) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return nil, err
	}

	return obj, nil
}

func setMWCondition(mw *ocmworkv1.ManifestWork, condType, reason string) bool {
	return meta.SetStatusCondition(&mw.Status.Conditions, metav1.Condition{
		Type: condType, Status: metav1.ConditionTrue, Reason: reason,
		Message: "simtest work agent", ObservedGeneration: mw.Generation,
	})
}

// ---- view agent ----

type viewAgent struct {
	hub, managed client.Client
	cluster      string
	rt           *Runtime
}

func (a *viewAgent) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	mcv := &viewv1beta1.ManagedClusterView{}
	if err := a.hub.Get(ctx, req.NamespacedName, mcv); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	d := a.rt.Store.Decide(View(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	scope := mcv.Spec.Scope
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: scope.Group, Version: scope.Version, Kind: scope.Kind})

	cond := metav1.Condition{Type: viewv1beta1.ConditionViewProcessing, ObservedGeneration: mcv.Generation}

	err := a.managed.Get(ctx, types.NamespacedName{Name: scope.Name, Namespace: scope.Namespace}, obj)

	switch {
	case err == nil:
		raw, jerr := json.Marshal(obj.Object)
		if jerr != nil {
			return ctrl.Result{}, jerr
		}
		mcv.Status.Result.Raw = raw
		cond.Status = metav1.ConditionTrue
		cond.Reason = "GetResourceProcessing"
		cond.Message = "Watching resources successfully"
	default:
		mcv.Status.Result.Raw = nil
		cond.Status = metav1.ConditionFalse
		cond.Reason = viewv1beta1.ReasonGetResourceFailed
		cond.Message = err.Error() // contains "not found" for NotFound; ramen parses this
	}

	mcv.Status.Conditions = []metav1.Condition{cond} // contract: exactly one condition

	if err := a.hub.Status().Update(ctx, mcv); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: viewRefresh}, nil
}
```

Delete the `setupOCMAgents` stub from `runtime.go`.

(Notes for the implementer: `scope.Kind`/`scope.Group`/`scope.Version` field names in `viewv1beta1.ViewScope` — verify against the vendored type (they may be `Resource`/`Group`/`Version`; ramen sets `Kind`, `Group`, `Version`, `Name`, `Namespace`). The MCV updates every second by design, so log MCV fulfillment only on transition (compare previous condition status) to keep the event log readable.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go test ./actors/ -run TestOCMAgent -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simtest/actors/
git commit -s -m "simtest: OCM work and view agent actors

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 10: Bootstrap objects and World assembly

**Files:**
- Create: `simtest/world/bootstrap.go`, `simtest/world/world.go`, `simtest/world/world_test.go`

**Interfaces:**
- Consumes: everything above.
- Produces:
  - `world.New(t *testing.T) *World` — full bring-up with `t.Cleanup` teardown; `type World struct { Dir string; Hub, DR1, DR2 *Cluster; S3 *S3Server; Actors *actors.Runtime }`.
  - `(*World).Cluster(name string) *Cluster`, `(*World).Managed() []*Cluster`.
  - `(*World).KillManager(name string) error`, `(*World).RestartManager(name string) error` — names: `"hub"`, `"dr1"`, `"dr2"`.
  - `world.SharedWorld(t *testing.T) *World` — package-level lazy singleton for the `tests/` package.

- [ ] **Step 1: Write the failing test**

`simtest/world/world_test.go` (this is the world smoke test — brings everything up, requires `bin/manager` built):

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world_test

import (
	"context"
	"os"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ramendr/ramen/simtest/world"
)

func TestWorldBringUp(t *testing.T) {
	if _, err := os.Stat(world.ManagerBin()); err != nil {
		t.Skipf("bin/manager not built, run 'make -C .. build': %v", err)
	}

	w := world.New(t)
	ctx := context.Background()

	// The hub operator must validate the DRClusters and then the DRPolicy —
	// this exercises: config load, S3 (validate profile), bootstrap MW apply
	// (work agent), DRClusterConfig MW + reconcile (dr-cluster operator), MCV
	// (view agent), ManagedCluster claims.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		policy := &rmn.DRPolicy{}
		if err := w.Hub.Client.Get(ctx, types.NamespacedName{Name: world.DRPolicyName}, policy); err == nil {
			if meta.IsStatusConditionTrue(policy.Status.Conditions, rmn.DRPolicyValidated) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("DRPolicy never became Validated; check .artifacts logs")
		}
		time.Sleep(500 * time.Millisecond)
	}
}
```

(Constant check: the DRPolicy validated condition type is `rmn.DRPolicyValidated` — verify the exact exported name in `api/v1alpha1/drpolicy_types.go`; the value is the string `"Validated"`.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./world/ -run TestWorldBringUp -v -timeout 10m`
Expected: FAIL (compile error: `world.New` undefined)

- [ ] **Step 3: Implement bootstrap**

`simtest/world/bootstrap.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"context"
	"fmt"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ocmv1 "open-cluster-management.io/api/cluster/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// bootstrap seeds every object the operators expect to find. It runs before
// the manager processes start.
func bootstrap(ctx context.Context, hub *Cluster, managed []*Cluster, s3URL string) error {
	// Hub: system namespaces + one namespace per managed cluster (MW/MCV home).
	hubNamespaces := []string{RamenSystemNS, RamenOpsNS}
	for _, m := range managed {
		hubNamespaces = append(hubNamespaces, m.Name)
	}
	if err := createNamespaces(ctx, hub.Client, hubNamespaces...); err != nil {
		return err
	}

	if err := createS3Secret(ctx, hub.Client); err != nil {
		return err
	}
	if err := createConfigMap(ctx, hub.Client, "dr-hub", s3URL); err != nil {
		return err
	}

	for _, m := range managed {
		if err := createNamespaces(ctx, m.Client, RamenSystemNS, RamenOpsNS); err != nil {
			return err
		}
		if err := createS3Secret(ctx, m.Client); err != nil {
			return err
		}
		if err := createConfigMap(ctx, m.Client, "dr-cluster", s3URL); err != nil {
			return err
		}
		if err := createStorageClasses(ctx, m); err != nil {
			return err
		}
		if err := createManagedCluster(ctx, hub.Client, m.Name); err != nil {
			return err
		}
	}

	if err := createDRClusters(ctx, hub.Client, managed); err != nil {
		return err
	}

	return createDRPolicy(ctx, hub.Client, managed)
}

func createNamespaces(ctx context.Context, c client.Client, names ...string) error {
	for _, n := range names {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n}}
		if err := client.IgnoreAlreadyExists(c.Create(ctx, ns)); err != nil {
			return err
		}
	}

	return nil
}

func createS3Secret(ctx context.Context, c client.Client) error {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: S3SecretName, Namespace: RamenSystemNS},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     S3AccessKey,
			"AWS_SECRET_ACCESS_KEY": S3SecretKey,
		},
	}

	return client.IgnoreAlreadyExists(c.Create(ctx, sec))
}

func createConfigMap(ctx context.Context, c client.Client, controllerType, s3URL string) error {
	cm, err := OperatorConfigMap(controllerType, s3URL)
	if err != nil {
		return err
	}

	return client.IgnoreAlreadyExists(c.Create(ctx, cm))
}

// createStorageClasses seeds the volrep storage/replication classes, labeled
// the way ramen's class matching and DRClusterConfig discovery require
// (mirrors ramen-mock's internal/mockenv/classes.go).
func createStorageClasses(ctx context.Context, m *Cluster) error {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: StorageClassName,
			Labels: map[string]string{
				StorageIDLabel:     StorageID(m.Name),
				ReplicationIDLabel: ReplicationID,
			},
		},
		Provisioner: Provisioner,
	}
	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, sc)); err != nil {
		return err
	}

	vrc := &volrep.VolumeReplicationClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: VRClassName,
			Labels: map[string]string{
				StorageIDLabel:     StorageID(m.Name),
				ReplicationIDLabel: ReplicationID,
			},
			Annotations: map[string]string{"replication.storage.openshift.io/is-default-class": "true"},
		},
		Spec: volrep.VolumeReplicationClassSpec{
			Provisioner: Provisioner,
			Parameters:  map[string]string{"schedulingInterval": SchedulingInterval},
		},
	}

	return client.IgnoreAlreadyExists(m.Client.Create(ctx, vrc))
}

// createManagedCluster registers the cluster on the hub with the status ramen
// requires: Joined condition and the id.k8s.io cluster claim
// (internal/controller/util/managedcluster.go).
func createManagedCluster(ctx context.Context, hubClient client.Client, name string) error {
	mc := &ocmv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       ocmv1.ManagedClusterSpec{HubAcceptsClient: true},
	}
	if err := client.IgnoreAlreadyExists(hubClient.Create(ctx, mc)); err != nil {
		return err
	}
	if err := hubClient.Get(ctx, client.ObjectKeyFromObject(mc), mc); err != nil {
		return err
	}

	mc.Status = ocmv1.ManagedClusterStatus{
		Conditions: []metav1.Condition{{
			Type: ocmv1.ManagedClusterConditionJoined, Status: metav1.ConditionTrue,
			Reason: "Joined", Message: "simtest", LastTransitionTime: metav1.Now(),
		}},
		ClusterClaims: []ocmv1.ManagedClusterClaim{{Name: "id.k8s.io", Value: name + "-cluster-id"}},
		Version:       ocmv1.ManagedClusterVersion{Kubernetes: "v1.33.0"},
	}

	return hubClient.Status().Update(ctx, mc)
}

func createDRClusters(ctx context.Context, hubClient client.Client, managed []*Cluster) error {
	for _, m := range managed {
		drc := &rmn.DRCluster{
			ObjectMeta: metav1.ObjectMeta{Name: m.Name},
			Spec:       rmn.DRClusterSpec{S3ProfileName: S3Profile(m.Name)},
		}
		if err := client.IgnoreAlreadyExists(hubClient.Create(ctx, drc)); err != nil {
			return err
		}
	}

	return nil
}

func createDRPolicy(ctx context.Context, hubClient client.Client, managed []*Cluster) error {
	names := make([]string, 0, len(managed))
	for _, m := range managed {
		names = append(names, m.Name)
	}

	if len(names) != 2 {
		return fmt.Errorf("drpolicy needs exactly 2 clusters, got %d", len(names))
	}

	pol := &rmn.DRPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: DRPolicyName},
		Spec: rmn.DRPolicySpec{
			DRClusters:         [2]string{names[0], names[1]},
			SchedulingInterval: SchedulingInterval,
		},
	}

	return client.IgnoreAlreadyExists(hubClient.Create(ctx, pol))
}
```

(Type check: `rmn.DRPolicySpec.DRClusters` — verify whether it is `[]string` or a fixed-size array in `api/v1alpha1/drpolicy_types.go` and adjust.)

- [ ] **Step 4: Implement World**

`simtest/world/world.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ramendr/ramen/simtest/actors"
)

type World struct {
	Dir    string
	Hub    *Cluster
	DR1    *Cluster
	DR2    *Cluster
	S3     *S3Server
	Actors *actors.Runtime

	procs  map[string]*ManagerProcess
	cancel context.CancelFunc
}

// New brings up the full world: 3 envtest clusters, S3, bootstrap objects,
// framework actors, and the two ramen operator flavors as subprocesses.
func New(t *testing.T) *World {
	t.Helper()
	EnsureAssets(t)

	dir := filepath.Join(RepoRoot(), "simtest", ".artifacts",
		fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	w := &World{Dir: dir, procs: map[string]*ManagerProcess{}}
	t.Cleanup(w.teardown)

	for _, name := range []string{HubName, DR1Name, DR2Name} {
		c, err := StartCluster(name, dir)
		if err != nil {
			t.Fatalf("start cluster %s: %v", name, err)
		}
		switch name {
		case HubName:
			w.Hub = c
		case DR1Name:
			w.DR1 = c
		case DR2Name:
			w.DR2 = c
		}
	}

	w.S3 = StartS3(S3Bucket(DR1Name), S3Bucket(DR2Name))

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	if err := bootstrap(ctx, w.Hub, w.Managed(), w.S3.URL); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	evlog, err := actors.NewEvLog(filepath.Join(dir, "actors.log"))
	if err != nil {
		t.Fatal(err)
	}

	rt, err := actors.Start(ctx, NewScheme(),
		actors.ClusterRef{Name: w.Hub.Name, Cfg: w.Hub.Cfg},
		[]actors.ClusterRef{
			{Name: w.DR1.Name, Cfg: w.DR1.Cfg},
			{Name: w.DR2.Name, Cfg: w.DR2.Cfg},
		}, evlog)
	if err != nil {
		t.Fatalf("start actors: %v", err)
	}
	w.Actors = rt

	w.startManager(t, HubName, w.Hub.KubeconfigPath, "dr-hub", "drpolicy,drcluster,drpc")
	w.startManager(t, DR1Name, w.DR1.KubeconfigPath, "dr-cluster", "vrg,drclusterconfig")
	w.startManager(t, DR2Name, w.DR2.KubeconfigPath, "dr-cluster", "vrg,drclusterconfig")

	return w
}

func (w *World) startManager(t *testing.T, name, kubeconfig, ctype, reconcilers string) {
	t.Helper()

	p, err := StartManager(ManagerOpts{
		Name: name, Bin: ManagerBin(), Kubeconfig: kubeconfig, LogDir: w.Dir,
		ControllerType: ctype, Reconcilers: reconcilers,
	})
	if err != nil {
		t.Fatalf("start manager %s: %v", name, err)
	}

	w.procs[name] = p
}

func (w *World) Managed() []*Cluster { return []*Cluster{w.DR1, w.DR2} }

func (w *World) Cluster(name string) *Cluster {
	switch name {
	case HubName:
		return w.Hub
	case DR1Name:
		return w.DR1
	case DR2Name:
		return w.DR2
	}

	return nil
}

func (w *World) KillManager(name string) error    { return w.procs[name].Kill() }
func (w *World) RestartManager(name string) error { return w.procs[name].Restart() }

func (w *World) teardown() {
	for _, p := range w.procs {
		p.Stop()
	}
	if w.cancel != nil {
		w.cancel()
	}
	if w.S3 != nil {
		w.S3.Stop()
	}
	for _, c := range []*Cluster{w.DR1, w.DR2, w.Hub} {
		if c != nil {
			_ = c.Stop()
		}
	}
}

var (
	sharedMu sync.Mutex
	shared   *World
)

// SharedWorld returns a world shared by the whole test binary. The first
// caller's t owns the cleanup, so it must be created from TestMain-driven
// code paths that outlive individual subtests — tests/main_test.go does this.
func SharedWorld(t *testing.T) *World {
	sharedMu.Lock()
	defer sharedMu.Unlock()

	if shared == nil {
		shared = New(t)
	}

	return shared
}
```

(Design note recorded for implementers: `SharedWorld` bound to the first test's `t.Cleanup` would tear down while later tests still run. `tests/main_test.go` (Task 13) solves this properly by creating the world in a top-level wrapper test that runs all scenario subtests inside it. `SharedWorld` stays but is only called from that wrapper.)

- [ ] **Step 5: Run test to verify it passes**

Run: `cd .. && make build && cd simtest && go test ./world/ -run TestWorldBringUp -v -timeout 15m`
Expected: PASS. This is the first full-stack milestone: hub validates DRClusters (S3 + bootstrap MW + MCV + ManagedCluster claims + DRClusterConfig round-trip) and the DRPolicy becomes Validated. If it fails, read `simtest/.artifacts/<test>/hub.log`, `dr1.log`, `dr2.log`, `actors.log`.
Expected debugging iterations here — this task validates every contract at once. Common causes: missing CRD (manager log shows failed informer sync), wrong ConfigMap field names, MCV scope field mismatch.

- [ ] **Step 6: Commit**

```bash
git add simtest/world/
git commit -s -m "simtest: bootstrap objects and full world assembly

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 11: observe + user packages

**Files:**
- Create: `simtest/observe/recorder.go`, `simtest/observe/ready.go`, `simtest/user/actions.go`

**Interfaces:**
- Consumes: `world.World`, DRPC types.
- Produces:
  - `observe.NewRecorder(ctx, cfg *rest.Config, ns, name string) (*Recorder, error)` — watches one DRPC via `client.WithWatch`; records deduped `status.progression` and `status.phase` sequences; `(*Recorder).OnProgression(value string, fn func())` registers a one-shot hook; `(*Recorder).WaitProgression(v string, timeout time.Duration) error`; `(*Recorder).WaitPhase(v string, timeout) error`; `(*Recorder).Progressions() []string`; `(*Recorder).Stop()`.
  - `observe.WaitDRPCReady(ctx, c client.Client, ns, name string, timeout time.Duration) error` — polls the e2e 4-way AND: `Available=True && PeerReady=True && progression==Completed && lastGroupSyncTime != nil`.
  - `user.App{Name string}` with derived `Namespace() string` (`<name>-ns`), `PVCName`, `ManagementNamespace` (= `world.RamenOpsNS`).
  - `user.CreateApp(ctx, w, app, cluster string) error` — creates the app namespace on BOTH managed clusters and a labeled PVC (`appname=<name>`, storageClass `mock-rbd`, 1Gi, RWO) on `cluster`.
  - `user.DeleteApp(ctx, w, app, cluster string) error` — deletes the PVC on `cluster` (the "user cleans up" step for `WaitOnUserToCleanUp`).
  - `user.EnableProtection(ctx, w, app) error` — Placement (annotated `experimental-scheduling-disable`, `numberOfClusters=1`) + DRPC (discovered-app shape: `preferredCluster=dr1`, `drPolicyRef`, `placementRef`, `pvcSelector{appname}`, `protectedNamespaces=[<app-ns>]`) in `ramen-ops` on the hub.
  - `user.Failover(ctx, w, app, target string) error` / `user.Relocate(ctx, w, app, preferred string) error` — patch DRPC spec action fields (retry on conflict).
  - `user.Disable(ctx, w, app) error` — annotate DRPC `drplacementcontrol.ramendr.openshift.io/do-not-delete-pvc: "true"`, delete DRPC, wait gone, delete Placement.

- [ ] **Step 1: Implement observe**

`simtest/observe/recorder.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package observe

import (
	"context"
	"fmt"
	"sync"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/simtest/world"
)

// Recorder watches a single DRPC and records every distinct progression and
// phase it observes, in order. Gates run against the recorded sequence, so a
// state that flashes by between two waits is still caught (the granularity is
// status updates, the finest available signal).
type Recorder struct {
	mu           sync.Mutex
	progressions []string
	phases       []string
	hooks        map[string][]func() // progression value -> one-shot hooks
	stop         context.CancelFunc
	done         chan struct{}
}

func NewRecorder(ctx context.Context, cfg *rest.Config, ns, name string) (*Recorder, error) {
	wc, err := client.NewWithWatch(cfg, client.Options{Scheme: world.NewScheme()})
	if err != nil {
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)
	r := &Recorder{hooks: map[string][]func(){}, stop: cancel, done: make(chan struct{})}

	list := &rmn.DRPlacementControlList{}
	wi, err := wc.Watch(wctx, list,
		client.InNamespace(ns),
		client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("metadata.name", name)})
	if err != nil {
		cancel()

		return nil, err
	}

	go func() {
		defer close(r.done)
		defer wi.Stop()

		for {
			select {
			case <-wctx.Done():
				return
			case ev, ok := <-wi.ResultChan():
				if !ok {
					return
				}
				drpc, isDRPC := ev.Object.(*rmn.DRPlacementControl)
				if !isDRPC {
					continue
				}
				r.record(string(drpc.Status.Progression), string(drpc.Status.Phase))
			}
		}
	}()

	return r, nil
}

func (r *Recorder) record(progression, phase string) {
	r.mu.Lock()

	var fire []func()

	if progression != "" && (len(r.progressions) == 0 || r.progressions[len(r.progressions)-1] != progression) {
		r.progressions = append(r.progressions, progression)
		fire = r.hooks[progression]
		delete(r.hooks, progression)
	}
	if phase != "" && (len(r.phases) == 0 || r.phases[len(r.phases)-1] != phase) {
		r.phases = append(r.phases, phase)
	}
	r.mu.Unlock()

	for _, f := range fire {
		f()
	}
}

func (r *Recorder) OnProgression(value string, fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, p := range r.progressions { // already seen? fire immediately
		if p == value {
			go fn()

			return
		}
	}
	r.hooks[value] = append(r.hooks[value], fn)
}

func (r *Recorder) seen(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}

	return false
}

func (r *Recorder) WaitProgression(v string, timeout time.Duration) error {
	return r.wait(func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.seen(r.progressions, v) },
		timeout, "progression "+v)
}

func (r *Recorder) WaitPhase(v string, timeout time.Duration) error {
	return r.wait(func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.seen(r.phases, v) },
		timeout, "phase "+v)
}

func (r *Recorder) wait(cond func() bool, timeout time.Duration, what string) error {
	deadline := time.Now().Add(Scale(timeout))
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return fmt.Errorf("timed out waiting for %s; progressions=%v phases=%v", what, r.progressions, r.phases)
}

func (r *Recorder) Progressions() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, len(r.progressions))
	copy(out, r.progressions)

	return out
}

func (r *Recorder) Stop() { r.stop(); <-r.done }
```

`simtest/observe/ready.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package observe

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Scale multiplies timeouts by SIMTEST_TIMEOUT_SCALE (default 1) so slow CI
// machines can widen every gate from one knob.
func Scale(d time.Duration) time.Duration {
	if s := os.Getenv("SIMTEST_TIMEOUT_SCALE"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil && f > 0 {
			return time.Duration(float64(d) * f)
		}
	}

	return d
}

// WaitDRPCReady mirrors e2e's waitDRPCReady: Available, PeerReady,
// progression Completed, and a non-nil lastGroupSyncTime.
func WaitDRPCReady(ctx context.Context, c client.Client, ns, name string, timeout time.Duration) error {
	deadline := time.Now().Add(Scale(timeout))

	var last string

	for time.Now().Before(deadline) {
		drpc := &rmn.DRPlacementControl{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, drpc); err == nil {
			available := meta.IsStatusConditionTrue(drpc.Status.Conditions, rmn.ConditionAvailable)
			peerReady := meta.IsStatusConditionTrue(drpc.Status.Conditions, rmn.ConditionPeerReady)
			completed := drpc.Status.Progression == rmn.ProgressionCompleted
			synced := drpc.Status.LastGroupSyncTime != nil

			if available && peerReady && completed && synced {
				return nil
			}

			last = fmt.Sprintf("available=%v peerReady=%v progression=%s synced=%v",
				available, peerReady, drpc.Status.Progression, synced)
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("DRPC %s/%s not ready: %s", ns, name, last)
}
```

- [ ] **Step 2: Implement user actions**

`simtest/user/actions.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package user

import (
	"context"
	"fmt"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	clrapiv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/simtest/world"
)

type App struct{ Name string }

func (a App) Namespace() string           { return a.Name + "-ns" }
func (a App) PVCName() string             { return a.Name + "-data" }
func (a App) ManagementNamespace() string { return world.RamenOpsNS }

// CreateApp creates the app namespace on both managed clusters (failover
// targets need it) and a labeled PVC on the given cluster. No pods: envtest
// has no kubelet, and the VRG in-use checks pass with none.
func CreateApp(ctx context.Context, w *world.World, app App, cluster string) error {
	for _, m := range w.Managed() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: app.Namespace()}}
		if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, ns)); err != nil {
			return err
		}
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: app.PVCName(), Namespace: app.Namespace(),
			Labels: map[string]string{world.AppLabelKey: app.Name},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: ptr.To(world.StorageClassName),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}

	return client.IgnoreAlreadyExists(w.Cluster(cluster).Client.Create(ctx, pvc))
}

// DeleteApp removes the app PVC on a cluster — the manual cleanup ramen waits
// for at ProgressionWaitOnUserToCleanUp for discovered apps.
func DeleteApp(ctx context.Context, w *world.World, app App, cluster string) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: app.PVCName(), Namespace: app.Namespace()},
	}

	err := w.Cluster(cluster).Client.Delete(ctx, pvc)
	if errors.IsNotFound(err) {
		return nil
	}

	return err
}

func EnableProtection(ctx context.Context, w *world.World, app App) error {
	placement := &clrapiv1beta1.Placement{
		ObjectMeta: metav1.ObjectMeta{
			Name: app.Name, Namespace: app.ManagementNamespace(),
			Annotations: map[string]string{world.OcmSchedulingDisable: "true"},
		},
		Spec: clrapiv1beta1.PlacementSpec{NumberOfClusters: ptr.To(int32(1))},
	}
	if err := client.IgnoreAlreadyExists(w.Hub.Client.Create(ctx, placement)); err != nil {
		return err
	}

	drpc := &rmn.DRPlacementControl{
		ObjectMeta: metav1.ObjectMeta{
			Name: app.Name, Namespace: app.ManagementNamespace(),
			Labels: map[string]string{"app": app.Name},
		},
		Spec: rmn.DRPlacementControlSpec{
			PreferredCluster: world.DR1Name,
			DRPolicyRef:      corev1.ObjectReference{Name: world.DRPolicyName},
			PlacementRef: corev1.ObjectReference{
				Kind: "Placement", Name: app.Name, Namespace: app.ManagementNamespace(),
			},
			PVCSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{world.AppLabelKey: app.Name},
			},
			ProtectedNamespaces: &[]string{app.Namespace()},
		},
	}

	return client.IgnoreAlreadyExists(w.Hub.Client.Create(ctx, drpc))
}

func Failover(ctx context.Context, w *world.World, app App, target string) error {
	return patchDRPC(ctx, w, app, func(drpc *rmn.DRPlacementControl) {
		drpc.Spec.Action = rmn.ActionFailover
		drpc.Spec.FailoverCluster = target
	})
}

func Relocate(ctx context.Context, w *world.World, app App, preferred string) error {
	return patchDRPC(ctx, w, app, func(drpc *rmn.DRPlacementControl) {
		drpc.Spec.Action = rmn.ActionRelocate
		drpc.Spec.PreferredCluster = preferred
	})
}

func patchDRPC(ctx context.Context, w *world.World, app App, mutate func(*rmn.DRPlacementControl)) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		drpc := &rmn.DRPlacementControl{}
		key := types.NamespacedName{Name: app.Name, Namespace: app.ManagementNamespace()}
		if err := w.Hub.Client.Get(ctx, key, drpc); err != nil {
			return err
		}
		mutate(drpc)

		return w.Hub.Client.Update(ctx, drpc)
	})
}

// Disable removes DR protection: annotate to keep PVCs, delete DRPC, wait for
// it to be gone, delete the Placement.
func Disable(ctx context.Context, w *world.World, app App, timeout time.Duration) error {
	if err := patchDRPC(ctx, w, app, func(drpc *rmn.DRPlacementControl) {
		if drpc.Annotations == nil {
			drpc.Annotations = map[string]string{}
		}
		drpc.Annotations["drplacementcontrol.ramendr.openshift.io/do-not-delete-pvc"] = "true"
	}); err != nil {
		return err
	}

	key := types.NamespacedName{Name: app.Name, Namespace: app.ManagementNamespace()}
	drpc := &rmn.DRPlacementControl{ObjectMeta: metav1.ObjectMeta{Name: app.Name, Namespace: app.ManagementNamespace()}}
	if err := w.Hub.Client.Delete(ctx, drpc); err != nil && !errors.IsNotFound(err) {
		return err
	}

	deadline := time.Now().Add(timeout)
	for {
		err := w.Hub.Client.Get(ctx, key, drpc)
		if errors.IsNotFound(err) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("DRPC %s not deleted; finalizer stuck (last: %+v)", key, drpc.Status.Progression)
		}
		time.Sleep(200 * time.Millisecond)
	}

	placement := &clrapiv1beta1.Placement{
		ObjectMeta: metav1.ObjectMeta{Name: app.Name, Namespace: app.ManagementNamespace()},
	}

	err := w.Hub.Client.Delete(ctx, placement)
	if errors.IsNotFound(err) {
		return nil
	}

	return err
}
```

(Constant checks at implementation: `rmn.ConditionAvailable`, `rmn.ConditionPeerReady`, `rmn.ProgressionCompleted`, `rmn.ActionFailover`, `rmn.ActionRelocate` — exact exported names live in `api/v1alpha1/drplacementcontrol_types.go`. The do-not-delete-pvc annotation key: grep `do-not-delete-pvc` in `internal/controller/` for the authoritative string.)

- [ ] **Step 3: Compile**

Run: `cd simtest && go build ./... && go vet ./...`
Expected: clean build (no unit test here — these packages are exercised end-to-end by Task 13's T1, which is their real test; envtest field selector on `metadata.name` for custom resources is supported).

- [ ] **Step 4: Commit**

```bash
git add simtest/observe/ simtest/user/
git commit -s -m "simtest: DRPC observation gates and user actions

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 12: Invariants checker

**Files:**
- Create: `simtest/invariants/checker.go`, `simtest/invariants/checker_test.go`

**Interfaces:**
- Consumes: `world.World` cluster clients (hub + managed).
- Produces: `invariants.StartChecker(ctx, w *world.World, edgeFile string) *Checker`; `(*Checker).Violations() []string`; `(*Checker).AssertClean(t *testing.T)`. Checks every 250ms:
  1. **Single primary:** at most one managed cluster has a VRG with `spec.replicationState: primary` per app, EXCEPT while that app's DRPC on the hub is in the failover window (`Phase` `FailingOver`, or `FailedOver` with `PeerReady != True`) — the documented transitional window.
  2. **Progression edges:** appends every observed `(from → to)` DRPC progression transition to `edgeFile` (report-only in v1; the checked-in expected-transition table is promoted to a hard assert once stable).

- [ ] **Step 1: Write the failing test**

`simtest/invariants/checker_test.go` — pure-logic test of the violation rule (no cluster):

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package invariants

import (
	"testing"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func drpcWith(phase rmn.DRState, peerReady bool) *rmn.DRPlacementControl {
	d := &rmn.DRPlacementControl{}
	d.Status.Phase = phase
	st := metav1.ConditionFalse
	if peerReady {
		st = metav1.ConditionTrue
	}
	d.Status.Conditions = []metav1.Condition{{Type: rmn.ConditionPeerReady, Status: st, Reason: "x"}}

	return d
}

func TestSinglePrimaryRule(t *testing.T) {
	if !violatesSinglePrimary(2, drpcWith(rmn.Deployed, true)) {
		t.Fatal("two primaries while Deployed must violate")
	}
	if violatesSinglePrimary(2, drpcWith(rmn.FailingOver, false)) {
		t.Fatal("two primaries while FailingOver is the allowed window")
	}
	if violatesSinglePrimary(2, drpcWith(rmn.FailedOver, false)) {
		t.Fatal("two primaries in FailedOver before PeerReady is allowed (old cluster not cleaned)")
	}
	if !violatesSinglePrimary(2, drpcWith(rmn.FailedOver, true)) {
		t.Fatal("two primaries after PeerReady must violate")
	}
	if violatesSinglePrimary(1, drpcWith(rmn.Deployed, true)) {
		t.Fatal("one primary never violates")
	}
	if !violatesSinglePrimary(2, drpcWith(rmn.Relocating, false)) {
		t.Fatal("relocate demotes before promoting; two primaries during relocate violate")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./invariants/ -v`
Expected: FAIL (compile error)

- [ ] **Step 3: Implement**

`simtest/invariants/checker.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package invariants

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ramendr/ramen/simtest/world"
)

// Checker continuously enforces global safety properties across all clusters,
// independent of what the current scenario asserts.
type Checker struct {
	mu         sync.Mutex
	violations []string
	lastProg   map[string]string // drpc key -> last progression, for edge recording
	edgeFile   *os.File
	w          *world.World
	cancel     context.CancelFunc
	done       chan struct{}
}

func StartChecker(ctx context.Context, w *world.World, edgePath string) (*Checker, error) {
	f, err := os.OpenFile(edgePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	cctx, cancel := context.WithCancel(ctx)
	c := &Checker{lastProg: map[string]string{}, edgeFile: f, w: w, cancel: cancel, done: make(chan struct{})}

	go func() {
		defer close(c.done)

		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-cctx.Done():
				return
			case <-ticker.C:
				c.check(cctx)
			}
		}
	}()

	return c, nil
}

func (c *Checker) check(ctx context.Context) {
	drpcs := &rmn.DRPlacementControlList{}
	if err := c.w.Hub.Client.List(ctx, drpcs); err != nil {
		return
	}

	for i := range drpcs.Items {
		drpc := &drpcs.Items[i]
		c.recordEdge(drpc)
		c.checkSinglePrimary(ctx, drpc)
	}
}

func (c *Checker) recordEdge(drpc *rmn.DRPlacementControl) {
	key := drpc.Namespace + "/" + drpc.Name
	cur := string(drpc.Status.Progression)

	c.mu.Lock()
	defer c.mu.Unlock()

	if prev, ok := c.lastProg[key]; ok && prev != cur && cur != "" {
		fmt.Fprintf(c.edgeFile, "%s: %s -> %s\n", key, prev, cur)
	}
	if cur != "" {
		c.lastProg[key] = cur
	}
}

func (c *Checker) checkSinglePrimary(ctx context.Context, drpc *rmn.DRPlacementControl) {
	primaries := 0

	for _, m := range c.w.Managed() {
		vrg := &rmn.VolumeReplicationGroup{}
		key := types.NamespacedName{Name: drpc.Name, Namespace: world.RamenOpsNS}
		if err := m.Client.Get(ctx, key, vrg); err != nil {
			continue
		}
		if vrg.Spec.ReplicationState == rmn.Primary {
			primaries++
		}
	}

	if violatesSinglePrimary(primaries, drpc) {
		c.addViolation(fmt.Sprintf("single-primary violated for %s/%s: %d primaries in phase %s",
			drpc.Namespace, drpc.Name, primaries, drpc.Status.Phase))
	}
}

// violatesSinglePrimary encodes the documented transitional window: during
// failover the old primary is presumed unreachable and stays spec-primary
// until post-failover cleanup completes (PeerReady). Everywhere else, two
// primaries is split-brain.
func violatesSinglePrimary(primaries int, drpc *rmn.DRPlacementControl) bool {
	if primaries <= 1 {
		return false
	}

	switch drpc.Status.Phase {
	case rmn.FailingOver:
		return false
	case rmn.FailedOver:
		return peerReady(drpc)
	default:
		return true
	}
}

func peerReady(drpc *rmn.DRPlacementControl) bool {
	return meta.IsStatusConditionTrue(drpc.Status.Conditions, rmn.ConditionPeerReady)
}

func (c *Checker) addViolation(v string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, existing := range c.violations {
		if existing == v {
			return
		}
	}
	c.violations = append(c.violations, v)
}

func (c *Checker) Violations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]string, len(c.violations))
	copy(out, c.violations)

	return out
}

func (c *Checker) AssertClean(t interface{ Fatalf(string, ...any) }) {
	if v := c.Violations(); len(v) > 0 {
		t.Fatalf("invariant violations: %v", v)
	}
}

func (c *Checker) Stop() {
	c.cancel()
	<-c.done
	c.edgeFile.Close()
}

var _ = metav1.Now // keep metav1 import if only used in tests
```

(Remove the trailing `var _` line if `metav1` ends up unused in the final file. The VRG lookup key assumes discovered-app VRGs land in `ramen-ops` with the DRPC's name — verified in the contract extraction: VRG name = DRPC name, namespace = vrgNamespace = RamenOpsNamespace for discovered apps.)

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go test ./invariants/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add simtest/invariants/
git commit -s -m "simtest: global invariant checker with transitional windows

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 13: T1 baseline tests and reusable drivers

**Files:**
- Create: `simtest/tests/main_test.go`, `simtest/tests/baseline_test.go`

**Interfaces:**
- Produces (consumed by Tasks 14-15):
  - `type Hook struct { At rmn.ProgressionStatus; Do func() }`
  - `runEnable(t, w, app) *observe.Recorder` — CreateApp on dr1 + EnableProtection + wait Deployed/Ready.
  - `runFailover(t, w, app, hooks ...Hook)` — Failover to dr2, auto-DeleteApp(dr1) at `WaitOnUserToCleanUp`, wait FailedOver + Ready.
  - `runRelocate(t, w, app, hooks ...Hook)` — Relocate to dr1, auto-DeleteApp(dr2) at `WaitOnUserToCleanUp`, wait Relocated + Ready.
  - `getWorld(t) (*world.World, *invariants.Checker)` — the shared world + checker, created once inside the top-level wrapper test.

- [ ] **Step 1: Write the tests**

`simtest/tests/main_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ramendr/ramen/simtest/invariants"
	"github.com/ramendr/ramen/simtest/world"
)

var (
	worldOnce sync.Once
	sharedW   *world.World
	sharedC   *invariants.Checker
)

// getWorld lazily builds one world per test binary. The first caller's t owns
// cleanup, so every test funcs through the same top-level Test* functions in
// this package, which the go test runner executes sequentially per package —
// subtests inside them may parallelize.
func getWorld(t *testing.T) (*world.World, *invariants.Checker) {
	t.Helper()

	worldOnce.Do(func() {
		if _, err := os.Stat(world.ManagerBin()); err != nil {
			t.Skipf("bin/manager not built: %v", err)
		}
		sharedW = world.New(t)

		var err error
		sharedC, err = invariants.StartChecker(context.Background(), sharedW,
			filepath.Join(sharedW.Dir, "progression-edges.log"))
		if err != nil {
			t.Fatalf("start invariant checker: %v", err)
		}
	})

	if sharedW == nil {
		t.Skip("world unavailable")
	}

	return sharedW, sharedC
}
```

`simtest/tests/baseline_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"

	"github.com/ramendr/ramen/simtest/observe"
	"github.com/ramendr/ramen/simtest/user"
	"github.com/ramendr/ramen/simtest/world"
)

const (
	enableTimeout = 3 * time.Minute
	moveTimeout   = 5 * time.Minute
)

type Hook struct {
	At rmn.ProgressionStatus
	Do func()
}

func newRecorder(t *testing.T, w *world.World, app user.App) *observe.Recorder {
	t.Helper()

	r, err := observe.NewRecorder(context.Background(), w.Hub.Cfg, app.ManagementNamespace(), app.Name)
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}
	t.Cleanup(r.Stop)

	return r
}

func runEnable(t *testing.T, w *world.World, app user.App) *observe.Recorder {
	t.Helper()
	ctx := context.Background()

	if err := user.CreateApp(ctx, w, app, world.DR1Name); err != nil {
		t.Fatalf("create app: %v", err)
	}

	rec := newRecorder(t, w, app)

	if err := user.EnableProtection(ctx, w, app); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if err := rec.WaitPhase(string(rmn.Deployed), enableTimeout); err != nil {
		t.Fatal(err)
	}
	if err := observe.WaitDRPCReady(ctx, w.Hub.Client, app.ManagementNamespace(), app.Name, enableTimeout); err != nil {
		t.Fatal(err)
	}

	return rec
}

func runMove(t *testing.T, w *world.World, app user.App, rec *observe.Recorder,
	action func(context.Context) error, cleanupCluster string, donePhase rmn.DRState, hooks []Hook,
) {
	t.Helper()
	ctx := context.Background()

	for _, h := range hooks {
		rec.OnProgression(string(h.At), h.Do)
	}

	// Discovered apps park at WaitOnUserToCleanUp until the user deletes the
	// workload on the old cluster.
	rec.OnProgression(string(rmn.ProgressionWaitOnUserToCleanUp), func() {
		if err := user.DeleteApp(ctx, w, app, cleanupCluster); err != nil {
			t.Errorf("cleanup app on %s: %v", cleanupCluster, err)
		}
	})

	if err := action(ctx); err != nil {
		t.Fatalf("action: %v", err)
	}
	if err := rec.WaitPhase(string(donePhase), moveTimeout); err != nil {
		t.Fatal(err)
	}
	if err := observe.WaitDRPCReady(ctx, w.Hub.Client, app.ManagementNamespace(), app.Name, moveTimeout); err != nil {
		t.Fatal(err)
	}
}

func runFailover(t *testing.T, w *world.World, app user.App, rec *observe.Recorder, hooks ...Hook) {
	runMove(t, w, app, rec,
		func(ctx context.Context) error { return user.Failover(ctx, w, app, world.DR2Name) },
		world.DR1Name, rmn.FailedOver, hooks)
}

func runRelocate(t *testing.T, w *world.World, app user.App, rec *observe.Recorder, hooks ...Hook) {
	runMove(t, w, app, rec,
		func(ctx context.Context) error { return user.Relocate(ctx, w, app, world.DR1Name) },
		world.DR2Name, rmn.Relocated, hooks)
}

// TestBaselines is T1: the full happy-path lifecycle of one discovered app.
func TestBaselines(t *testing.T) {
	w, checker := getWorld(t)
	app := user.App{Name: "baseline"}
	ctx := context.Background()

	rec := runEnable(t, w, app)

	t.Run("failover", func(t *testing.T) { runFailover(t, w, app, rec) })
	t.Run("relocate", func(t *testing.T) { runRelocate(t, w, app, rec) })

	t.Run("disable", func(t *testing.T) {
		if err := user.Disable(ctx, w, app, observe.Scale(2*time.Minute)); err != nil {
			t.Fatal(err)
		}

		// Cleanup completeness: no VRG remains on either managed cluster.
		deadline := time.Now().Add(observe.Scale(2 * time.Minute))
		for {
			left := 0
			for _, m := range w.Managed() {
				vrgs := &rmn.VolumeReplicationGroupList{}
				if err := m.Client.List(ctx, vrgs); err == nil {
					left += len(vrgs.Items)
				}
			}
			if left == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d VRGs left after disable", left)
			}
			time.Sleep(500 * time.Millisecond)
		}
	})

	checker.AssertClean(t)
	t.Logf("baseline progression sequence: %v", rec.Progressions())
}
```

- [ ] **Step 2: Run T1**

Run: `cd simtest && go test ./tests/ -run TestBaselines -v -timeout 30m`
Expected: PASS. **This is the make-or-break integration point.** Budget debugging time. Failure triage order: (1) `hub.log` — DRPC events and progression, (2) `actors.log` — did the work agent apply the VRG MW? did MCVs fulfill?, (3) `dr1.log`/`dr2.log` — VRG reconcile errors (class matching, S3 upload), (4) `kubectl --kubeconfig .artifacts/<t>/hub.kubeconfig get drpc -A -o yaml` while the test hangs (envtest stays up until teardown). Likely first-run issues and their fixes belong in code, not the plan: VRG namespace mismatch (check DRPC `ProtectedNamespaces` vs `ramen-ops` VRG placement), missing `ManagedClusterSetBinding` (create it in bootstrap only if the DRPC controller logs demand it — the CRD is absent from `hack/test`, so if needed, add the CRD YAML under `simtest/testdata/crds/` and append that dir to `CRDDirectoryPaths` in `cluster.go`; never edit `hack/test`).

- [ ] **Step 3: Commit**

```bash
git add simtest/tests/
git commit -s -m "simtest: T1 baseline lifecycle tests with reusable drivers

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 14: T2 fault matrix + T3 ordering variants

**Files:**
- Create: `simtest/tests/matrix_test.go`

**Interfaces:**
- Consumes: baseline drivers (Task 13), `actors` policies, `world.S3Server.SetDown`.

- [ ] **Step 1: Write the matrix test**

`simtest/tests/matrix_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"

	"github.com/ramendr/ramen/simtest/actors"
	"github.com/ramendr/ramen/simtest/observe"
	"github.com/ramendr/ramen/simtest/user"
	"github.com/ramendr/ramen/simtest/world"
)

const faultWindow = 8 * time.Second

// fault is one injectable external failure; clear() must fully restore health.
type fault struct {
	name  string
	apply func(w *world.World)
	clear func(w *world.World)
}

func policyFault(name string, key func(string) actors.Key, cluster string, p actors.Policy) fault {
	return fault{
		name:  name,
		apply: func(w *world.World) { w.Actors.Store.Set(key(cluster), p) },
		clear: func(w *world.World) { w.Actors.Store.Set(key(cluster), actors.Normal{}) },
	}
}

func faults() []fault {
	fs := []fault{
		{
			name:  "s3-down",
			apply: func(w *world.World) { w.S3.SetDown(true) },
			clear: func(w *world.World) { w.S3.SetDown(false) },
		},
	}

	for _, cluster := range []string{world.DR1Name, world.DR2Name} {
		fs = append(fs,
			policyFault("volrep-silent-"+cluster, actors.VolRep, cluster, actors.Silent{}),
			policyFault("volrep-degraded-"+cluster, actors.VolRep, cluster, actors.FailWith{Mode: "degraded"}),
			policyFault("view-silent-"+cluster, actors.View, cluster, actors.Silent{}),
			policyFault("work-silent-"+cluster, actors.Work, cluster, actors.Silent{}),
			// T3 ordering variants: delays reorder independent external events
			// (e.g. VR status lands before/after the MCV refresh at the same gate).
			policyFault("volrep-delayed-"+cluster, actors.VolRep, cluster, actors.Delayed{After: 3 * time.Second}),
			policyFault("view-delayed-"+cluster, actors.View, cluster, actors.Delayed{After: 3 * time.Second}),
		)
	}

	return fs
}

// TestMatrix is T2 (+T3 via the delayed faults): run the failover baseline
// once to record its checkpoints, then for each checkpoint x fault, run a
// fresh app through failover with the fault injected at that checkpoint and
// cleared after faultWindow. Recovery to completion is the assertion; the
// invariant checker guards safety throughout.
func TestMatrix(t *testing.T) {
	w, checker := getWorld(t)

	// Seed run: discover the failover checkpoint sequence.
	seed := user.App{Name: "matrix-seed"}
	rec := runEnable(t, w, seed)
	runFailover(t, w, seed, rec)

	checkpoints := prefixBefore(rec.Progressions(), string(rmn.ProgressionCompleted))
	if len(checkpoints) == 0 {
		t.Fatalf("no checkpoints recorded; progressions=%v", rec.Progressions())
	}
	t.Logf("failover checkpoints: %v", checkpoints)

	n := 0

	for _, cp := range checkpoints {
		for _, f := range faults() {
			cp, f := cp, f
			n++
			name := fmt.Sprintf("failover/at=%s/fault=%s", cp, f.name)

			t.Run(name, func(t *testing.T) {
				app := user.App{Name: sanitize(fmt.Sprintf("mx-%s-%s", cp, f.name))}
				rec := runEnable(t, w, app)

				runFailover(t, w, app, rec, Hook{
					At: rmn.ProgressionStatus(cp),
					Do: func() {
						f.apply(w)
						w.Actors.Log.Logf("matrix: applied %s at %s for %s", f.name, cp, app.Name)
						time.AfterFunc(observe.Scale(faultWindow), func() {
							f.clear(w)
							w.Actors.Log.Logf("matrix: cleared %s for %s", f.name, app.Name)
						})
					},
				})
			})
		}
	}

	t.Logf("matrix ran %d combinations", n)
	checker.AssertClean(t)
}

// prefixBefore returns the sequence up to (excluding) the first occurrence of
// stop, deduplicated.
func prefixBefore(seq []string, stop string) []string {
	out := []string{}
	seen := map[string]bool{}

	for _, s := range seq {
		if s == stop {
			break
		}
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}

	return out
}

func sanitize(s string) string {
	s = strings.ToLower(s)
	s = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}

		return '-'
	}, s)
	if len(s) > 40 {
		s = s[:40]
	}

	return strings.Trim(s, "-")
}
```

(Notes: subtests run sequentially by default — leave them sequential in the first version; measure, then add `t.Parallel()` with a bounded `-parallel` only if wall-clock demands it and the apps prove independent. Faults whose target is irrelevant at a checkpoint (e.g. volrep-silent on dr1 after dr1's role is finished) still assert the recovery property — that's coverage of "irrelevant fault does not wedge the machine", keep them. If total wall-clock is unacceptable, cut checkpoints to phase boundaries first, faults second; `log()` what was dropped via `t.Logf`.)

- [ ] **Step 2: Run the matrix**

Run: `cd simtest && go test ./tests/ -run 'TestBaselines|TestMatrix' -v -timeout 90m`
Expected: PASS (TestBaselines first — the matrix reuses the same world). On failures, each subtest name pinpoints checkpoint × fault; `actors.log` has the apply/clear markers around the incident.

- [ ] **Step 3: Commit**

```bash
git add simtest/tests/
git commit -s -m "simtest: T2/T3 fault matrix over failover checkpoints

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 15: T4 crash/recovery tests

**Files:**
- Create: `simtest/tests/recovery_test.go`

**Interfaces:**
- Consumes: `world.KillManager`/`RestartManager`, baseline drivers.

- [ ] **Step 1: Write the test**

`simtest/tests/recovery_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"fmt"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"

	"github.com/ramendr/ramen/simtest/observe"
	"github.com/ramendr/ramen/simtest/user"
	"github.com/ramendr/ramen/simtest/world"
)

// TestCrashRecovery is T4: SIGKILL an operator at a progression checkpoint,
// restart it after a beat, and require the action to complete. Runs serially
// (killing an operator affects every in-flight app).
func TestCrashRecovery(t *testing.T) {
	w, checker := getWorld(t)

	// Checkpoints chosen from the failover flow's coarse phases; the fine
	// matrix already covers external-event faults, this covers operator loss.
	cases := []struct {
		victim     string
		checkpoint rmn.ProgressionStatus
	}{
		{world.HubName, rmn.ProgressionFailingOverToCluster},
		{world.HubName, rmn.ProgressionWaitingForResourceRestore},
		{world.DR2Name, rmn.ProgressionWaitingForResourceRestore}, // new primary's operator dies mid-restore
		{world.HubName, rmn.ProgressionCleaningUp},
		{world.DR1Name, rmn.ProgressionCleaningUp}, // old primary's operator dies during cleanup
	}

	for i, tc := range cases {
		tc := tc
		name := fmt.Sprintf("kill=%s/at=%s", tc.victim, tc.checkpoint)

		t.Run(name, func(t *testing.T) {
			app := user.App{Name: fmt.Sprintf("crash-%d", i)}
			rec := runEnable(t, w, app)

			runFailover(t, w, app, rec, Hook{
				At: tc.checkpoint,
				Do: func() {
					if err := w.KillManager(tc.victim); err != nil {
						t.Errorf("kill %s: %v", tc.victim, err)

						return
					}
					w.Actors.Log.Logf("crash: killed %s at %s", tc.victim, tc.checkpoint)
					time.AfterFunc(observe.Scale(3*time.Second), func() {
						if err := w.RestartManager(tc.victim); err != nil {
							t.Errorf("restart %s: %v", tc.victim, err)
						}
						w.Actors.Log.Logf("crash: restarted %s", tc.victim)
					})
				},
			})
		})
	}

	checker.AssertClean(t)
}
```

(Progression constant names — `rmn.ProgressionFailingOverToCluster`, `rmn.ProgressionWaitingForResourceRestore`, `rmn.ProgressionCleaningUp` — verify exact exported identifiers in `api/v1alpha1/drplacementcontrol_types.go`; `Cleaning Up` has a space in its string value, the Go constant differs. If a chosen checkpoint never appears in this environment's failover sequence (hooks that never fire), the fault window simply never opens and the subtest degenerates to a plain failover — detect this by asserting the hook fired when `rec.Progressions()` contains the checkpoint, and `t.Skip` otherwise.)

- [ ] **Step 2: Run**

Run: `cd simtest && go test ./tests/ -run 'TestBaselines|TestCrashRecovery' -v -timeout 45m`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add simtest/tests/
git commit -s -m "simtest: T4 operator crash/recovery tests

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 16: README, full run, wrap-up

**Files:**
- Create: `simtest/README.md`

- [ ] **Step 1: Write README**

`simtest/README.md`:

```markdown
<!--
SPDX-FileCopyrightText: The RamenDR authors
SPDX-License-Identifier: Apache-2.0
-->

# simtest — state-space e2e tests for Ramen

simtest runs the **unmodified** ramen operators (`bin/manager`) against three
in-process envtest control planes (hub, dr1, dr2). The framework plays every
external system ramen depends on — the OCM work/view agents, csi-addons
VolumeReplication, PVC binding, S3 — so tests can deliver any external event
late, wrong, or never, at any point in the DR state machine.

## Running

    make simtest          # builds ../bin/manager, installs envtest assets, runs everything

    # a single suite:
    go test ./tests/ -run TestBaselines -v -timeout 30m

Environment:
- `SIMTEST_TIMEOUT_SCALE=3` widens every wait (slow CI).
- Artifacts (operator logs, actor event log, progression edges) land in
  `.artifacts/<test>-<timestamp>/`.

## Layout

- `world/` — envtest clusters, fake S3, bootstrap objects, operator processes
- `actors/` — simulated external systems with per-actor fault policies
  (`Normal`, `Silent`, `Delayed`, `FailWith`)
- `observe/`, `user/` — DRPC gates/recorders and user-level actions
- `invariants/` — always-on safety checks (single-primary, progression edges)
- `tests/` — T1 baselines, T2/T3 fault matrix, T4 crash/recovery

## Test taxonomy

- **T1** happy-path lifecycle: enable → failover → relocate → disable
- **T2** fault matrix: every failover checkpoint × every fault, assert safe
  parking + recovery after the fault clears
- **T3** ordering: delayed-actor variants inside the matrix
- **T4** crash/recovery: SIGKILL an operator at a checkpoint, restart, recover

Deferred: VolSync/Velero/snapshot/fence actors, Metro-DR, consistency groups,
randomized exploration. Porting guides live in `internal/controller/mock/`
(copy logic; `internal/` cannot be imported from this module).
```

- [ ] **Step 2: Full verification run**

Run: `cd simtest && gofmt -l . && go vet ./... && make simtest`
Expected: no gofmt diffs, vet clean, all suites PASS. Also confirm zero changes outside `simtest/`: `git status --porcelain -- ':!simtest' ':!docs/superpowers'` prints nothing.

- [ ] **Step 3: Commit**

```bash
git add simtest/README.md
git commit -s -m "simtest: README and test taxonomy

Assisted-by: Claude Code/claude-fable-5"
```

---

## Self-Review Notes

- **Spec coverage:** world/actors/scenario-equivalents/invariants/matrix/T1-T4 all have tasks. The spec's `scenario` package is realized as the leaner `Hook` + drivers + `observe` gates (same primitives: Step=user calls, Gate=recorder waits, Perturb=policy sets, Expect=ready/phase waits); the spec's velero/volsync/snapshotter/fencer actors and `DropNext` are explicitly deferred with `volSync.disabled`/`kubeObjectProtection.disabled` making them unreachable in v1.
- **Known verification points** are marked inline (exact constant names in `api/v1alpha1`, `ViewScope` field names, `DRClusters` array type) — each sits inside a task whose test cycle catches a mismatch immediately.
- **Type consistency check done:** `ClusterRef`/`Runtime`/`Store`/`Decision`, `Recorder` methods, `App` methods, `Hook` are used with identical signatures across Tasks 7-15.
```
