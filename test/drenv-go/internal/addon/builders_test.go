// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon_test

// builder_test.go exercises the ported addon builders via FakeRunner, asserting
// argv-level correctness against the Python source. No real clusters are used.

import (
	"context"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const testCluster = "dr1"

// testDeps builds an addon.Deps backed by the given FakeRunner and addonsDir.
func testDeps(f *cli.FakeRunner, addonsDir string) addon.Deps {
	k := &cli.Kubectl{R: f}
	mc := &cli.MC{R: f}
	opts := ensure.Options{
		VerifyTimeout:  2 * time.Second,
		VerifyInterval: 10 * time.Millisecond,
	}
	return addon.Deps{
		K:         k,
		MC:        mc,
		AddonsDir: addonsDir,
		Opts:      opts,
	}
}

// runStep invokes a builder by name, then runs ensure.Ensure on the result.
// It fails the test if the builder is not registered or Ensure returns an error.
func runStep(t *testing.T, f *cli.FakeRunner, addonsDir, name, cluster string, args []string) {
	t.Helper()
	b, ok := addon.Lookup(name)
	if !ok {
		t.Fatalf("addon %q not registered", name)
	}
	d := testDeps(f, addonsDir)
	step := b(d, cluster, args)
	if _, err := ensure.Ensure(context.Background(), step, d.Opts); err != nil {
		t.Fatalf("Ensure(%q): %v", name, err)
	}
}

// callArgs returns the Args slice of the i-th call, failing the test if i is
// out of range.
func callArgs(t *testing.T, f *cli.FakeRunner, i int) []string {
	t.Helper()
	if i >= len(f.Calls) {
		t.Fatalf("call[%d]: only %d calls recorded", i, len(f.Calls))
	}
	return f.Calls[i].Args
}

// assertArgsEqual fails the test if got != want.
func assertArgsEqual(t *testing.T, label string, got, want []string) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s args:\n got  %v\n want %v", label, got, want)
	}
}

// ---- external-snapshotter ----

// TestExternalSnapshotterArgv verifies that the external-snapshotter builder
// produces the correct kubectl calls in the correct order:
//  1. apply --kustomize <crds-dir>
//  2. wait --for=condition=established crd --all (with timeout)
//  3. apply --kustomize <controller-dir>
//  4. rollout status kube-system deploy/snapshot-controller (with timeout)
func TestExternalSnapshotterArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}

	runStep(t, f, addonsDir, "external-snapshotter", testCluster, nil)

	if len(f.Calls) != 4 {
		t.Fatalf("expected 4 kubectl calls, got %d:\n%v", len(f.Calls), callNames(f))
	}

	crdsDir := filepath.Join(addonsDir, "external_snapshotter", "start-data", "crds")
	controllerDir := filepath.Join(addonsDir, "external_snapshotter", "start-data", "controller")

	assertArgsEqual(t, "apply-crds", callArgs(t, f, 0), []string{
		"--context", testCluster, "apply", "--kustomize", crdsDir,
	})
	// wait for established: kubectl --context dr1 wait crd --all --for=condition=established --timeout Xs
	assertArgsContain(t, "wait-crds", callArgs(t, f, 1),
		"--context", testCluster, "wait", "crd", "--all", "--for=condition=established")
	assertArgsEqual(t, "apply-controller", callArgs(t, f, 2), []string{
		"--context", testCluster, "apply", "--kustomize", controllerDir,
	})
	// rollout status kube-system deploy/snapshot-controller
	assertArgsContain(t, "rollout-controller", callArgs(t, f, 3),
		"--context", testCluster, "-n", "kube-system", "rollout", "status", "deploy/snapshot-controller")
}

// ---- olm ----

// TestOLMArgv verifies the OLM builder uses server-side apply for CRDs and
// produces all expected wait/rollout calls.
func TestOLMArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}

	runStep(t, f, addonsDir, "olm", testCluster, nil)

	if len(f.Calls) != 8 {
		t.Fatalf("expected 8 kubectl calls, got %d:\n%v", len(f.Calls), callNames(f))
	}

	crdsDir := filepath.Join(addonsDir, "olm", "start-data", "crds")
	operatorsDir := filepath.Join(addonsDir, "olm", "start-data", "operators")

	// 0: apply --server-side=true --kustomize <crds-dir>
	assertArgsEqual(t, "apply-crds-server-side", callArgs(t, f, 0), []string{
		"--context", testCluster, "apply", "--server-side=true", "--kustomize", crdsDir,
	})
	// 1: wait crd --all --for=condition=established
	assertArgsContain(t, "wait-crds", callArgs(t, f, 1),
		"--context", testCluster, "wait", "crd", "--all", "--for=condition=established")
	// 2: apply --kustomize <operators-dir>
	assertArgsEqual(t, "apply-operators", callArgs(t, f, 2), []string{
		"--context", testCluster, "apply", "--kustomize", operatorsDir,
	})
	// 3: rollout status olm deploy/olm-operator
	assertArgsContain(t, "rollout-olm-operator", callArgs(t, f, 3),
		"--context", testCluster, "-n", "olm", "rollout", "status", "deploy/olm-operator")
	// 4: rollout status olm deploy/catalog-operator
	assertArgsContain(t, "rollout-catalog-operator", callArgs(t, f, 4),
		"--context", testCluster, "-n", "olm", "rollout", "status", "deploy/catalog-operator")
	// 5: wait csv/packageserver --for=create -n olm
	assertArgsContain(t, "wait-packageserver-create", callArgs(t, f, 5),
		"--context", testCluster, "-n", "olm", "wait", "csv/packageserver", "--for=create")
	// 6: wait csv/packageserver --for=jsonpath={.status.phase}=Succeeded -n olm
	assertArgsContain(t, "wait-packageserver-succeeded", callArgs(t, f, 6),
		"--context", testCluster, "-n", "olm", "wait", "csv/packageserver",
		"--for=jsonpath={.status.phase}=Succeeded")
	// 7: rollout status olm deploy/packageserver
	assertArgsContain(t, "rollout-packageserver", callArgs(t, f, 7),
		"--context", testCluster, "-n", "olm", "rollout", "status", "deploy/packageserver")
}

// ---- minio ----

// TestMinioArgv verifies the minio builder produces apply, rollout, mc alias,
// and mc mb calls in the correct order. Two kubectl calls are needed for
// MinioServiceURL (pod hostIP + service nodePort), then mc alias set, mc mb.
func TestMinioArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}

	// Script results for all 6 calls in order (Run and Output share the queue):
	// 0: kubectl apply (Run) → nil error, no output needed
	// 1: kubectl rollout status (Run) → nil error, no output needed
	// 2: kubectl get pods ... hostIP (Output) → "192.168.64.10"
	// 3: kubectl get service ... nodePort (Output) → "30000"
	// 4: mc alias set (Run) → nil error
	// 5: mc mb (Run) → nil error
	f.Script(cli.FakeResult{})                     // apply
	f.Script(cli.FakeResult{})                     // rollout status
	f.Script(cli.FakeResult{Out: "192.168.64.10"}) // pod hostIP
	f.Script(cli.FakeResult{Out: "30000"})         // service nodePort
	// mc calls consume remaining (empty) queue entries → nil error

	runStep(t, f, addonsDir, "minio", testCluster, nil)

	// Calls:
	// 0: kubectl apply --filename <minio.yaml>
	// 1: kubectl rollout status minio deployment/minio
	// 2: kubectl get pods ... (hostIP) — Output
	// 3: kubectl get service ... (nodePort) — Output
	// 4: mc alias set <cluster> <url> minio minio123
	// 5: mc mb --ignore-existing <cluster>/bucket
	if len(f.Calls) != 6 {
		t.Fatalf("expected 6 calls, got %d:\n%v", len(f.Calls), callNames(f))
	}

	minioYAML := filepath.Join(addonsDir, "minio", "start-data", "minio.yaml")
	assertArgsEqual(t, "apply", callArgs(t, f, 0), []string{
		"--context", testCluster, "apply", "--filename", minioYAML,
	})
	assertArgsContain(t, "rollout", callArgs(t, f, 1),
		"--context", testCluster, "-n", "minio", "rollout", "status", "deployment/minio")

	// hostIP query: the resource and selector must be SEPARATE argv tokens,
	// not a single embedded string (which kubectl would reject).
	assertArgsEqual(t, "get-hostIP", callArgs(t, f, 2), []string{
		"--context", testCluster, "-n", "minio", "get",
		"pod", "--selector=component=minio",
		"--output=jsonpath={.items[0].status.hostIP}",
	})
	assertArgsContain(t, "get-nodePort", callArgs(t, f, 3),
		"--context", testCluster, "-n", "minio", "get",
		"--output=jsonpath={.spec.ports[0].nodePort}")

	// mc alias set dr1 http://192.168.64.10:30000 minio minio123
	assertArgsEqual(t, "mc-alias-set", callArgs(t, f, 4), []string{
		"alias", "set", testCluster, "http://192.168.64.10:30000", "minio", "minio123",
	})

	// mc mb --ignore-existing dr1/bucket
	assertArgsEqual(t, "mc-mb", callArgs(t, f, 5), []string{
		"mb", "--ignore-existing", testCluster + "/bucket",
	})
}

// ---- helpers ----

// callNames returns a slice of "<name> <args>" strings for all recorded calls,
// for use in diagnostic failure messages.
func callNames(f *cli.FakeRunner) []string {
	out := make([]string, len(f.Calls))
	for i, c := range f.Calls {
		out[i] = c.Name + " " + strings.Join(c.Args, " ")
	}
	return out
}

// assertArgsContain checks that all wanted elements appear in got (order
// matters for the elements that are contiguous, but we only assert that each
// wanted element appears in the args slice at the same relative position).
// This is a "subsequence" check — it passes as long as wanted elements appear
// in order somewhere in got.
func assertArgsContain(t *testing.T, label string, got []string, want ...string) {
	t.Helper()
	gi := 0
	for _, w := range want {
		found := false
		for ; gi < len(got); gi++ {
			if got[gi] == w {
				found = true
				gi++
				break
			}
		}
		if !found {
			t.Errorf("%s: args %v missing %q (after position %d)", label, got, w, gi)
			return
		}
	}
}

// TestRecipeArgv verifies the recipe builder issues exactly one kustomize apply.
func TestRecipeArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}
	runStep(t, f, addonsDir, "recipe", testCluster, nil)

	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 kubectl call, got %d: %v", len(f.Calls), callNames(f))
	}
	startData := filepath.Join(addonsDir, "recipe", "start-data")
	assertArgsEqual(t, "apply", callArgs(t, f, 0), []string{
		"--context", testCluster, "apply", "--kustomize", startData,
	})
}

// TestCSIAddonsArgv verifies the csi-addons builder issues apply + rollout.
func TestCSIAddonsArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}
	runStep(t, f, addonsDir, "csi-addons", testCluster, nil)

	if len(f.Calls) != 2 {
		t.Fatalf("expected 2 kubectl calls, got %d: %v", len(f.Calls), callNames(f))
	}
	startData := filepath.Join(addonsDir, "csi_addons", "start-data")
	assertArgsEqual(t, "apply", callArgs(t, f, 0), []string{
		"--context", testCluster, "apply", "--kustomize", startData,
	})
	assertArgsContain(t, "rollout", callArgs(t, f, 1),
		"--context", testCluster, "-n", "csi-addons-system",
		"rollout", "status", "deployment/csi-addons-controller-manager")
}

// TestOCMControllerArgv verifies the ocm/controller builder issues apply + rollout.
func TestOCMControllerArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}
	runStep(t, f, addonsDir, "ocm-controller", testCluster, nil)

	if len(f.Calls) != 2 {
		t.Fatalf("expected 2 kubectl calls, got %d: %v", len(f.Calls), callNames(f))
	}
	startData := filepath.Join(addonsDir, "ocm", "controller", "start-data")
	assertArgsEqual(t, "apply", callArgs(t, f, 0), []string{
		"--context", testCluster, "apply", "--kustomize", startData,
	})
	assertArgsContain(t, "rollout", callArgs(t, f, 1),
		"--context", testCluster, "-n", "open-cluster-management",
		"rollout", "status", "deploy/ocm-controller")
}

// TestODFExternalSnapshotterArgv verifies the odf-external-snapshotter builder
// issues apply-crds + wait-established.
func TestODFExternalSnapshotterArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}
	runStep(t, f, addonsDir, "odf-external-snapshotter", testCluster, nil)

	if len(f.Calls) != 2 {
		t.Fatalf("expected 2 kubectl calls, got %d: %v", len(f.Calls), callNames(f))
	}
	crdsDir := filepath.Join(addonsDir, "odf_external_snapshotter", "start-data", "crds")
	assertArgsEqual(t, "apply-crds", callArgs(t, f, 0), []string{
		"--context", testCluster, "apply", "--kustomize", crdsDir,
	})
	assertArgsContain(t, "wait-crds", callArgs(t, f, 1),
		"--context", testCluster, "wait", "crd", "--all", "--for=condition=established")
}
