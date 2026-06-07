// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func TestKubectlApplyFileIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.ApplyFile(ctx, "dr1", "manifest.yaml"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{"--context", "dr1", "apply", "--filename", "manifest.yaml"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlApplyKustomizeDirIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.ApplyKustomizeDir(ctx, "dr1", "./overlays/dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"--context", "dr1", "apply", "--kustomize", "./overlays/dr1"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlApplyServerSideFileIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.ApplyServerSideFile(ctx, "hub", "crds.yaml"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"--context", "hub", "apply", "--server-side=true", "--filename", "crds.yaml"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlApplyStdinIssuesCorrectArgvAndPassesStdin(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	manifest := []byte("apiVersion: v1\nkind: ConfigMap\n")
	if err := k.ApplyStdin(ctx, "dr2", manifest); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	wantArgs := []string{"--context", "dr2", "apply", "--filename", "-"}
	if !reflect.DeepEqual(c.Args, wantArgs) {
		t.Errorf("args = %v, want %v", c.Args, wantArgs)
	}
	if c.Stdin != string(manifest) {
		t.Errorf("stdin = %q, want %q", c.Stdin, string(manifest))
	}
}

func TestKubectlWaitForWithNamespaceIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	timeout := 3 * time.Minute
	if err := k.WaitFor(ctx, "hub", "ramen-system", "condition=established", timeout, "crd/volumereplicationgroups.ramendr.openshift.io"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "hub",
		"-n", "ramen-system",
		"wait", "crd/volumereplicationgroups.ramendr.openshift.io",
		"--for=condition=established",
		"--timeout", "180s",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlWaitForWithoutNamespaceOmitsNFlag(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.WaitFor(ctx, "dr1", "", "condition=Ready", 30*time.Second, "pod/foo"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "dr1",
		"wait", "pod/foo",
		"--for=condition=Ready",
		"--timeout", "30s",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlWaitForFileIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.WaitForFile(ctx, "dr1", "condition=Ready", "deploy.yaml", 2*time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "dr1",
		"wait",
		"--for=condition=Ready",
		"--filename", "deploy.yaml",
		"--timeout", "120s",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlRolloutStatusIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.RolloutStatus(ctx, "hub", "olm", "deploy/olm-operator", 5*time.Minute); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "hub",
		"-n", "olm",
		"rollout", "status", "deploy/olm-operator",
		"--timeout", "300s",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlGetJSONPathIssuesCorrectArgvAndReturnsOutput(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "192.168.64.10"})
	k := cli.Kubectl{R: f}

	out, err := k.GetJSONPath(ctx, "dr1", "minio", "pods", "{.items[0].status.hostIP}")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "dr1",
		"-n", "minio",
		"get", "pods",
		"--output=jsonpath={.items[0].status.hostIP}",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
	if out != "192.168.64.10" {
		t.Errorf("output = %q, want %q", out, "192.168.64.10")
	}
}

func TestKubectlKubectlExecIssuesCorrectArgvAndReturnsOutput(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "ceph status output"})
	k := cli.Kubectl{R: f}

	out, err := k.KubectlExec(ctx, "dr1", "rook-ceph", "deploy/rook-ceph-tools", "ceph", "status")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "dr1",
		"-n", "rook-ceph",
		"exec", "deploy/rook-ceph-tools",
		"--", "ceph", "status",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
	if out != "ceph status output" {
		t.Errorf("output = %q, want non-empty", out)
	}
}

func TestKubectlPatchIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.Patch(ctx, "dr1", "ramen-system", "deployment/ramen-operator", "merge", `{"spec":{"replicas":0}}`); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "dr1",
		"-n", "ramen-system",
		"patch", "deployment/ramen-operator",
		"--type=merge",
		`--patch={"spec":{"replicas":0}}`,
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlAnnotateIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.Annotate(ctx, "dr1", "namespace/ramen-system", "backup.velero.io/backup-volumes=false"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "dr1",
		"annotate", "namespace/ramen-system",
		"backup.velero.io/backup-volumes=false",
		"--overwrite",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlLabelIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.Label(ctx, "hub", "namespace/ramen-system", "ramen.io/created-by=ramen"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "hub",
		"label", "namespace/ramen-system",
		"ramen.io/created-by=ramen",
		"--overwrite",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlWaitForMultipleTargets(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.WaitFor(ctx, "dr1", "kube-system", "condition=Ready", 60*time.Second, "pod/a", "pod/b"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"--context", "dr1",
		"-n", "kube-system",
		"wait", "pod/a", "pod/b",
		"--for=condition=Ready",
		"--timeout", "60s",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

// TestRunStdinFakeRunner ensures FakeRunner captures stdin and returns the right call index.
func TestRunStdinFakeRunner(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}

	if err := f.RunStdin(ctx, "hello stdin", "kubectl", "apply", "-f", "-"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Stdin != "hello stdin" {
		t.Errorf("Stdin = %q, want %q", c.Stdin, "hello stdin")
	}
	if c.Name != "kubectl" {
		t.Errorf("Name = %q, want kubectl", c.Name)
	}
}

func TestRunStdinFakeRunnerScriptedError(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	boom := errors.New("boom")
	f.Script(cli.FakeResult{Err: boom})

	err := f.RunStdin(ctx, "data", "kubectl", "apply", "-f", "-")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestKubectlApplyServerSideKustomizeDirIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.ApplyServerSideKustomizeDir(ctx, "hub", "/addons/olm/start-data/crds"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{
		"--context", "hub",
		"apply", "--server-side=true", "--kustomize", "/addons/olm/start-data/crds",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}
