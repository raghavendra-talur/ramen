// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func TestKubectlApplyIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.Apply(ctx, "dr1", "-f", "manifest.yaml"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "kubectl" {
		t.Errorf("name = %q, want %q", c.Name, "kubectl")
	}
	want := []string{"--context", "dr1", "apply", "-f", "manifest.yaml"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlApplyNoExtraArgsIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.Apply(ctx, "hub"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"--context", "hub", "apply"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlApplyKustomizationIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	if err := k.ApplyKustomization(ctx, "dr1", "./overlays/dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{"--context", "dr1", "apply", "-k", "./overlays/dr1"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlWaitRolloutIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	timeout := 5 * time.Minute
	if err := k.WaitRollout(ctx, "dr1", "ramen-system", "deployment/ramen-dr-cluster-operator", timeout); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{
		"--context", "dr1",
		"-n", "ramen-system",
		"rollout", "status", "deployment/ramen-dr-cluster-operator",
		"--timeout", "300s",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlWaitRolloutFormatsTimeoutAsSeconds(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	// 90 seconds → "90s", not "1m30s"
	if err := k.WaitRollout(ctx, "dr1", "default", "deployment/foo", 90*time.Second); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	// timeout arg is the last one
	got := c.Args[len(c.Args)-1]
	if got != "90s" {
		t.Errorf("timeout arg = %q, want %q", got, "90s")
	}
}

func TestKubectlWaitConditionIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	k := cli.Kubectl{R: f}

	timeout := 2 * time.Minute
	if err := k.WaitCondition(ctx, "hub", "ramen-system", "pod/ramen-hub-0", "Ready", timeout); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{
		"--context", "hub",
		"-n", "ramen-system",
		"wait", "pod/ramen-hub-0",
		"--for=condition=Ready",
		"--timeout", "120s",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestKubectlGetIssuesCorrectArgvAndReturnsOutput(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "NAME   STATUS\ndr1    Running"})
	k := cli.Kubectl{R: f}

	out, err := k.Get(ctx, "dr1", "ramen-system", "pods", "-o", "wide")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{"--context", "dr1", "-n", "ramen-system", "get", "pods", "-o", "wide"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
	if out == "" {
		t.Error("Get returned empty output, want non-empty")
	}
}
