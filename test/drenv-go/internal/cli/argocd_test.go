// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func TestArgocdLoginIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	a := cli.Argocd{R: f}

	if err := a.Login(ctx, "/tmp/kubeconfig"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "argocd" {
		t.Errorf("Name = %q, want %q", c.Name, "argocd")
	}
	want := []string{"login", "--core"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
	wantEnv := []string{"KUBECONFIG=/tmp/kubeconfig"}
	if !reflect.DeepEqual(c.Env, wantEnv) {
		t.Errorf("env = %v, want %v", c.Env, wantEnv)
	}
}

func TestArgocdClusterAddIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	a := cli.Argocd{R: f}

	if _, err := a.ClusterAdd(ctx, "/tmp/kubeconfig", "dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "argocd" {
		t.Errorf("Name = %q, want %q", c.Name, "argocd")
	}
	want := []string{"cluster", "add", "dr1", "-y"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
	wantEnv := []string{"KUBECONFIG=/tmp/kubeconfig"}
	if !reflect.DeepEqual(c.Env, wantEnv) {
		t.Errorf("env = %v, want %v", c.Env, wantEnv)
	}
}

func TestFakeRunnerRunEnvRecordsEnvAndArgs(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}

	env := []string{"KUBECONFIG=/tmp/kc", "SOME_VAR=value"}
	if err := f.RunEnv(ctx, env, "mytool", "arg1", "arg2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "mytool" {
		t.Errorf("Name = %q, want mytool", c.Name)
	}
	wantArgs := []string{"arg1", "arg2"}
	if !reflect.DeepEqual(c.Args, wantArgs) {
		t.Errorf("Args = %v, want %v", c.Args, wantArgs)
	}
	if !reflect.DeepEqual(c.Env, env) {
		t.Errorf("Env = %v, want %v", c.Env, env)
	}
}

func TestKubectlConfigIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "apiVersion: v1\n"})
	k := cli.Kubectl{R: f}

	out, err := k.Config(ctx, "view", "--flatten", "--output=yaml")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out == "" {
		t.Error("expected non-empty output")
	}
	c := f.Calls[0]
	want := []string{"config", "view", "--flatten", "--output=yaml"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}
