// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func TestClusteradmInitWithFeatureGatesAndWait(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	c := cli.Clusteradm{R: f}

	if err := c.Init(ctx, "hub", []string{"ManagedClusterAutoApproval=true"}, true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	call := f.Calls[0]
	if call.Name != "clusteradm" {
		t.Errorf("Name = %q, want %q", call.Name, "clusteradm")
	}
	want := []string{"init", "--feature-gates=ManagedClusterAutoApproval=true", "--wait", "--context", "hub"}
	if !reflect.DeepEqual(call.Args, want) {
		t.Errorf("args = %v, want %v", call.Args, want)
	}
}

func TestClusteradmInitNoFeatureGatesNoWait(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	c := cli.Clusteradm{R: f}

	if err := c.Init(ctx, "hub", nil, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	call := f.Calls[0]
	want := []string{"init", "--context", "hub"}
	if !reflect.DeepEqual(call.Args, want) {
		t.Errorf("args = %v, want %v", call.Args, want)
	}
}

func TestClusteradmInitMultipleFeatureGates(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	c := cli.Clusteradm{R: f}

	if err := c.Init(ctx, "hub", []string{"FeatureA=true", "FeatureB=false"}, false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	call := f.Calls[0]
	want := []string{"init", "--feature-gates=FeatureA=true,FeatureB=false", "--context", "hub"}
	if !reflect.DeepEqual(call.Args, want) {
		t.Errorf("args = %v, want %v", call.Args, want)
	}
}

func TestClusteradmGetWithOutput(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: `{"token":"abc123","apiserver":"https://192.168.64.1:8443"}`})
	c := cli.Clusteradm{R: f}

	out, err := c.Get(ctx, "hub", "token", "json")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	call := f.Calls[0]
	want := []string{"get", "token", "--output=json", "--context", "hub"}
	if !reflect.DeepEqual(call.Args, want) {
		t.Errorf("args = %v, want %v", call.Args, want)
	}
	if out == "" {
		t.Error("expected non-empty output")
	}
}

func TestClusteradmGetWithoutOutput(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	c := cli.Clusteradm{R: f}

	_, _ = c.Get(ctx, "hub", "token", "")
	call := f.Calls[0]
	want := []string{"get", "token", "--context", "hub"}
	if !reflect.DeepEqual(call.Args, want) {
		t.Errorf("args = %v, want %v", call.Args, want)
	}
}

func TestClusteradmJoinIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	c := cli.Clusteradm{R: f}

	if err := c.Join(ctx, "dr1", "abc123", "https://192.168.64.1:8443", "dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	call := f.Calls[0]
	want := []string{
		"join",
		"--hub-token=abc123",
		"--hub-apiserver=https://192.168.64.1:8443",
		"--cluster-name=dr1",
		"--context", "dr1",
	}
	if !reflect.DeepEqual(call.Args, want) {
		t.Errorf("args = %v, want %v", call.Args, want)
	}
}

func TestClusteradmAddonEnableIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	c := cli.Clusteradm{R: f}

	if err := c.Addon(ctx, "hub", "enable", []string{"work-manager", "application-manager"}, []string{"dr1", "dr2"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	call := f.Calls[0]
	want := []string{
		"addon", "enable",
		"--names=work-manager,application-manager",
		"--clusters=dr1,dr2",
		"--context", "hub",
	}
	if !reflect.DeepEqual(call.Args, want) {
		t.Errorf("args = %v, want %v", call.Args, want)
	}
}

func TestClusteradmInstallIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	c := cli.Clusteradm{R: f}

	if err := c.Install(ctx, "hub", "hub-addon", []string{"governance-policy-framework", "config-policy-controller"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	call := f.Calls[0]
	want := []string{
		"install", "hub-addon",
		"--names=governance-policy-framework,config-policy-controller",
		"--context", "hub",
	}
	if !reflect.DeepEqual(call.Args, want) {
		t.Errorf("args = %v, want %v", call.Args, want)
	}
}
