// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func TestSubctlDeployBrokerMinimal(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	s := cli.Subctl{R: f}

	if err := s.DeployBroker(ctx, "hub", false, ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "subctl" {
		t.Errorf("Name = %q, want %q", c.Name, "subctl")
	}
	want := []string{"deploy-broker", "--context", "hub"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestSubctlDeployBrokerWithGlobalnetAndVersion(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	s := cli.Subctl{R: f}

	if err := s.DeployBroker(ctx, "hub", true, "0.15.0"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"deploy-broker", "--context", "hub", "--globalnet", "--version", "0.15.0"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestSubctlJoinMinimal(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	s := cli.Subctl{R: f}

	if err := s.Join(ctx, "broker-info.subm", "dr1", "dr1", "", ""); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"join", "broker-info.subm",
		"--context", "dr1",
		"--clusterid", "dr1",
		"--check-broker-certificate=false",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestSubctlJoinWithCableDriverAndVersion(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	s := cli.Subctl{R: f}

	if err := s.Join(ctx, "broker-info.subm", "dr1", "dr1", "libreswan", "0.15.0"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"join", "broker-info.subm",
		"--context", "dr1",
		"--clusterid", "dr1",
		"--cable-driver", "libreswan",
		"--version", "0.15.0",
		"--check-broker-certificate=false",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}
