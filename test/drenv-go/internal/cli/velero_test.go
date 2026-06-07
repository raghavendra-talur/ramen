// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func TestVeleroInstallNoFlags(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	v := cli.Velero{R: f}

	if err := v.Install(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "velero" {
		t.Errorf("Name = %q, want %q", c.Name, "velero")
	}
	want := []string{"install"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestVeleroInstallWithFlags(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	v := cli.Velero{R: f}

	if err := v.Install(ctx,
		"--provider", "aws",
		"--plugins", "velero/velero-plugin-for-aws:v1.8.0",
		"--bucket", "ramen",
		"--backup-location-config", "region=us-east-1",
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"install",
		"--provider", "aws",
		"--plugins", "velero/velero-plugin-for-aws:v1.8.0",
		"--bucket", "ramen",
		"--backup-location-config", "region=us-east-1",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}
