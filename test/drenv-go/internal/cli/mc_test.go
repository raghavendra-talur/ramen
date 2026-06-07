// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func TestMCSetAliasIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	m := cli.MC{R: f}

	if err := m.SetAlias(ctx, "minio", "http://192.168.64.10:30000", "minio", "minio123"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "mc" {
		t.Errorf("Name = %q, want %q", c.Name, "mc")
	}
	want := []string{"alias", "set", "minio", "http://192.168.64.10:30000", "minio", "minio123"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMCMakeBucketWithIgnoreExisting(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	m := cli.MC{R: f}

	if err := m.MakeBucket(ctx, "minio/ramen", true); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"mb", "--ignore-existing", "minio/ramen"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMCMakeBucketWithoutIgnoreExisting(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	m := cli.MC{R: f}

	if err := m.MakeBucket(ctx, "minio/ramen", false); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"mb", "minio/ramen"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}
