// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestMCStatIssuesCorrectArgv(t *testing.T) {
	f := &cli.FakeRunner{}
	m := cli.MC{R: f}

	if err := m.Stat(context.Background(), "dr1/bucket"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := []string{"stat", "dr1/bucket"}
	if len(f.Calls) != 1 || f.Calls[0].Name != "mc" || !reflect.DeepEqual(f.Calls[0].Args, want) {
		t.Errorf("calls = %+v, want mc %v", f.Calls, want)
	}
}

// overlapRunner records the peak number of concurrent Run calls.
type overlapRunner struct {
	*cli.FakeRunner

	running, peak atomic.Int32
}

func (r *overlapRunner) Run(context.Context, string, ...string) error {
	n := r.running.Add(1)
	defer r.running.Add(-1)

	for {
		p := r.peak.Load()
		if n <= p || r.peak.CompareAndSwap(p, n) {
			break
		}
	}

	time.Sleep(20 * time.Millisecond)

	return nil
}

// TestMCSetAliasIsSerialized verifies that concurrent alias updates never
// overlap: each mc alias set rewrites the whole mc config file, so parallel
// runs lose each other's alias.
func TestMCSetAliasIsSerialized(t *testing.T) {
	r := &overlapRunner{FakeRunner: &cli.FakeRunner{}}

	var wg sync.WaitGroup
	for _, name := range []string{"dr1", "dr2", "dr3"} {
		wg.Add(1)

		go func() {
			defer wg.Done()

			m := cli.MC{R: r}
			if err := m.SetAlias(context.Background(), name, "http://192.168.64.10:30000", "minio", "minio123"); err != nil {
				t.Errorf("SetAlias(%s): %v", name, err)
			}
		}()
	}

	wg.Wait()

	if p := r.peak.Load(); p != 1 {
		t.Errorf("peak concurrent mc alias set = %d, want 1", p)
	}
}
