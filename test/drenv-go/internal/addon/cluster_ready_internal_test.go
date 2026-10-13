// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

// TestWaitClusterReadyRetries verifies the wait keeps probing a booting
// cluster until its API server is ready.
func TestWaitClusterReadyRetries(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Err: errors.New("connection refused")})
	f.Script(cli.FakeResult{Err: errors.New("connection refused")})
	f.Script(cli.FakeResult{Out: "ok"})

	if err := waitClusterReady(context.Background(), healthDeps(f), "dr2", time.Minute); err != nil {
		t.Fatalf("waitClusterReady: %v", err)
	}

	if len(f.Calls) != 3 {
		t.Fatalf("expected 3 probes, got %d:\n%v", len(f.Calls), f.Calls)
	}

	for i, c := range f.Calls {
		want := []string{"--context", "dr2", "get", "--raw", "/readyz"}
		if c.Name != "kubectl" || !slices.Equal(c.Args, want) {
			t.Errorf("call[%d] = %s %v, want kubectl %v", i, c.Name, c.Args, want)
		}
	}
}

// TestWaitClusterReadyTimesOut verifies the wait gives up after the timeout
// and names the cluster.
func TestWaitClusterReadyTimesOut(t *testing.T) {
	f := &cli.FakeRunner{}
	for range 1000 {
		f.Script(cli.FakeResult{Err: errors.New("connection refused")})
	}

	err := waitClusterReady(context.Background(), healthDeps(f), "dr2", 20*time.Millisecond)
	if err == nil {
		t.Fatal("waitClusterReady: want a timeout error, got nil")
	}

	if want := "cluster dr2 is not ready"; !strings.Contains(err.Error(), want) {
		t.Errorf("error %q does not contain %q", err, want)
	}
}
