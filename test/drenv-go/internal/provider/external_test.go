// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// newExternalProvider wires a FakeRunner into an ExternalProvider.
func newExternalProvider(f *cli.FakeRunner) provider.ExternalProvider {
	return provider.ExternalProvider{K: &cli.Kubectl{R: f}}
}

// TestExternalProviderStatusReachable verifies that a successful /readyz probe
// maps to StatusRunning.
func TestExternalProviderStatusReachable(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "ok"})
	p := newExternalProvider(f)

	s, err := p.Status(context.Background(), "ext1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != provider.StatusRunning {
		t.Errorf("Status = %s, want running", s)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	// Verify the correct argv was issued.
	c := f.Calls[0]
	if c.Name != "kubectl" {
		t.Errorf("command = %q, want kubectl", c.Name)
	}
	wantArgs := []string{"--context", "ext1", "get", "--raw", "/readyz"}
	for i, a := range wantArgs {
		if i >= len(c.Args) || c.Args[i] != a {
			t.Errorf("args[%d] = %q, want %q (full args: %v)", i, c.Args[i], a, c.Args)
		}
	}
}

// TestExternalProviderStatusUnreachable verifies that a failed /readyz probe
// maps to StatusNotFound (cluster not usable from here).
func TestExternalProviderStatusUnreachable(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Err: errors.New("connection refused")})
	p := newExternalProvider(f)

	s, err := p.Status(context.Background(), "ext1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != provider.StatusNotFound {
		t.Errorf("Status = %s, want not-found", s)
	}
}

// TestExternalProviderExistsReachable mirrors the minikube Exists tests.
func TestExternalProviderExistsReachable(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "ok"})
	p := newExternalProvider(f)

	ok, err := p.Exists(context.Background(), "ext1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Errorf("Exists = false, want true")
	}
}

// TestExternalProviderExistsUnreachable verifies Exists returns false when the
// cluster API is not reachable.
func TestExternalProviderExistsUnreachable(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Err: errors.New("connection refused")})
	p := newExternalProvider(f)

	ok, err := p.Exists(context.Background(), "ext1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Errorf("Exists = true, want false")
	}
}

// TestExternalProviderLifecycleNoOps verifies that Start, Stop, Delete,
// Suspend, Resume, and LoadImage all issue zero kubectl commands.
func TestExternalProviderLifecycleNoOps(t *testing.T) {
	ctx := context.Background()

	t.Run("Start", func(t *testing.T) {
		f := &cli.FakeRunner{}
		p := newExternalProvider(f)
		if err := p.Start(ctx, envfile.Profile{Name: "ext1"}); err != nil {
			t.Fatalf("Start: %v", err)
		}
		if len(f.Calls) != 0 {
			t.Errorf("expected 0 calls, got %d: %v", len(f.Calls), f.Calls)
		}
	})

	t.Run("Stop", func(t *testing.T) {
		f := &cli.FakeRunner{}
		p := newExternalProvider(f)
		if err := p.Stop(ctx, "ext1"); err != nil {
			t.Fatalf("Stop: %v", err)
		}
		if len(f.Calls) != 0 {
			t.Errorf("expected 0 calls, got %d: %v", len(f.Calls), f.Calls)
		}
	})

	t.Run("Delete", func(t *testing.T) {
		f := &cli.FakeRunner{}
		p := newExternalProvider(f)
		if err := p.Delete(ctx, "ext1"); err != nil {
			t.Fatalf("Delete: %v", err)
		}
		if len(f.Calls) != 0 {
			t.Errorf("expected 0 calls, got %d: %v", len(f.Calls), f.Calls)
		}
	})

	t.Run("LoadImage", func(t *testing.T) {
		f := &cli.FakeRunner{}
		p := newExternalProvider(f)
		if err := p.LoadImage(ctx, "ext1", "my-image:latest"); err != nil {
			t.Fatalf("LoadImage: %v", err)
		}
		if len(f.Calls) != 0 {
			t.Errorf("expected 0 calls, got %d: %v", len(f.Calls), f.Calls)
		}
	})

	t.Run("Suspend", func(t *testing.T) {
		f := &cli.FakeRunner{}
		p := newExternalProvider(f)
		if err := p.Suspend(ctx, "ext1"); err != nil {
			t.Fatalf("Suspend: %v", err)
		}
		if len(f.Calls) != 0 {
			t.Errorf("expected 0 calls, got %d: %v", len(f.Calls), f.Calls)
		}
	})

	t.Run("Resume", func(t *testing.T) {
		f := &cli.FakeRunner{}
		p := newExternalProvider(f)
		if err := p.Resume(ctx, "ext1"); err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if len(f.Calls) != 0 {
			t.Errorf("expected 0 calls, got %d: %v", len(f.Calls), f.Calls)
		}
	})
}
