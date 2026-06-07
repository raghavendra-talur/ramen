// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package build_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// fakeProvider is an in-memory, mutex-protected provider.Provider used only in
// this test file. The parallel "clusters" group inside build.Start may call
// Status/Start/Delete concurrently, so all state accesses are guarded by mu.
type fakeProvider struct {
	mu       sync.Mutex
	statuses map[string]provider.Status
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{statuses: make(map[string]provider.Status)}
}

func (f *fakeProvider) setStatus(profile string, s provider.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[profile] = s
}

func (f *fakeProvider) status(profile string) provider.Status {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.statuses[profile]
	if !ok {
		return provider.StatusNotFound
	}
	return s
}

func (f *fakeProvider) Status(_ context.Context, profile string) (provider.Status, error) {
	return f.status(profile), nil
}

func (f *fakeProvider) Exists(ctx context.Context, profile string) (bool, error) {
	s, err := f.Status(ctx, profile)
	if err != nil {
		return false, err
	}
	return s != provider.StatusNotFound, nil
}

func (f *fakeProvider) Start(_ context.Context, p envfile.Profile) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[p.Name] = provider.StatusRunning
	return nil
}

func (f *fakeProvider) Stop(_ context.Context, profile string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[profile] = provider.StatusStopped
	return nil
}

func (f *fakeProvider) Delete(_ context.Context, profile string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.statuses, profile)
	return nil
}

func (f *fakeProvider) LoadImage(_ context.Context, _, _ string) error { return nil }

// testEnv returns a small inline Env with three profiles for testing.
func testEnv() *envfile.Env {
	return &envfile.Env{
		Name: "test-env",
		Profiles: []envfile.Profile{
			{Name: "dr1"},
			{Name: "dr2"},
			{Name: "hub"},
		},
	}
}

// smallOpts returns Options with very short timeouts suitable for unit tests.
func smallOpts() ensure.Options {
	return ensure.Options{
		VerifyTimeout:  2 * time.Second,
		VerifyInterval: 10 * time.Millisecond,
	}
}

func TestStartMakesClustersRunning(t *testing.T) {
	fp := newFakeProvider()
	env := testEnv()
	opts := smallOpts()

	step := build.Start(env, fp, opts)
	_, err := ensure.Ensure(context.Background(), step, opts)
	if err != nil {
		t.Fatalf("Ensure(Start) unexpected error: %v", err)
	}

	for _, prof := range env.Profiles {
		if got := fp.status(prof.Name); got != provider.StatusRunning {
			t.Errorf("after Start: profile %q status = %s, want running", prof.Name, got)
		}
	}
}

func TestStartSkipsAlreadyRunningClusters(t *testing.T) {
	fp := newFakeProvider()
	env := testEnv()
	opts := smallOpts()

	// Pre-seed all clusters as already running.
	for _, prof := range env.Profiles {
		fp.setStatus(prof.Name, provider.StatusRunning)
	}

	step := build.Start(env, fp, opts)
	res, err := ensure.Ensure(context.Background(), step, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The top-level group is Done immediately (all children already running),
	// so the result should be Skipped.
	if res != ensure.Skipped {
		t.Errorf("result = %s, want Skipped", res)
	}
}

func TestDeleteMakesClustersAbsent(t *testing.T) {
	fp := newFakeProvider()
	env := testEnv()
	opts := smallOpts()

	// Pre-seed all clusters as running so Delete has work to do.
	for _, prof := range env.Profiles {
		fp.setStatus(prof.Name, provider.StatusRunning)
	}

	step := build.Delete(env, fp, opts)
	_, err := ensure.Ensure(context.Background(), step, opts)
	if err != nil {
		t.Fatalf("Ensure(Delete) unexpected error: %v", err)
	}

	for _, prof := range env.Profiles {
		if got := fp.status(prof.Name); got != provider.StatusNotFound {
			t.Errorf("after Delete: profile %q status = %s, want not-found", prof.Name, got)
		}
	}
}

func TestDeleteSkipsAbsentClusters(t *testing.T) {
	fp := newFakeProvider()
	env := testEnv()
	opts := smallOpts()

	// No statuses set → all NotFound already.
	step := build.Delete(env, fp, opts)
	res, err := ensure.Ensure(context.Background(), step, opts)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Skipped {
		t.Errorf("result = %s, want Skipped", res)
	}
}
