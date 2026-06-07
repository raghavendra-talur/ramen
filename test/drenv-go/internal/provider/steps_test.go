// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// fakeProvider is an in-memory implementation of provider.Provider for tests.
// It keeps a map of profile name → Status and records Start/Delete/Stop call
// counts, making it safe for concurrent use so parallel ensure.Group tests can
// share it.
type fakeProvider struct {
	mu       sync.Mutex
	statuses map[string]provider.Status
	starts   map[string]int
	deletes  map[string]int
	stops    map[string]int
}

func newFakeProvider() *fakeProvider {
	return &fakeProvider{
		statuses: make(map[string]provider.Status),
		starts:   make(map[string]int),
		deletes:  make(map[string]int),
		stops:    make(map[string]int),
	}
}

func (f *fakeProvider) setStatus(profile string, s provider.Status) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[profile] = s
}

func (f *fakeProvider) startCount(profile string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.starts[profile]
}

func (f *fakeProvider) deleteCount(profile string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.deletes[profile]
}

func (f *fakeProvider) Status(_ context.Context, profile string) (provider.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.statuses[profile]
	if !ok {
		return provider.StatusNotFound, nil
	}
	return s, nil
}

func (f *fakeProvider) Exists(ctx context.Context, profile string) (bool, error) {
	s, err := f.Status(ctx, profile)
	if err != nil {
		return false, err
	}
	return s != provider.StatusNotFound, nil
}

// Start transitions the profile to StatusRunning.
func (f *fakeProvider) Start(_ context.Context, p envfile.Profile) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts[p.Name]++
	f.statuses[p.Name] = provider.StatusRunning
	return nil
}

// Stop transitions the profile to StatusStopped and records the call.
func (f *fakeProvider) Stop(_ context.Context, profile string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops[profile]++
	f.statuses[profile] = provider.StatusStopped
	return nil
}

// Delete removes the profile (sets to StatusNotFound).
func (f *fakeProvider) Delete(_ context.Context, profile string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deletes[profile]++
	delete(f.statuses, profile)
	return nil
}

func (f *fakeProvider) LoadImage(_ context.Context, _, _ string) error { return nil }

// stopCount returns how many times Stop was called for the given profile.
func (f *fakeProvider) stopCount(profile string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.stops[profile]
}

func (f *fakeProvider) Suspend(_ context.Context, profile string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[profile] = provider.StatusStopped
	return nil
}

func (f *fakeProvider) Resume(_ context.Context, profile string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statuses[profile] = provider.StatusRunning
	return nil
}

// smallOpts returns ensure.Options with very short timeouts suitable for unit tests.
func smallOpts() ensure.Options {
	return ensure.Options{
		VerifyTimeout:  2 * time.Second,
		VerifyInterval: 10 * time.Millisecond,
	}
}

// ---- ClusterRunningStep tests ----

func TestClusterRunningStepName(t *testing.T) {
	fp := newFakeProvider()
	prof := envfile.Profile{Name: "dr1"}
	s := provider.ClusterRunningStep(fp, prof)
	if want := "cluster/dr1"; s.Name() != want {
		t.Errorf("Name() = %q, want %q", s.Name(), want)
	}
}

func TestClusterRunningStepStartsNotRunningCluster(t *testing.T) {
	fp := newFakeProvider()
	fp.setStatus("dr1", provider.StatusStopped)

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterRunningStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Changed {
		t.Errorf("result = %s, want Changed", res)
	}
	if sc := fp.startCount("dr1"); sc != 1 {
		t.Errorf("Start called %d times, want 1", sc)
	}
	// Verify the cluster is now running.
	s, _ := fp.Status(context.Background(), "dr1")
	if s != provider.StatusRunning {
		t.Errorf("post-ensure status = %s, want running", s)
	}
}

func TestClusterRunningStepSkipsAlreadyRunning(t *testing.T) {
	fp := newFakeProvider()
	fp.setStatus("dr1", provider.StatusRunning)

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterRunningStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Skipped {
		t.Errorf("result = %s, want Skipped", res)
	}
	if sc := fp.startCount("dr1"); sc != 0 {
		t.Errorf("Start called %d times on already-running cluster, want 0", sc)
	}
}

func TestClusterRunningStepStartsNotFoundCluster(t *testing.T) {
	fp := newFakeProvider()
	// No status set → defaults to StatusNotFound.

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterRunningStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Changed {
		t.Errorf("result = %s, want Changed", res)
	}
	if sc := fp.startCount("dr1"); sc != 1 {
		t.Errorf("Start called %d times, want 1", sc)
	}
}

// ---- ClusterAbsentStep tests ----

func TestClusterAbsentStepName(t *testing.T) {
	fp := newFakeProvider()
	prof := envfile.Profile{Name: "dr1"}
	s := provider.ClusterAbsentStep(fp, prof)
	if want := "cluster/dr1 absent"; s.Name() != want {
		t.Errorf("Name() = %q, want %q", s.Name(), want)
	}
}

func TestClusterAbsentStepDeletesPresentCluster(t *testing.T) {
	fp := newFakeProvider()
	fp.setStatus("dr1", provider.StatusRunning)

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterAbsentStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Changed {
		t.Errorf("result = %s, want Changed", res)
	}
	if dc := fp.deleteCount("dr1"); dc != 1 {
		t.Errorf("Delete called %d times, want 1", dc)
	}
	// Verify the cluster is now absent.
	s, _ := fp.Status(context.Background(), "dr1")
	if s != provider.StatusNotFound {
		t.Errorf("post-ensure status = %s, want not-found", s)
	}
}

func TestClusterAbsentStepSkipsAbsentCluster(t *testing.T) {
	fp := newFakeProvider()
	// No status set → defaults to StatusNotFound.

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterAbsentStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Skipped {
		t.Errorf("result = %s, want Skipped", res)
	}
	if dc := fp.deleteCount("dr1"); dc != 0 {
		t.Errorf("Delete called %d times on absent cluster, want 0", dc)
	}
}

func TestClusterAbsentStepDeletesStoppedCluster(t *testing.T) {
	fp := newFakeProvider()
	fp.setStatus("dr1", provider.StatusStopped)

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterAbsentStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Changed {
		t.Errorf("result = %s, want Changed", res)
	}
	if dc := fp.deleteCount("dr1"); dc != 1 {
		t.Errorf("Delete called %d times, want 1", dc)
	}
}

// ---- ClusterStoppedStep tests ----

func TestClusterStoppedStepName(t *testing.T) {
	fp := newFakeProvider()
	prof := envfile.Profile{Name: "dr1"}
	s := provider.ClusterStoppedStep(fp, prof)
	if want := "cluster/dr1 stopped"; s.Name() != want {
		t.Errorf("Name() = %q, want %q", s.Name(), want)
	}
}

func TestClusterStoppedStepStopsRunningCluster(t *testing.T) {
	fp := newFakeProvider()
	fp.setStatus("dr1", provider.StatusRunning)

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterStoppedStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Changed {
		t.Errorf("result = %s, want Changed", res)
	}
	if sc := fp.stopCount("dr1"); sc != 1 {
		t.Errorf("Stop called %d times, want 1", sc)
	}
	// Verify the cluster is now stopped.
	s, _ := fp.Status(context.Background(), "dr1")
	if s != provider.StatusStopped {
		t.Errorf("post-ensure status = %s, want stopped", s)
	}
}

func TestClusterStoppedStepSkipsAlreadyStoppedCluster(t *testing.T) {
	fp := newFakeProvider()
	fp.setStatus("dr1", provider.StatusStopped)

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterStoppedStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Skipped {
		t.Errorf("result = %s, want Skipped", res)
	}
	if sc := fp.stopCount("dr1"); sc != 0 {
		t.Errorf("Stop called %d times on already-stopped cluster, want 0", sc)
	}
}

func TestClusterStoppedStepSkipsAbsentCluster(t *testing.T) {
	fp := newFakeProvider()
	// No status set → defaults to StatusNotFound.

	prof := envfile.Profile{Name: "dr1"}
	step := provider.ClusterStoppedStep(fp, prof)

	res, err := ensure.Ensure(context.Background(), step, smallOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != ensure.Skipped {
		t.Errorf("result = %s, want Skipped", res)
	}
	if sc := fp.stopCount("dr1"); sc != 0 {
		t.Errorf("Stop called %d times on absent cluster, want 0", sc)
	}
}
