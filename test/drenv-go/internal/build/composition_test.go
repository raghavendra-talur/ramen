// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package build_test

// Tests for the Milestone 4 worker/addon composition wired into build.Start.
// These tests verify the SHAPE of the step tree (which groups are parallel vs
// serial, what names appear, and that addon builders are invoked) without
// running any real cluster commands.

import (
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// TestRegionalDRAddonsAllRegistered guards against the class of bug where an
// addon's registry key does not match the name used in an environment file,
// which would silently turn it into a no-op. It loads the real regional-dr
// environment and asserts every worker addon resolves via addon.Lookup.
func TestRegionalDRAddonsAllRegistered(t *testing.T) {
	env, err := envfile.Load("../../../envs/regional-dr.yaml")
	if err != nil {
		t.Fatalf("load regional-dr.yaml: %v", err)
	}

	check := func(workers []envfile.Worker) {
		for _, w := range workers {
			for _, a := range w.Addons {
				if _, ok := addon.Lookup(a.Name); !ok {
					t.Errorf("addon %q used by regional-dr.yaml is not registered (would silently no-op)", a.Name)
				}
			}
		}
	}
	for _, p := range env.Profiles {
		check(p.Workers)
	}
	check(env.Workers)
}

// registrationName returns a unique addon name for test-scoped registrations to
// avoid colliding with other tests (the global registry is process-wide).
func registrationName(t *testing.T, suffix string) string {
	t.Helper()
	return "test-composition-" + t.Name() + "-" + suffix
}

// asGroup asserts s is an *ensure.Group and returns it.
func asGroup(t *testing.T, s ensure.Step, ctx string) *ensure.Group {
	t.Helper()
	g, ok := s.(*ensure.Group)
	if !ok {
		t.Fatalf("%s: expected *ensure.Group, got %T", ctx, s)
	}
	return g
}

// TestStartTreeShape verifies the overall structure of the step tree returned
// by build.Start:
//
//	env (serial)
//	  profiles (parallel)
//	    profile/dr1 (serial)
//	      cluster/dr1
//	      workers (parallel)
//	        worker/0 (serial)
//	          addon/<name>
//	  workers (parallel)  [global workers]
//	    global-worker/0 (serial)
//	      addon/<name>
func TestStartTreeShape(t *testing.T) {
	addonName := registrationName(t, "simple")
	builderCalled := false

	addon.Register(addonName, func(d addon.Deps, cluster string, args []string) ensure.Step {
		builderCalled = true
		return ensure.NewGroup("addon/"+addonName, ensure.Serial, d.Opts)
	})

	globalAddonName := registrationName(t, "global")
	globalBuilderCalled := false

	addon.Register(globalAddonName, func(d addon.Deps, cluster string, args []string) ensure.Step {
		globalBuilderCalled = true
		// Global addons pass cluster="" and read targets from args.
		if cluster != "" {
			t.Errorf("global addon: expected empty cluster, got %q", cluster)
		}
		return ensure.NewGroup("addon/"+globalAddonName, ensure.Serial, d.Opts)
	})

	env := &envfile.Env{
		Name: "test-env",
		Profiles: []envfile.Profile{
			{
				Name: "dr1",
				Workers: []envfile.Worker{
					{Addons: []envfile.Addon{{Name: addonName}}},
				},
			},
		},
		Workers: []envfile.Worker{
			{Addons: []envfile.Addon{{Name: globalAddonName, Args: []string{"dr1", "dr2"}}}},
		},
	}

	fp := newFakeProvider()
	fp.setStatus("dr1", provider.StatusRunning)
	opts := smallOpts()

	step := build.Start(env, uniformSelector(fp), addon.Deps{Opts: opts}, opts)

	// Top-level group: env name, serial.
	top := asGroup(t, step, "top")
	if top.Name() != "test-env" {
		t.Errorf("top name = %q, want %q", top.Name(), "test-env")
	}
	if top.GroupMode() != ensure.Serial {
		t.Error("top group should be Serial")
	}

	// top children: [profiles, workers]
	topChildren := top.Steps()
	if len(topChildren) != 2 {
		t.Fatalf("top children count = %d, want 2 (profiles + global workers)", len(topChildren))
	}

	// First child: profiles (parallel).
	profilesGroup := asGroup(t, topChildren[0], "profiles")
	if profilesGroup.Name() != "profiles" {
		t.Errorf("profiles group name = %q, want %q", profilesGroup.Name(), "profiles")
	}
	if profilesGroup.GroupMode() != ensure.Parallel {
		t.Error("profiles group should be Parallel")
	}

	// profiles has one entry for dr1.
	profileChildren := profilesGroup.Steps()
	if len(profileChildren) != 1 {
		t.Fatalf("profiles children count = %d, want 1", len(profileChildren))
	}

	// profile/dr1 is serial.
	dr1Group := asGroup(t, profileChildren[0], "profile/dr1")
	if dr1Group.Name() != "profile/dr1" {
		t.Errorf("profile group name = %q, want profile/dr1", dr1Group.Name())
	}
	if dr1Group.GroupMode() != ensure.Serial {
		t.Error("profile group should be Serial")
	}

	// dr1 children: [cluster/dr1, workers]
	dr1Children := dr1Group.Steps()
	if len(dr1Children) != 2 {
		t.Fatalf("dr1 children count = %d, want 2 (cluster + workers)", len(dr1Children))
	}
	if dr1Children[0].Name() != "cluster/dr1" {
		t.Errorf("first dr1 child = %q, want cluster/dr1", dr1Children[0].Name())
	}

	// workers group for dr1 is parallel.
	workersGroup := asGroup(t, dr1Children[1], "dr1 workers")
	if workersGroup.Name() != "workers" {
		t.Errorf("workers group name = %q, want workers", workersGroup.Name())
	}
	if workersGroup.GroupMode() != ensure.Parallel {
		t.Error("workers group should be Parallel")
	}

	// worker/0 is serial.
	w0 := asGroup(t, workersGroup.Steps()[0], "worker/0")
	if w0.Name() != "worker/0" {
		t.Errorf("worker/0 name = %q, want worker/0", w0.Name())
	}
	if w0.GroupMode() != ensure.Serial {
		t.Error("worker/0 should be Serial")
	}

	// The addon step inside worker/0.
	addonStep := w0.Steps()[0]
	wantAddonName := "addon/" + addonName
	if addonStep.Name() != wantAddonName {
		t.Errorf("addon step name = %q, want %q", addonStep.Name(), wantAddonName)
	}

	// Second top child: global workers (parallel).
	globalWorkersGroup := asGroup(t, topChildren[1], "global workers")
	if globalWorkersGroup.Name() != "workers" {
		t.Errorf("global workers group name = %q, want workers", globalWorkersGroup.Name())
	}
	if globalWorkersGroup.GroupMode() != ensure.Parallel {
		t.Error("global workers group should be Parallel")
	}

	// Verify builders were actually called during composition.
	if !builderCalled {
		t.Error("profile addon builder was not called during composition")
	}
	if !globalBuilderCalled {
		t.Error("global addon builder was not called during composition")
	}
}

// TestStartUnregisteredAddonBecomesNoop verifies that an addon not in the
// registry produces a named no-op step (Done=true) rather than panicking.
func TestStartUnregisteredAddonBecomesNoop(t *testing.T) {
	env := &envfile.Env{
		Name: "test-noop-env",
		Profiles: []envfile.Profile{
			{
				Name: "dr1",
				Workers: []envfile.Worker{
					{Addons: []envfile.Addon{{Name: "this-addon-is-not-registered-ever"}}},
				},
			},
		},
	}

	fp := newFakeProvider()
	fp.setStatus("dr1", provider.StatusRunning)
	opts := smallOpts()

	// Should not panic.
	step := build.Start(env, uniformSelector(fp), addon.Deps{Opts: opts}, opts)

	// Dig down to the addon step.
	top := asGroup(t, step, "top")
	profilesGroup := asGroup(t, top.Steps()[0], "profiles")
	dr1Group := asGroup(t, profilesGroup.Steps()[0], "profile/dr1")
	workersGroup := asGroup(t, dr1Group.Steps()[1], "workers")
	w0 := asGroup(t, workersGroup.Steps()[0], "worker/0")
	addonStep := w0.Steps()[0]

	wantName := "addon/this-addon-is-not-registered-ever (unimplemented)"
	if addonStep.Name() != wantName {
		t.Errorf("noop step name = %q, want %q", addonStep.Name(), wantName)
	}
}

// TestStartProfileWithNoWorkers verifies a profile with no workers produces a
// profile group containing only the cluster step.
func TestStartProfileWithNoWorkers(t *testing.T) {
	env := &envfile.Env{
		Name:     "bare-env",
		Profiles: []envfile.Profile{{Name: "hub"}},
	}

	fp := newFakeProvider()
	fp.setStatus("hub", provider.StatusRunning)
	opts := smallOpts()

	step := build.Start(env, uniformSelector(fp), addon.Deps{Opts: opts}, opts)

	top := asGroup(t, step, "top")
	// No global workers → only "profiles" child.
	if len(top.Steps()) != 1 {
		t.Fatalf("top children = %d, want 1 (only profiles, no global workers)", len(top.Steps()))
	}

	profilesGroup := asGroup(t, top.Steps()[0], "profiles")
	hubGroup := asGroup(t, profilesGroup.Steps()[0], "profile/hub")
	// Only the cluster step, no workers group.
	if len(hubGroup.Steps()) != 1 {
		t.Errorf("hub group children = %d, want 1 (only cluster step)", len(hubGroup.Steps()))
	}
	if hubGroup.Steps()[0].Name() != "cluster/hub" {
		t.Errorf("hub child name = %q, want cluster/hub", hubGroup.Steps()[0].Name())
	}
}
