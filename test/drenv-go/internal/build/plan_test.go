// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package build_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

// registerNoop registers a test-scoped addon whose step is an empty group named
// addon/<name>, and returns the name.
func registerNoop(t *testing.T, suffix string) string {
	t.Helper()
	name := registrationName(t, suffix)
	addon.Register(name, func(d addon.Deps, _ string, _ []string) ensure.Step {
		return ensure.NewGroup("addon/"+name, ensure.Serial, d.Opts)
	})
	return name
}

// planEnv returns a two-profile env with two workers on dr1, one on hub, and
// two global workers, using the given addon names.
func planEnv(a, b, g string) *envfile.Env {
	return &envfile.Env{
		Name: "plan-env",
		Profiles: []envfile.Profile{
			{Name: "dr1", Workers: []envfile.Worker{
				{Addons: []envfile.Addon{{Name: a}, {Name: b, Args: []string{"dr1", "hub"}}}},
				{Addons: []envfile.Addon{{Name: "not-registered-plan-addon"}}},
			}},
			{Name: "hub", Workers: []envfile.Worker{
				{Addons: []envfile.Addon{{Name: a}}},
			}},
		},
		Workers: []envfile.Worker{
			{Addons: []envfile.Addon{{Name: g, Args: []string{"dr1"}}}},
			{Addons: []envfile.Addon{{Name: g, Args: []string{"hub"}}}},
		},
	}
}

// addonKey is the location/identity part of a PlannedAddon (no Step).
type addonKey struct {
	Profile    string
	Global     bool
	Worker     int
	Name       string
	Args       []string
	Registered bool
}

func keys(as []build.PlannedAddon) []addonKey {
	out := make([]addonKey, len(as))
	for i, a := range as {
		out[i] = addonKey{a.Profile, a.Global, a.Worker, a.Name, a.Args, a.Registered}
	}
	return out
}

func TestPlanAddonsLocations(t *testing.T) {
	a, b, g := registerNoop(t, "a"), registerNoop(t, "b"), registerNoop(t, "g")
	fp := newFakeProvider()
	opts := smallOpts()

	p := build.NewPlan(planEnv(a, b, g), uniformSelector(fp), addon.Deps{Opts: opts}, opts)

	want := []addonKey{
		{"dr1", false, 0, a, nil, true},
		{"dr1", false, 0, b, []string{"dr1", "hub"}, true},
		{"dr1", false, 1, "not-registered-plan-addon", nil, false},
		{"hub", false, 0, a, nil, true},
		{"", true, 0, g, []string{"dr1"}, true},
		{"", true, 1, g, []string{"hub"}, true},
	}
	if got := keys(p.Addons()); !reflect.DeepEqual(got, want) {
		t.Fatalf("Addons() =\n%+v\nwant\n%+v", got, want)
	}

	for _, pa := range p.Addons() {
		if pa.Step == nil {
			t.Errorf("%s@%s: nil step", pa.Name, pa.Profile)
		}
	}
	if got := p.Profiles[0].Cluster.Name(); got != "cluster/dr1" {
		t.Errorf("dr1 cluster step = %q, want cluster/dr1", got)
	}
	if p.Profiles[0].Containerd != nil {
		t.Error("dr1 has no containerd block; Containerd step should be nil")
	}
}

// TestPlanTreeMatchesStart pins that Start is exactly the plan's tree: same
// group names, nesting and leaf step names.
func TestPlanTreeMatchesStart(t *testing.T) {
	a, b, g := registerNoop(t, "a"), registerNoop(t, "b"), registerNoop(t, "g")
	fp := newFakeProvider()
	opts := smallOpts()
	env := planEnv(a, b, g)

	got := treeNames(build.NewPlan(env, uniformSelector(fp), addon.Deps{Opts: opts}, opts).Tree())
	want := treeNames(build.Start(env, uniformSelector(fp), addon.Deps{Opts: opts}, opts))
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Plan.Tree names = %v, want Start names %v", got, want)
	}

	wantPrefix := []string{
		"plan-env", " profiles", "  profile/dr1", "   cluster/dr1", "   workers",
		"    worker/0", "     addon/" + a, "     addon/" + b,
		"    worker/1", "     addon/not-registered-plan-addon (unimplemented)",
	}
	if !reflect.DeepEqual(got[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("tree prefix = %v, want %v", got[:len(wantPrefix)], wantPrefix)
	}
}

// treeNames flattens a step tree into depth-indented names. Addon steps are
// treated as leaves even when they are groups.
func treeNames(s ensure.Step) []string {
	var out []string
	var walk func(ensure.Step, string)
	walk = func(s ensure.Step, indent string) {
		out = append(out, indent+s.Name())
		g, ok := s.(*ensure.Group)
		if !ok || strings.HasPrefix(s.Name(), "addon/") {
			return
		}
		for _, c := range g.Steps() {
			walk(c, indent+" ")
		}
	}
	walk(s, "")
	return out
}
