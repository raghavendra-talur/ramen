// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package build_test

import (
	"reflect"
	"strings"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/build"
)

func TestParseSelectors(t *testing.T) {
	tests := []struct {
		name    string
		in      []string
		want    []build.Selector
		wantErr bool
	}{
		{"none", nil, nil, false},
		{"bare", []string{"rook-pool"}, []build.Selector{{Name: "rook-pool"}}, false},
		{"profile", []string{"rook-pool@dr1"}, []build.Selector{{Name: "rook-pool", Scope: "dr1"}}, false},
		{"global", []string{"rbd-mirror@global"}, []build.Selector{{Name: "rbd-mirror", Scope: "global"}}, false},
		{
			"repeated and comma separated",
			[]string{"a,b@dr1", " c ,"},
			[]build.Selector{{Name: "a"}, {Name: "b", Scope: "dr1"}, {Name: "c"}},
			false,
		},
		{"missing name", []string{"@dr1"}, nil, true},
		{"missing scope", []string{"a@"}, nil, true},
		{"double at", []string{"a@b@c"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := build.ParseSelectors(tt.in)
			if (err != nil) != tt.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr && !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestPlanOnly(t *testing.T) {
	a, b, g := registerNoop(t, "a"), registerNoop(t, "b"), registerNoop(t, "g")
	fp := newFakeProvider()
	opts := smallOpts()
	full := build.NewPlan(planEnv(a, b, g), uniformSelector(fp), addon.Deps{Opts: opts}, opts)

	tests := []struct {
		name     string
		sels     []build.Selector
		profiles []string
		addons   []addonKey
	}{
		{
			name:     "no selectors keeps everything",
			sels:     nil,
			profiles: []string{"dr1", "hub"},
			addons:   keys(full.Addons()),
		},
		{
			name:     "bare name selects every occurrence",
			sels:     []build.Selector{{Name: a}},
			profiles: []string{"dr1", "hub"},
			addons:   []addonKey{{"dr1", false, 0, a, nil, true}, {"hub", false, 0, a, nil, true}},
		},
		{
			name:     "profile scope keeps only that profile",
			sels:     []build.Selector{{Name: a, Scope: "hub"}},
			profiles: []string{"hub"},
			addons:   []addonKey{{"hub", false, 0, a, nil, true}},
		},
		{
			name:     "keeps original worker index",
			sels:     []build.Selector{{Name: "not-registered-plan-addon", Scope: "dr1"}},
			profiles: []string{"dr1"},
			addons:   []addonKey{{"dr1", false, 1, "not-registered-plan-addon", nil, false}},
		},
		{
			name:     "global keeps profiles named in args",
			sels:     []build.Selector{{Name: g, Scope: "global"}},
			profiles: []string{"dr1", "hub"},
			addons:   []addonKey{{"", true, 0, g, []string{"dr1"}, true}, {"", true, 1, g, []string{"hub"}, true}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, err := full.Only(tt.sels)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			var profiles []string
			for _, pp := range p.Profiles {
				profiles = append(profiles, pp.Profile.Name)
				if pp.Cluster == nil {
					t.Errorf("%s: kept profile lost its cluster step", pp.Profile.Name)
				}
			}
			if !reflect.DeepEqual(profiles, tt.profiles) {
				t.Errorf("profiles = %v, want %v", profiles, tt.profiles)
			}
			if got := keys(p.Addons()); !reflect.DeepEqual(got, tt.addons) {
				t.Errorf("addons =\n%+v\nwant\n%+v", got, tt.addons)
			}
		})
	}
}

// TestPlanOnlyTreeDropsEmptyWorkers checks the filtered start tree: a profile
// kept only for a global addon has just its cluster step, and worker groups
// keep their original index.
func TestPlanOnlyTreeDropsEmptyWorkers(t *testing.T) {
	a, b, g := registerNoop(t, "a"), registerNoop(t, "b"), registerNoop(t, "g")
	fp := newFakeProvider()
	opts := smallOpts()
	full := build.NewPlan(planEnv(a, b, g), uniformSelector(fp), addon.Deps{Opts: opts}, opts)

	p, err := full.Only([]build.Selector{
		{Name: "not-registered-plan-addon"},
		{Name: g, Scope: "global"},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{
		"plan-env", " profiles",
		"  profile/dr1", "   cluster/dr1", "   workers",
		"    worker/1", "     addon/not-registered-plan-addon (unimplemented)",
		"  profile/hub", "   cluster/hub",
		" workers",
		"  global-worker/0", "   addon/" + g,
		"  global-worker/1", "   addon/" + g,
	}
	if got := treeNames(p.Tree()); !reflect.DeepEqual(got, want) {
		t.Fatalf("tree =\n%s\nwant\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

func TestPlanOnlyUnmatchedSelectorErrors(t *testing.T) {
	a, b, g := registerNoop(t, "a"), registerNoop(t, "b"), registerNoop(t, "g")
	fp := newFakeProvider()
	opts := smallOpts()
	full := build.NewPlan(planEnv(a, b, g), uniformSelector(fp), addon.Deps{Opts: opts}, opts)

	tests := []build.Selector{
		{Name: "nope"},
		{Name: a, Scope: "dr2"},    // no such profile
		{Name: a, Scope: "global"}, // a is not global
		{Name: g, Scope: "dr1"},    // g is only global
	}
	for _, sel := range tests {
		t.Run(sel.String(), func(t *testing.T) {
			_, err := full.Only([]build.Selector{{Name: b}, sel})
			if err == nil {
				t.Fatal("expected error for unmatched selector")
			}
			msg := err.Error()
			if !strings.Contains(msg, sel.String()) || !strings.Contains(msg, a) || !strings.Contains(msg, g) {
				t.Errorf("error %q should name the selector and list valid addons", msg)
			}
		})
	}
}
