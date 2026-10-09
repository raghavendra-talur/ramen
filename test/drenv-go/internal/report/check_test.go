// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package report_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
	"github.com/ramendr/ramen/test/drenv-go/internal/report"
)

// probeStep is an addon step with a scripted Done outcome. It counts Done and
// Do calls; check must never call Do.
type probeStep struct {
	name      string
	ok        bool
	err       error
	hang      bool // block forever, ignoring ctx
	doneCalls atomic.Int32
	doCalls   atomic.Int32
}

func (s *probeStep) Name() string { return s.name }

func (s *probeStep) Done(context.Context) (bool, error) {
	s.doneCalls.Add(1)
	if s.hang {
		select {}
	}
	return s.ok, s.err
}

func (s *probeStep) Do(context.Context) error {
	s.doCalls.Add(1)
	return nil
}

// register registers step under a test-unique addon name and returns the name.
func register(t *testing.T, suffix string, step *probeStep) string {
	t.Helper()
	name := "test-check-" + t.Name() + "-" + suffix
	step.name = "addon/" + name
	addon.Register(name, func(addon.Deps, string, []string) ensure.Step { return step })
	return name
}

func fastCheck() report.CheckOptions {
	return report.CheckOptions{Timeout: 200 * time.Millisecond, Parallel: 2}
}

func newPlan(env *envfile.Env, fp *fakeProvider, d addon.Deps) *build.Plan {
	return build.NewPlan(env, fp.selector(), d, ensure.Options{})
}

// summary maps "<scope>/<name>" to "state" or "state: error".
func summary(c report.Check) map[string]string {
	out := map[string]string{}
	for _, s := range c.Steps {
		scope := s.Profile
		if s.Global {
			scope = "global"
		}
		v := s.State
		if s.Error != "" {
			v += ": " + s.Error
		}
		out[scope+"/"+s.Name] = v
	}
	return out
}

func TestBuildCheckStates(t *testing.T) {
	ready := &probeStep{ok: true}
	notReady := &probeStep{}
	failing := &probeStep{err: errors.New("probe failed")}
	dead := &probeStep{ok: true}
	global := &probeStep{ok: true}
	globalDead := &probeStep{ok: true}

	r, n, f := register(t, "ready", ready), register(t, "notready", notReady), register(t, "failing", failing)
	d, g, gd := register(t, "dead", dead), register(t, "global", global), register(t, "globaldead", globalDead)

	env := &envfile.Env{
		Name: "rdr",
		Profiles: []envfile.Profile{
			{Name: "dr1", Workers: []envfile.Worker{
				{Addons: []envfile.Addon{{Name: r}, {Name: n}}},
				{Addons: []envfile.Addon{{Name: f}, {Name: "not-registered-check-addon"}}},
			}},
			{Name: "dr2", Workers: []envfile.Worker{{Addons: []envfile.Addon{{Name: d}}}}},
			{Name: "hub", Workers: []envfile.Worker{{Addons: []envfile.Addon{{Name: d}}}}},
		},
		Workers: []envfile.Worker{
			{Addons: []envfile.Addon{{Name: g, Args: []string{"dr1", "hub-not-a-profile"}}}},
			{Addons: []envfile.Addon{{Name: gd, Args: []string{"dr1", "dr2"}}}},
		},
	}
	fp := newFakeProvider()
	fp.statuses["dr1"] = provider.StatusRunning
	fp.statuses["dr2"] = provider.StatusStopped
	fp.errs["hub"] = errors.New("minikube exploded")

	c := report.BuildCheck(context.Background(), newPlan(env, fp, addon.Deps{}), fastCheck())

	want := map[string]string{
		"dr1/cluster/dr1":                      "ready",
		"dr1/addon/" + r:                       "ready",
		"dr1/addon/" + n:                       "not-ready",
		"dr1/addon/" + f:                       "error: probe failed",
		"dr1/addon/not-registered-check-addon": "unimplemented",
		"dr2/cluster/dr2":                      "not-ready",
		"dr2/addon/" + d:                       "skipped: cluster not running",
		"hub/cluster/hub":                      "error: minikube exploded",
		"hub/addon/" + d:                       "skipped: cluster status unknown",
		"global/addon/" + g:                    "ready",
		"global/addon/" + gd:                   "skipped: cluster dr2 not running",
	}
	got := summary(c)
	if len(got) != len(want) || len(c.Steps) != len(want) {
		t.Errorf("got %d steps (%d unique), want %d: %v", len(c.Steps), len(got), len(want), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %q, want %q", k, got[k], v)
		}
	}

	if dead.doneCalls.Load() != 0 || globalDead.doneCalls.Load() != 0 {
		t.Error("addons on a non-running cluster must not be probed")
	}
	for _, s := range []*probeStep{ready, notReady, failing, dead, global, globalDead} {
		if s.doCalls.Load() != 0 {
			t.Errorf("%s: Do called; check must be read-only", s.name)
		}
	}

	// Plan order: each profile's cluster then its addons, then global addons.
	var order []string
	for _, s := range c.Steps {
		order = append(order, s.Name)
	}
	wantOrder := []string{
		"cluster/dr1", "addon/" + r, "addon/" + n, "addon/" + f, "addon/not-registered-check-addon",
		"cluster/dr2", "addon/" + d, "cluster/hub", "addon/" + d, "addon/" + g, "addon/" + gd,
	}
	if strings.Join(order, " ") != strings.Join(wantOrder, " ") {
		t.Errorf("order = %v, want %v", order, wantOrder)
	}
}

func TestBuildCheckTimesOutHungProbe(t *testing.T) {
	hung := &probeStep{hang: true}
	name := register(t, "hung", hung)
	env := &envfile.Env{Name: "e", Profiles: []envfile.Profile{
		{Name: "dr1", Workers: []envfile.Worker{{Addons: []envfile.Addon{{Name: name}}}}},
	}}
	fp := newFakeProvider()
	fp.statuses["dr1"] = provider.StatusRunning

	start := time.Now()
	c := report.BuildCheck(context.Background(), newPlan(env, fp, addon.Deps{}), fastCheck())
	if time.Since(start) > 2*time.Second {
		t.Fatalf("check blocked on a hung probe for %s", time.Since(start))
	}
	if got := summary(c)["dr1/addon/"+name]; !strings.HasPrefix(got, "error: timed out") {
		t.Fatalf("hung probe = %q, want error: timed out", got)
	}
}

// TestBuildCheckContainerdNotApplied: a running cluster whose containerd config
// cannot be confirmed is not ready, yet its addons are still probed.
func TestBuildCheckContainerdNotApplied(t *testing.T) {
	ready := &probeStep{ok: true}
	name := register(t, "ready", ready)
	env := &envfile.Env{Name: "e", Profiles: []envfile.Profile{{
		Name:         "dr1",
		MinikubeSpec: envfile.MinikubeSpec{Containerd: map[string]any{"plugins": map[string]any{"x": true}}},
		Workers:      []envfile.Worker{{Addons: []envfile.Addon{{Name: name}}}},
	}}}
	fp := newFakeProvider()
	fp.statuses["dr1"] = provider.StatusRunning
	fr := &cli.FakeRunner{}
	fr.Script(cli.FakeResult{Err: errors.New("cp failed")})

	c := report.BuildCheck(context.Background(), newPlan(env, fp, addon.Deps{MK: &cli.Minikube{R: fr}}), fastCheck())

	got := summary(c)
	if v := got["dr1/cluster/dr1"]; !strings.HasPrefix(v, "error: containerd-config/dr1: ") {
		t.Errorf("cluster = %q, want error naming containerd-config/dr1", v)
	}
	if v := got["dr1/addon/"+name]; v != "ready" {
		t.Errorf("addon = %q, want ready (still probed)", v)
	}
}

func TestCheckJSONContract(t *testing.T) {
	ready := &probeStep{ok: true}
	name := register(t, "ready", ready)
	env := &envfile.Env{
		Name:     "rdr",
		Profiles: []envfile.Profile{{Name: "dr1"}},
		Workers:  []envfile.Worker{{Addons: []envfile.Addon{{Name: name, Args: []string{"dr1"}}}}},
	}
	fp := newFakeProvider()

	c := report.BuildCheck(context.Background(), newPlan(env, fp, addon.Deps{}), fastCheck())
	for i := range c.Steps {
		c.Steps[i].DurationMs = 0 // timing is nondeterministic
	}
	got, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"env":"rdr","steps":[` +
		`{"kind":"cluster","profile":"dr1","global":false,"worker":0,"addon":"","args":[],` +
		`"name":"cluster/dr1","state":"not-ready","error":"","durationMs":0},` +
		`{"kind":"addon","profile":"","global":true,"worker":0,"addon":"` + name + `","args":["dr1"],` +
		`"name":"addon/` + name + `","state":"skipped","error":"cluster dr1 not running","durationMs":0}]}`
	if string(got) != want {
		t.Fatalf("check JSON =\n%s\nwant\n%s", got, want)
	}
}

func TestBuildCheckEmptyPlanHasStepsArray(t *testing.T) {
	c := report.BuildCheck(context.Background(), newPlan(&envfile.Env{Name: "e"}, newFakeProvider(), addon.Deps{}), fastCheck())
	got, _ := json.Marshal(c)
	if want := `{"env":"e","steps":[]}`; string(got) != want {
		t.Fatalf("got %s, want %s", got, want)
	}
}

func TestWriteCheckText(t *testing.T) {
	c := report.Check{Env: "rdr", Steps: []report.CheckStep{
		{Profile: "dr1", Name: "addon/rook-operator", State: report.StateReady},
		{Profile: "dr1", Name: "addon/rook-pool", State: report.StateNotReady},
		{Profile: "dr2", Name: "cluster/dr2", State: report.StateNotReady, Error: "containerd-config/dr2: not applied"},
		{Profile: "dr1", Name: "addon/velero", State: report.StateError, Error: "boom"},
		{Global: true, Name: "addon/rbd-mirror", State: report.StateSkipped, Error: "cluster dr2 not running"},
		{Profile: "hub", Name: "addon/foo", State: report.StateUnimplemented},
	}}
	var buf bytes.Buffer
	if err := report.WriteCheckText(&buf, c); err != nil {
		t.Fatal(err)
	}
	want := "✓ dr1/addon/rook-operator\n" +
		"✗ dr1/addon/rook-pool (not-ready)\n" +
		"✗ dr2/cluster/dr2 (not-ready: containerd-config/dr2: not applied)\n" +
		"? dr1/addon/velero (error: boom)\n" +
		"- global/addon/rbd-mirror (skipped: cluster dr2 not running)\n" +
		"? hub/addon/foo (unimplemented)\n"
	if buf.String() != want {
		t.Fatalf("text =\n%s\nwant\n%s", buf.String(), want)
	}
}
