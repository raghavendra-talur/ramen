// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"context"
	"errors"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// Step kinds.
const (
	KindCluster = "cluster"
	KindAddon   = "addon"
)

// Step states. Only a Done() that returned true is ever reported as ready.
const (
	StateReady         = "ready"
	StateNotReady      = "not-ready"
	StateError         = "error"
	StateUnimplemented = "unimplemented"
	StateSkipped       = "skipped"
)

// Check is the JSON form of `drenv-go check`.
type Check struct {
	Env   string      `json:"env"`
	Steps []CheckStep `json:"steps"`
}

// CheckStep is the readiness of one cluster or addon step.
type CheckStep struct {
	Kind string `json:"kind"`
	// Profile is the owning profile; "" for a global worker's addon.
	Profile string `json:"profile"`
	Global  bool   `json:"global"`
	Worker  int    `json:"worker"`
	// Addon is the addon name; "" for a cluster step.
	Addon      string   `json:"addon"`
	Args       []string `json:"args"`
	Name       string   `json:"name"`
	State      string   `json:"state"`
	Error      string   `json:"error"`
	DurationMs int64    `json:"durationMs"`
}

// CheckOptions bound the cost of a check.
type CheckOptions struct {
	// Timeout caps each Done() probe so one hung probe cannot block the report.
	Timeout time.Duration
	// Parallel bounds concurrent profiles, and concurrent probes per scope.
	Parallel int
}

// DefaultCheckOptions returns a 30s per-probe timeout and parallelism of 4.
func DefaultCheckOptions() CheckOptions {
	return CheckOptions{Timeout: 30 * time.Second, Parallel: 4}
}

// BuildCheck evaluates every step's Done() in the plan WITHOUT calling Do and
// reports one entry per cluster and per addon, in plan order. Profiles are
// probed in parallel; addons of a profile whose cluster is not running are
// skipped rather than probed, as are global addons whose target profiles are
// not all running. Unregistered addons are reported as unimplemented.
func BuildCheck(ctx context.Context, p *build.Plan, o CheckOptions) Check {
	if o.Parallel < 1 {
		o.Parallel = 1
	}

	perProfile := make([][]CheckStep, len(p.Profiles))
	running := make([]bool, len(p.Profiles))

	var eg errgroup.Group
	eg.SetLimit(o.Parallel)
	for i, pp := range p.Profiles {
		eg.Go(func() error {
			perProfile[i], running[i] = checkProfile(ctx, pp, o)
			return nil
		})
	}
	_ = eg.Wait()

	c := Check{Env: p.Name, Steps: []CheckStep{}}
	profiles := map[string]bool{}
	for i, pp := range p.Profiles {
		c.Steps = append(c.Steps, perProfile[i]...)
		profiles[pp.Profile.Name] = running[i]
	}

	var global []build.PlannedAddon
	for _, w := range p.Workers {
		global = append(global, w.Addons...)
	}
	c.Steps = append(c.Steps, checkAddons(ctx, global, o, func(a build.PlannedAddon) string {
		for _, arg := range a.Args {
			if up, isProfile := profiles[arg]; isProfile && !up {
				return "cluster " + arg + " not running"
			}
		}
		return ""
	})...)
	return c
}

// checkProfile probes the cluster (and its containerd config) and then the
// profile's addons. It reports whether the cluster is running.
func checkProfile(ctx context.Context, pp build.PlannedProfile, o CheckOptions) ([]CheckStep, bool) {
	cs := CheckStep{
		Kind:    KindCluster,
		Profile: pp.Profile.Name,
		Args:    []string{},
		Name:    pp.Cluster.Name(),
	}
	cs.State, cs.Error, cs.DurationMs = probe(ctx, pp.Cluster, o.Timeout)
	clusterState := cs.State
	running := clusterState == StateReady

	// The containerd config is part of preparing the cluster: a running
	// cluster without it is not ready, but its addons can still be probed.
	if running && pp.Containerd != nil {
		state, msg, ms := probe(ctx, pp.Containerd, o.Timeout)
		cs.DurationMs += ms
		if state != StateReady {
			cs.State = state
			cs.Error = pp.Containerd.Name() + ": " + orDefault(msg, "not applied")
		}
	}

	var addons []build.PlannedAddon
	for _, w := range pp.Workers {
		addons = append(addons, w.Addons...)
	}

	skip := ""
	switch {
	case clusterState == StateError:
		skip = "cluster status unknown"
	case !running:
		skip = "cluster not running"
	}
	steps := checkAddons(ctx, addons, o, func(build.PlannedAddon) string { return skip })
	return append([]CheckStep{cs}, steps...), running
}

// checkAddons probes addons concurrently (bounded), keeping input order.
// skipReason returns a non-empty reason to skip an addon without probing it.
func checkAddons(ctx context.Context, addons []build.PlannedAddon, o CheckOptions,
	skipReason func(build.PlannedAddon) string,
) []CheckStep {
	out := make([]CheckStep, len(addons))
	var eg errgroup.Group
	eg.SetLimit(o.Parallel)
	for i, a := range addons {
		eg.Go(func() error {
			out[i] = checkAddon(ctx, a, o, skipReason(a))
			return nil
		})
	}
	_ = eg.Wait()
	return out
}

func checkAddon(ctx context.Context, a build.PlannedAddon, o CheckOptions, skip string) CheckStep {
	s := CheckStep{
		Kind:    KindAddon,
		Profile: a.Profile,
		Global:  a.Global,
		Worker:  a.Worker,
		Addon:   a.Name,
		Args:    nonNil(a.Args),
		Name:    "addon/" + a.Name,
	}
	switch {
	case skip != "":
		s.State, s.Error = StateSkipped, skip
	case !a.Registered:
		// The placeholder step is always Done; never report it as ready.
		s.State = StateUnimplemented
	default:
		s.Name = a.Step.Name()
		s.State, s.Error, s.DurationMs = probe(ctx, a.Step, o.Timeout)
	}
	return s
}

// probe evaluates s.Done under timeout and maps the outcome to a state. A probe
// that hangs past the timeout (even one ignoring its context) or panics is
// reported as an error, never as ready.
func probe(ctx context.Context, s ensure.Step, timeout time.Duration) (state, msg string, ms int64) {
	start := time.Now()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	type result struct {
		ok  bool
		err error
	}
	ch := make(chan result, 1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				ch <- result{err: fmt.Errorf("probe panicked: %v", r)}
			}
		}()
		ok, err := s.Done(ctx)
		ch <- result{ok: ok, err: err}
	}()

	var r result
	select {
	case r = <-ch:
	case <-ctx.Done():
		r = result{err: ctx.Err()}
	}
	ms = time.Since(start).Milliseconds()

	switch {
	case errors.Is(ctx.Err(), context.DeadlineExceeded) && !r.ok:
		return StateError, fmt.Sprintf("timed out after %s", timeout), ms
	case r.err != nil:
		return StateError, r.err.Error(), ms
	case r.ok:
		return StateReady, "", ms
	default:
		return StateNotReady, "", ms
	}
}

func orDefault(s, def string) string {
	if s == "" {
		return def
	}
	return s
}
