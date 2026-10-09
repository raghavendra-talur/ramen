// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package build

import (
	"fmt"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// Plan is the structured layout of a start tree: every cluster and addon step
// together with where it lives (profile, worker, position). Start renders it
// into nested ensure groups via Tree; read-only consumers (e.g. a readiness
// check) walk it directly instead of reaching into opaque group internals.
type Plan struct {
	Name     string
	Profiles []PlannedProfile
	// Workers are the global workers, run after every profile.
	Workers []PlannedWorker

	opts ensure.Options
}

// PlannedProfile is one profile's slice of the plan.
type PlannedProfile struct {
	Profile envfile.Profile
	// Cluster ensures the profile's cluster is running.
	Cluster ensure.Step
	// Containerd applies the profile's containerd config after the cluster is
	// up; nil when the profile has none (or is external).
	Containerd ensure.Step
	Workers    []PlannedWorker
}

// PlannedWorker is a serial list of addons; workers of one scope run in
// parallel. Index is the worker's position in the envfile, kept stable when a
// plan is filtered.
type PlannedWorker struct {
	Index  int
	Addons []PlannedAddon
}

// PlannedAddon is one addon invocation and its step.
type PlannedAddon struct {
	// Profile is the owning profile, or "" for a global worker's addon.
	Profile string
	Global  bool
	Worker  int
	Name    string
	Args    []string
	// Registered is false when the addon has no builder; Step is then a
	// no-op placeholder that is always Done.
	Registered bool
	Step       ensure.Step
}

// NewPlan builds the plan for e: one provider per profile (via providerFor),
// one cluster step per profile, an optional containerd step, and one step per
// addon built from the registry.
func NewPlan(e *envfile.Env, providerFor ProviderSelector, d addon.Deps, opts ensure.Options) *Plan {
	p := &Plan{Name: e.Name, opts: opts}

	p.Profiles = make([]PlannedProfile, len(e.Profiles))
	for i, prof := range e.Profiles {
		prov := providerFor(prof)
		pp := PlannedProfile{Profile: prof}
		pp.Workers = planWorkers(d, prof.Name, false, prof.Workers, opts)
		pp.Cluster = provider.ClusterRunningStep(prov, prof)
		// After the cluster is up, apply the profile's containerd config (e.g.
		// rook's device_ownership_from_security_context) before any addon runs.
		// Only minikube profiles with a containerd block need this; external
		// clusters are managed elsewhere.
		if !prof.External && len(prof.Containerd) > 0 && d.MK != nil {
			pp.Containerd = provider.ContainerdConfigStep(d.MK, prof)
		}
		p.Profiles[i] = pp
	}

	// Global addons target clusters given by their args; they are built with
	// cluster="" so builders know they are in global context.
	p.Workers = planWorkers(d, "", true, e.Workers, opts)
	return p
}

func planWorkers(d addon.Deps, profile string, global bool, ws []envfile.Worker, opts ensure.Options) []PlannedWorker {
	out := make([]PlannedWorker, len(ws))
	for wi, w := range ws {
		addons := make([]PlannedAddon, len(w.Addons))
		for ai, a := range w.Addons {
			_, registered := addon.Lookup(a.Name)
			addons[ai] = PlannedAddon{
				Profile:    profile,
				Global:     global,
				Worker:     wi,
				Name:       a.Name,
				Args:       a.Args,
				Registered: registered,
				Step:       buildAddonStep(d, profile, a, opts),
			}
		}
		out[wi] = PlannedWorker{Index: wi, Addons: addons}
	}
	return out
}

// Addons returns every addon in the plan in tree order: profile addons first
// (profile, worker, position), then global addons.
func (p *Plan) Addons() []PlannedAddon {
	var out []PlannedAddon
	for _, pp := range p.Profiles {
		for _, w := range pp.Workers {
			out = append(out, w.Addons...)
		}
	}
	for _, w := range p.Workers {
		out = append(out, w.Addons...)
	}
	return out
}

// Tree renders the plan as the start step tree: a serial top-level group named
// after the environment with
//  1. a parallel "profiles" group: per profile a serial group of
//     [cluster, (containerd), parallel "workers" of serial worker groups];
//  2. a parallel "workers" group of serial global-worker groups (omitted when
//     there are no global workers).
func (p *Plan) Tree() ensure.Step {
	profileSteps := make([]ensure.Step, len(p.Profiles))
	for i, pp := range p.Profiles {
		children := []ensure.Step{pp.Cluster}
		if pp.Containerd != nil {
			children = append(children, pp.Containerd)
		}
		if len(pp.Workers) > 0 {
			children = append(children,
				ensure.NewGroup("workers", ensure.Parallel, p.opts, p.workerGroups("worker/%d", pp.Workers)...),
			)
		}
		profileSteps[i] = ensure.NewGroup("profile/"+pp.Profile.Name, ensure.Serial, p.opts, children...)
	}

	children := []ensure.Step{
		ensure.NewGroup("profiles", ensure.Parallel, p.opts, profileSteps...),
	}
	if len(p.Workers) > 0 {
		children = append(children,
			ensure.NewGroup("workers", ensure.Parallel, p.opts, p.workerGroups("global-worker/%d", p.Workers)...),
		)
	}
	return ensure.NewGroup(p.Name, ensure.Serial, p.opts, children...)
}

func (p *Plan) workerGroups(nameFmt string, ws []PlannedWorker) []ensure.Step {
	out := make([]ensure.Step, len(ws))
	for i, w := range ws {
		steps := make([]ensure.Step, len(w.Addons))
		for ai, a := range w.Addons {
			steps[ai] = a.Step
		}
		out[i] = ensure.NewGroup(fmt.Sprintf(nameFmt, w.Index), ensure.Serial, p.opts, steps...)
	}
	return out
}
