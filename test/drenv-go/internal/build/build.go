// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package build turns an envfile.Env into ensure.Step trees for the start, stop,
// and delete operations. The returned steps compose provider.ClusterRunningStep /
// ClusterStoppedStep / ClusterAbsentStep entries into groups that can be handed
// directly to ensure.Ensure.
package build

import (
	"context"
	"fmt"
	"log"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// ProviderSelector is a function that maps a profile to its Provider. This
// allows each profile to use a different backend (e.g. minikube vs. external).
// Use provider.For to construct the standard selector from real CLI clients,
// or supply a stub in tests.
type ProviderSelector func(envfile.Profile) provider.Provider

// Start returns a serial top-level ensure.Group named after the environment.
// Its children are:
//  1. A parallel "profiles" group: for each profile, a serial group containing
//     [ClusterRunningStep, parallel worker-addon groups].
//  2. A parallel "workers" group (global workers): each global worker is a
//     serial group of addon steps (the addon's cluster is passed as "" since
//     global addons address clusters via their args).
//
// The providerFor selector is called once per profile to pick the correct
// Provider backend (e.g. MinikubeProvider for normal profiles,
// ExternalProvider for external ones).
//
// Addons not found in the registry are represented as no-op steps (Done=true)
// named "addon/<name> (unimplemented)" so an env with not-yet-ported addons
// still composes without error.
func Start(e *envfile.Env, providerFor ProviderSelector, d addon.Deps, opts ensure.Options) ensure.Step {
	profileSteps := make([]ensure.Step, len(e.Profiles))
	for i, prof := range e.Profiles {
		p := providerFor(prof)
		// Build per-worker steps for this profile.
		workerSteps := make([]ensure.Step, len(prof.Workers))
		for wi, w := range prof.Workers {
			addonSteps := make([]ensure.Step, len(w.Addons))
			for ai, a := range w.Addons {
				addonSteps[ai] = buildAddonStep(d, prof.Name, a, opts)
			}
			workerSteps[wi] = ensure.NewGroup(
				fmt.Sprintf("worker/%d", wi),
				ensure.Serial, opts,
				addonSteps...,
			)
		}

		// Profile group: [cluster-running, parallel-workers].
		profileChildren := []ensure.Step{provider.ClusterRunningStep(p, prof)}
		if len(workerSteps) > 0 {
			profileChildren = append(profileChildren,
				ensure.NewGroup("workers", ensure.Parallel, opts, workerSteps...),
			)
		}
		profileSteps[i] = ensure.NewGroup("profile/"+prof.Name, ensure.Serial, opts, profileChildren...)
	}

	children := []ensure.Step{
		ensure.NewGroup("profiles", ensure.Parallel, opts, profileSteps...),
	}

	// Global workers come after all profiles.
	if len(e.Workers) > 0 {
		globalWorkerSteps := make([]ensure.Step, len(e.Workers))
		for wi, w := range e.Workers {
			addonSteps := make([]ensure.Step, len(w.Addons))
			for ai, a := range w.Addons {
				// Global addons target clusters given by their args; pass
				// cluster="" so builders know they are in global context.
				addonSteps[ai] = buildAddonStep(d, "", a, opts)
			}
			globalWorkerSteps[wi] = ensure.NewGroup(
				fmt.Sprintf("global-worker/%d", wi),
				ensure.Serial, opts,
				addonSteps...,
			)
		}
		children = append(children,
			ensure.NewGroup("workers", ensure.Parallel, opts, globalWorkerSteps...),
		)
	}

	return ensure.NewGroup(e.Name, ensure.Serial, opts, children...)
}

// buildAddonStep looks up the addon builder and invokes it. If the addon is not
// registered it returns a no-op step (Done=true) so composition succeeds.
func buildAddonStep(d addon.Deps, cluster string, a envfile.Addon, _ ensure.Options) ensure.Step {
	b, ok := addon.Lookup(a.Name)
	if !ok {
		log.Printf("build: addon %q not in registry — using no-op step", a.Name)
		return noopStep{name: "addon/" + a.Name + " (unimplemented)"}
	}
	return b(d, cluster, a.Args)
}

// noopStep is an ensure.Step that is always Done and does nothing. It is used
// as a placeholder for addons that have not been ported yet.
type noopStep struct{ name string }

func (s noopStep) Name() string                         { return s.name }
func (s noopStep) Done(_ context.Context) (bool, error) { return true, nil }
func (s noopStep) Do(_ context.Context) error           { return nil }

// Delete returns a parallel ensure.Group that removes every profile's cluster.
// providerFor selects the backend per profile.
func Delete(e *envfile.Env, providerFor ProviderSelector, opts ensure.Options) ensure.Step {
	steps := make([]ensure.Step, len(e.Profiles))
	for i, prof := range e.Profiles {
		steps[i] = provider.ClusterAbsentStep(providerFor(prof), prof)
	}
	return ensure.NewGroup(e.Name+" delete", ensure.Parallel, opts, steps...)
}

// Stop returns a parallel ensure.Group that stops every profile's cluster.
// A cluster that does not exist is treated as already stopped (see
// provider.ClusterStoppedStep). providerFor selects the backend per profile.
func Stop(e *envfile.Env, providerFor ProviderSelector, opts ensure.Options) ensure.Step {
	steps := make([]ensure.Step, len(e.Profiles))
	for i, prof := range e.Profiles {
		steps[i] = provider.ClusterStoppedStep(providerFor(prof), prof)
	}
	return ensure.NewGroup(e.Name+" stop", ensure.Parallel, opts, steps...)
}
