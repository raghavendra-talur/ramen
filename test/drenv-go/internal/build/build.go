// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package build turns an envfile.Env into ensure.Step trees for the start and
// delete operations. The returned steps compose provider.ClusterRunningStep /
// ClusterAbsentStep entries into groups that can be handed directly to
// ensure.Ensure.
package build

import (
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// Start returns a serial top-level ensure.Group named after the environment.
// Its first child is a parallel "clusters" group that ensures every profile's
// cluster is running.
//
// Milestone 3 will append additional groups here for workers and addons — see
// the comment inside the function body for the exact seam.
func Start(e *envfile.Env, p provider.Provider, opts ensure.Options) ensure.Step {
	clusterSteps := make([]ensure.Step, len(e.Profiles))
	for i, prof := range e.Profiles {
		clusterSteps[i] = provider.ClusterRunningStep(p, prof)
	}

	clustersGroup := ensure.NewGroup("clusters", ensure.Parallel, opts, clusterSteps...)

	// --- Milestone 3 seam ---
	// After clustersGroup, append worker/addon groups here, e.g.:
	//   workerGroups := buildWorkerGroups(e, p, opts)
	//   children = append(children, workerGroups...)
	// Do NOT add any steps here until Milestone 3.
	children := []ensure.Step{clustersGroup}

	return ensure.NewGroup(e.Name, ensure.Serial, opts, children...)
}

// Delete returns a parallel ensure.Group that removes every profile's cluster.
func Delete(e *envfile.Env, p provider.Provider, opts ensure.Options) ensure.Step {
	steps := make([]ensure.Step, len(e.Profiles))
	for i, prof := range e.Profiles {
		steps[i] = provider.ClusterAbsentStep(p, prof)
	}
	return ensure.NewGroup(e.Name+" delete", ensure.Parallel, opts, steps...)
}
