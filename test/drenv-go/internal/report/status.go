// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package report produces the machine-readable (JSON) reports that let other
// programs, such as a dashboard, drive drenv-go: environment status and a
// read-only readiness check. The JSON field names are a contract; do not
// rename them.
package report

import (
	"context"

	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// Status is the JSON form of `drenv-go status`.
type Status struct {
	Env      string          `json:"env"`
	Clusters []ClusterStatus `json:"clusters"`
	Profiles []Profile       `json:"profiles"`
	Workers  []Worker        `json:"workers"`
}

// ClusterStatus is one profile's cluster lifecycle state.
type ClusterStatus struct {
	Name string `json:"name"`
	// State is running, stopped, not-found or unknown.
	State    string `json:"state"`
	External bool   `json:"external"`
	// Error is set when the provider could not read the state (State is then
	// "unknown").
	Error string `json:"error"`
}

// Profile is one profile of the expanded env tree.
type Profile struct {
	Name     string   `json:"name"`
	External bool     `json:"external"`
	Workers  []Worker `json:"workers"`
}

// Worker is one worker of the expanded env tree.
type Worker struct {
	Addons []Addon `json:"addons"`
}

// Addon is one addon invocation of the expanded env tree.
type Addon struct {
	Name string   `json:"name"`
	Args []string `json:"args"`
}

// BuildStatus queries every profile's cluster through providerFor and returns
// the status report together with the expanded env tree. A provider error is
// reported as state "unknown" with the error text, never as a known state.
func BuildStatus(ctx context.Context, e *envfile.Env, providerFor build.ProviderSelector) Status {
	s := Status{
		Env:      e.Name,
		Clusters: make([]ClusterStatus, len(e.Profiles)),
		Profiles: make([]Profile, len(e.Profiles)),
		Workers:  workers(e.Workers),
	}
	for i, prof := range e.Profiles {
		cs := ClusterStatus{Name: prof.Name, External: prof.External}
		st, err := providerFor(prof).Status(ctx, prof.Name)
		if err != nil {
			cs.State = provider.StatusUnknown.String()
			cs.Error = err.Error()
		} else {
			cs.State = st.String()
		}
		s.Clusters[i] = cs
		s.Profiles[i] = Profile{Name: prof.Name, External: prof.External, Workers: workers(prof.Workers)}
	}
	return s
}

func workers(ws []envfile.Worker) []Worker {
	out := make([]Worker, len(ws))
	for i, w := range ws {
		addons := make([]Addon, len(w.Addons))
		for j, a := range w.Addons {
			addons[j] = Addon{Name: a.Name, Args: nonNil(a.Args)}
		}
		out[i] = Worker{Addons: addons}
	}
	return out
}

// nonNil returns s, or an empty slice when s is nil, so JSON arrays are never
// emitted as null.
func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}
