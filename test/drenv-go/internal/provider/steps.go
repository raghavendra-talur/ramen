// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

// clusterRunningStep is an ensure.Step that ensures a cluster is running.
type clusterRunningStep struct {
	p    Provider
	prof envfile.Profile
}

// ClusterRunningStep returns an ensure.Step whose Done condition is
// Status==StatusRunning and whose Do action calls p.Start.
func ClusterRunningStep(p Provider, prof envfile.Profile) ensure.Step {
	return clusterRunningStep{p: p, prof: prof}
}

func (s clusterRunningStep) Name() string { return "cluster/" + s.prof.Name }

func (s clusterRunningStep) Done(ctx context.Context) (bool, error) {
	st, err := s.p.Status(ctx, s.prof.Name)
	if err != nil {
		return false, err
	}
	return st == StatusRunning, nil
}

func (s clusterRunningStep) Do(ctx context.Context) error {
	return s.p.Start(ctx, s.prof)
}

// clusterAbsentStep is an ensure.Step that ensures a cluster does not exist.
type clusterAbsentStep struct {
	p    Provider
	prof envfile.Profile
}

// ClusterAbsentStep returns an ensure.Step whose Done condition is
// Status==StatusNotFound and whose Do action calls p.Delete.
func ClusterAbsentStep(p Provider, prof envfile.Profile) ensure.Step {
	return clusterAbsentStep{p: p, prof: prof}
}

func (s clusterAbsentStep) Name() string { return "cluster/" + s.prof.Name + " absent" }

func (s clusterAbsentStep) Done(ctx context.Context) (bool, error) {
	st, err := s.p.Status(ctx, s.prof.Name)
	if err != nil {
		return false, err
	}
	return st == StatusNotFound, nil
}

func (s clusterAbsentStep) Do(ctx context.Context) error {
	return s.p.Delete(ctx, s.prof.Name)
}
