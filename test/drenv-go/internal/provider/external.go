// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

// ExternalProvider implements Provider for clusters that already exist and are
// managed outside of drenv-go (e.g. pre-provisioned cloud clusters, another
// minikube environment, or any cluster reachable via kubeconfig).
//
// Lifecycle operations (Start, Stop, Delete, Suspend, Resume, LoadImage) are
// all no-ops: the cluster is not ours to create or destroy. This mirrors the
// Python external provider (test/drenv/providers/external.py).
//
// Status is determined by probing the API server's /readyz endpoint via
// `kubectl --context <profile> get --raw=/readyz`. A successful probe returns
// StatusRunning; any failure returns StatusNotFound (meaning "not usable from
// here"), matching the Python cluster.status semantics where a cluster without
// a reachable API server is treated as CONFIGURED (not READY).
type ExternalProvider struct {
	K *cli.Kubectl
}

var _ Provider = ExternalProvider{}

// Status probes the cluster API server's /readyz endpoint.
// Returns StatusRunning if reachable, StatusNotFound otherwise.
func (ep ExternalProvider) Status(ctx context.Context, profile string) (Status, error) {
	_, err := ep.K.GetRaw(ctx, profile, "/readyz")
	if err != nil {
		return StatusNotFound, nil //nolint:nilerr // unreachable = not-found for external clusters
	}
	return StatusRunning, nil
}

// Exists reports whether the cluster is reachable.
func (ep ExternalProvider) Exists(ctx context.Context, profile string) (bool, error) {
	s, err := ep.Status(ctx, profile)
	if err != nil {
		return false, err
	}
	return s != StatusNotFound, nil
}

// Start is a no-op for external clusters. The cluster is expected to already
// be running; if not, the ensure model's Done check (Status → StatusRunning)
// will return false and the caller will detect the problem via a timeout.
func (ep ExternalProvider) Start(_ context.Context, _ envfile.Profile) error {
	return nil
}

// Stop is a no-op for external clusters.
func (ep ExternalProvider) Stop(_ context.Context, _ string) error {
	return nil
}

// Delete is a no-op for external clusters.
func (ep ExternalProvider) Delete(_ context.Context, _ string) error {
	return nil
}

// LoadImage is a no-op for external clusters.
func (ep ExternalProvider) LoadImage(_ context.Context, _, _ string) error {
	return nil
}

// Suspend is a no-op for external clusters.
func (ep ExternalProvider) Suspend(_ context.Context, _ string) error {
	return nil
}

// Resume is a no-op for external clusters.
func (ep ExternalProvider) Resume(_ context.Context, _ string) error {
	return nil
}
