// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package provider defines the Provider interface and its Status enum for
// abstracting cluster lifecycle operations. MinikubeProvider implements
// Provider using a cli.Minikube instance.
package provider

import (
	"context"

	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

// Status is the lifecycle state of a cluster as reported by the provider.
type Status int

const (
	// StatusRunning means the cluster host and API server are both running.
	StatusRunning Status = iota
	// StatusStopped means the cluster host is stopped.
	StatusStopped
	// StatusNotFound means the cluster profile does not exist.
	StatusNotFound
	// StatusUnknown means the cluster is in some other unrecognised state.
	StatusUnknown
)

// String returns a human-readable representation of the Status.
func (s Status) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusStopped:
		return "stopped"
	case StatusNotFound:
		return "not-found"
	default:
		return "unknown"
	}
}

// Provider abstracts cluster lifecycle operations so that different backends
// (minikube, kind, …) can be swapped without changing the ensure layer.
type Provider interface {
	// Exists reports whether the cluster profile exists (i.e. is not StatusNotFound).
	Exists(ctx context.Context, profile string) (bool, error)
	// Start ensures the cluster is running, creating it if necessary.
	Start(ctx context.Context, p envfile.Profile) error
	// Stop stops a running cluster without deleting it.
	Stop(ctx context.Context, profile string) error
	// Delete removes the cluster and all its resources.
	Delete(ctx context.Context, profile string) error
	// Status returns the current lifecycle status of the cluster.
	Status(ctx context.Context, profile string) (Status, error)
	// LoadImage loads a container image into the cluster's container runtime.
	LoadImage(ctx context.Context, profile, image string) error
}
