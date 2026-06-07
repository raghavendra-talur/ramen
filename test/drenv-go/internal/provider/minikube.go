// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"strconv"
	"strings"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

// MinikubeProvider implements Provider using a cli.Minikube instance.
type MinikubeProvider struct {
	MK *cli.Minikube
}

var _ Provider = MinikubeProvider{}

// Status maps the raw cli.MinikubeStatus to the provider Status enum.
//
// Mapping rules:
//   - Host == "Running" && APIServer == "Running" → StatusRunning
//   - Host == "Stopped" → StatusStopped
//   - Host == "" (cli layer's "not found" signal, nil error) → StatusNotFound
//   - any other state (including error with non-empty Host) → StatusUnknown (or propagated error)
func (mp MinikubeProvider) Status(ctx context.Context, profile string) (Status, error) {
	st, err := mp.MK.Status(ctx, profile)
	if err != nil {
		// The cli layer returns ({}, err) on any genuine failure. This err-first
		// check ensures such a failure is never misread as NotFound below.
		return StatusUnknown, err
	}

	// Empty Host is the cli layer's signal that the profile does not exist.
	if st.Host == "" {
		return StatusNotFound, nil
	}

	if st.Host == "Running" && st.APIServer == "Running" {
		return StatusRunning, nil
	}

	if st.Host == "Stopped" {
		return StatusStopped, nil
	}

	return StatusUnknown, nil
}

// Exists reports whether the cluster profile is not StatusNotFound.
func (mp MinikubeProvider) Exists(ctx context.Context, profile string) (bool, error) {
	s, err := mp.Status(ctx, profile)
	if err != nil {
		return false, err
	}
	return s != StatusNotFound, nil
}

// Start builds the minikube start argument list from the profile and delegates
// to cli.Minikube.Start.
//
// Argument-building rules:
//   - Always include: -p <name>
//   - Include --driver <driver> only when Driver is set and does not start with "$"
//     (a leading "$" means the value is an unresolved template placeholder).
//   - Include --network <network> under the same condition.
//   - Include --cpus <n> when CPUs > 0.
//   - Include --memory <mem> when Memory is non-empty.
func (mp MinikubeProvider) Start(ctx context.Context, p envfile.Profile) error {
	args := []string{"-p", p.Name}

	if p.Driver != "" && !strings.HasPrefix(p.Driver, "$") {
		args = append(args, "--driver", p.Driver)
	}

	if p.Network != "" && !strings.HasPrefix(p.Network, "$") {
		args = append(args, "--network", p.Network)
	}

	if p.CPUs > 0 {
		args = append(args, "--cpus", strconv.Itoa(p.CPUs))
	}

	if p.Memory != "" {
		args = append(args, "--memory", p.Memory)
	}

	return mp.MK.Start(ctx, args...)
}

// Stop delegates to cli.Minikube.Stop.
func (mp MinikubeProvider) Stop(ctx context.Context, profile string) error {
	return mp.MK.Stop(ctx, profile)
}

// Delete delegates to cli.Minikube.Delete.
func (mp MinikubeProvider) Delete(ctx context.Context, profile string) error {
	return mp.MK.Delete(ctx, profile)
}

// LoadImage delegates to cli.Minikube.LoadImage.
func (mp MinikubeProvider) LoadImage(ctx context.Context, profile, image string) error {
	return mp.MK.LoadImage(ctx, profile, image)
}
