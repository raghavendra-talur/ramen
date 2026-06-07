// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"strings"
)

// MinikubeStatus holds the subset of fields returned by `minikube status -o json`
// that the provider layer needs to determine cluster health. The raw string values
// ("Running", "Stopped", …) are intentionally left uninterpreted here; the
// provider package maps them to its own Status enum to avoid an import cycle.
type MinikubeStatus struct {
	Name      string
	Host      string
	APIServer string
}

// Minikube wraps a Runner to issue minikube CLI commands. All methods take a
// context so callers can cancel long-running operations.
type Minikube struct {
	R Runner
}

// notFoundMarker is a substring present in minikube's output when a profile
// does not exist. We use this to distinguish "profile absent" (not an error)
// from genuine failures.
const notFoundMarker = "not found"

// Status queries `minikube status -p <profile> -o json` and returns the parsed
// result. When the profile does not exist (minikube prints a "not found"
// message, possibly with a non-zero exit) Status returns a zero-value
// MinikubeStatus (Host == "") and a nil error — callers treat an empty Host as
// the "not found" signal.
func (m Minikube) Status(ctx context.Context, profile string) (MinikubeStatus, error) {
	out, err := m.R.Output(ctx, "minikube", "status", "-p", profile, "-o", "json")

	// Check for the "not found" case regardless of whether there was also an
	// exit error — minikube exits non-zero for missing profiles.
	if strings.Contains(out, notFoundMarker) {
		return MinikubeStatus{}, nil
	}

	if err != nil {
		return MinikubeStatus{}, err
	}

	// Parse the JSON status payload.
	var raw struct {
		Name      string `json:"Name"`
		Host      string `json:"Host"`
		APIServer string `json:"APIServer"`
	}
	if jsonErr := json.Unmarshal([]byte(out), &raw); jsonErr != nil {
		return MinikubeStatus{}, jsonErr
	}
	return MinikubeStatus{
		Name:      raw.Name,
		Host:      raw.Host,
		APIServer: raw.APIServer,
	}, nil
}

// Start runs `minikube start` with the provided args. The caller is
// responsible for constructing the full argument list (e.g. ["-p", profile,
// "--driver", driver, ...]). This gives the provider layer full control.
func (m Minikube) Start(ctx context.Context, args ...string) error {
	return m.R.Run(ctx, "minikube", append([]string{"start"}, args...)...)
}

// Stop runs `minikube stop -p <profile>`.
func (m Minikube) Stop(ctx context.Context, profile string) error {
	return m.R.Run(ctx, "minikube", "stop", "-p", profile)
}

// Delete runs `minikube delete -p <profile>`.
func (m Minikube) Delete(ctx context.Context, profile string) error {
	return m.R.Run(ctx, "minikube", "delete", "-p", profile)
}

// LoadImage runs `minikube image load -p <profile> <image>`.
func (m Minikube) LoadImage(ctx context.Context, profile, image string) error {
	return m.R.Run(ctx, "minikube", "image", "load", "-p", profile, image)
}
