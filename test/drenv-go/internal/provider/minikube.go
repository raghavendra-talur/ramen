// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"runtime"
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

// startExtraConfig is always passed to `minikube start`, matching EXTRA_CONFIG in
// the Python drenv minikube provider. Telling the kubelet not to serialize image
// pulls speeds up concurrent addon image pulls (~20% faster regional-dr start).
var startExtraConfig = []string{"kubelet.serialize-image-pulls=false"}

// startWaitTimeout bounds `minikube start --wait-timeout`, matching Python's
// _START_TIMEOUT (180s).
const startWaitTimeout = "180s"

// Start builds the minikube start argument list from the profile and delegates
// to cli.Minikube.Start. The flag order and conditions mirror the Python drenv
// minikube provider's start() so the two tools create equivalent clusters.
//
// Argument-building rules:
//   - Always include: -p <name>.
//   - Include --driver/--network only when set and not a "$"-prefixed unresolved
//     template placeholder.
//   - Include --container-runtime, --extra-disks, --disk-size, --nodes, --cni,
//     --cpus, --memory, --service-cluster-ip-range, --feature-gates only when set.
//   - Always include --extra-config for each startExtraConfig entry, then any
//     profile-specific --extra-config entries.
//   - Include --rosetta on darwin/arm64 unless the profile sets rosetta: false
//     (enables running amd64 images on Apple silicon).
//   - Always include --wait-timeout.
func (mp MinikubeProvider) Start(ctx context.Context, p envfile.Profile) error {
	return mp.MK.Start(ctx, buildStartArgs(p, runtime.GOOS, runtime.GOARCH)...)
}

// buildStartArgs is the pure arg-builder behind Start, parameterized on GOOS and
// GOARCH so the platform-dependent --rosetta rule can be unit-tested.
func buildStartArgs(p envfile.Profile, goos, goarch string) []string {
	args := []string{"-p", p.Name}

	if p.Driver != "" && !strings.HasPrefix(p.Driver, "$") {
		args = append(args, "--driver", p.Driver)
	}

	if p.ContainerRuntime != "" {
		args = append(args, "--container-runtime", p.ContainerRuntime)
	}

	if p.ExtraDisks > 0 {
		args = append(args, "--extra-disks", strconv.Itoa(p.ExtraDisks))
	}

	if p.DiskSize != "" {
		args = append(args, "--disk-size", p.DiskSize)
	}

	if p.Network != "" && !strings.HasPrefix(p.Network, "$") {
		args = append(args, "--network", p.Network)
	}

	if p.Nodes > 0 {
		args = append(args, "--nodes", strconv.Itoa(p.Nodes))
	}

	if p.CNI != "" {
		args = append(args, "--cni", p.CNI)
	}

	if p.CPUs > 0 {
		args = append(args, "--cpus", strconv.Itoa(p.CPUs))
	}

	if p.Memory != "" {
		args = append(args, "--memory", p.Memory)
	}

	if p.ServiceClusterIPRange != "" {
		args = append(args, "--service-cluster-ip-range", p.ServiceClusterIPRange)
	}

	for _, pair := range startExtraConfig {
		args = append(args, "--extra-config", pair)
	}
	for _, pair := range p.ExtraConfig {
		args = append(args, "--extra-config", pair)
	}

	if len(p.FeatureGates) > 0 {
		args = append(args, "--feature-gates", strings.Join(p.FeatureGates, ","))
	}

	if rosettaEnabled(p) && goos == "darwin" && goarch == "arm64" {
		args = append(args, "--rosetta")
	}

	args = append(args, "--wait-timeout", startWaitTimeout)

	return args
}

// rosettaEnabled reports whether --rosetta should be considered for the profile.
// It defaults to true (matching Python) when unset, and honors an explicit
// rosetta: false.
func rosettaEnabled(p envfile.Profile) bool {
	return p.Rosetta == nil || *p.Rosetta
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

// Suspend delegates to cli.Minikube.Pause, freezing the cluster's workloads.
func (mp MinikubeProvider) Suspend(ctx context.Context, profile string) error {
	return mp.MK.Pause(ctx, profile)
}

// Resume delegates to cli.Minikube.Unpause, resuming the cluster's workloads.
func (mp MinikubeProvider) Resume(ctx context.Context, profile string) error {
	return mp.MK.Unpause(ctx, profile)
}
