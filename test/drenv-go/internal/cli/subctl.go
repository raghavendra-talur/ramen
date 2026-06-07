// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"runtime"
)

// Subctl wraps a Runner to issue subctl CLI commands. All methods take a context
// so callers can cancel long-running operations.
type Subctl struct {
	R Runner
}

// DeployBroker runs:
//
//	subctl deploy-broker --context <kubeContext> [--globalnet] [--version <version>]
//
// Pass an empty version to omit the flag.
func (s Subctl) DeployBroker(ctx context.Context, kubeContext string, globalnet bool, version string) error {
	args := []string{"deploy-broker", "--context", kubeContext}
	if globalnet {
		args = append(args, "--globalnet")
	}
	if version != "" {
		args = append(args, "--version", version)
	}
	return s.R.Run(ctx, "subctl", args...)
}

// Join runs:
//
//	subctl join <brokerInfo> --context <kubeContext> --clusterid <clusterID>
//	    [--cable-driver <cableDriver>] [--version <version>]
//	    --check-broker-certificate=false
//
// Pass empty strings for cableDriver and version to omit those flags.
func (s Subctl) Join(ctx context.Context, brokerInfo, kubeContext, clusterID, cableDriver, version string) error {
	args := []string{"join", brokerInfo,
		"--context", kubeContext,
		"--clusterid", clusterID,
	}
	if cableDriver != "" {
		args = append(args, "--cable-driver", cableDriver)
	}
	if version != "" {
		args = append(args, "--version", version)
	}
	// Mirror Python subctl.join: --check-broker-certificate=false is only
	// appended on macOS (darwin). See test/drenv/subctl.py join().
	if runtime.GOOS == "darwin" {
		args = append(args, "--check-broker-certificate=false")
	}
	return s.R.Run(ctx, "subctl", args...)
}
