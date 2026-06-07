// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"strings"
)

// Clusteradm wraps a Runner to issue clusteradm CLI commands. All methods take
// a context so callers can cancel long-running operations.
type Clusteradm struct {
	R Runner
}

// Init runs:
//
//	clusteradm init [--feature-gates=<csv>] [--wait] --context <kubeContext>
//
// featureGates is a list of "Key=Value" strings. When wait is true, --wait is
// appended.
func (c Clusteradm) Init(ctx context.Context, kubeContext string, featureGates []string, wait bool) error {
	args := []string{"init"}
	if len(featureGates) > 0 {
		args = append(args, "--feature-gates="+strings.Join(featureGates, ","))
	}
	if wait {
		args = append(args, "--wait")
	}
	args = append(args, "--context", kubeContext)
	return c.R.Run(ctx, "clusteradm", args...)
}

// Get runs:
//
//	clusteradm get <what> [--output=<output>] --context <kubeContext>
//
// and returns the combined output. Pass an empty output to omit the flag.
func (c Clusteradm) Get(ctx context.Context, kubeContext, what string, output string) (string, error) {
	args := []string{"get", what}
	if output != "" {
		args = append(args, "--output="+output)
	}
	args = append(args, "--context", kubeContext)
	return c.R.Output(ctx, "clusteradm", args...)
}

// Join runs:
//
//	clusteradm join --hub-token=<hubToken> --hub-apiserver=<hubAPIServer>
//	    --cluster-name=<clusterName> --context <kubeContext>
func (c Clusteradm) Join(ctx context.Context, kubeContext, hubToken, hubAPIServer, clusterName string) error {
	return c.R.Run(ctx, "clusteradm",
		"join",
		"--hub-token="+hubToken,
		"--hub-apiserver="+hubAPIServer,
		"--cluster-name="+clusterName,
		"--context", kubeContext,
	)
}

// Addon runs:
//
//	clusteradm addon <action> --names=<csv> --clusters=<csv> --context <kubeContext>
//
// action is typically "enable" or "disable".
func (c Clusteradm) Addon(ctx context.Context, kubeContext, action string, names, clusters []string) error {
	return c.R.Run(ctx, "clusteradm",
		"addon", action,
		"--names="+strings.Join(names, ","),
		"--clusters="+strings.Join(clusters, ","),
		"--context", kubeContext,
	)
}

// Install runs:
//
//	clusteradm install <what> --names=<csv> --context <kubeContext>
func (c Clusteradm) Install(ctx context.Context, kubeContext, what string, names []string) error {
	return c.R.Run(ctx, "clusteradm",
		"install", what,
		"--names="+strings.Join(names, ","),
		"--context", kubeContext,
	)
}
