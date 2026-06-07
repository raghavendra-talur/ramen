// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import "context"

// Helm wraps a Runner to issue helm CLI commands. All methods take a context so
// callers can cancel long-running operations.
type Helm struct {
	R Runner
}

// RepoAdd runs `helm repo add --force-update <name> <url>`.
func (h Helm) RepoAdd(ctx context.Context, name, url string) error {
	return h.R.Run(ctx, "helm", "repo", "add", "--force-update", name, url)
}

// UpgradeInstall runs:
//
//	helm upgrade --install <release> <chart> --kube-context <kubeContext> [extra...]
func (h Helm) UpgradeInstall(ctx context.Context, release, chart, kubeContext string, extra ...string) error {
	args := []string{"upgrade", "--install", release, chart, "--kube-context", kubeContext}
	args = append(args, extra...)
	return h.R.Run(ctx, "helm", args...)
}
