// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import "context"

// Argocd wraps a Runner to issue argocd CLI commands. All methods take a
// context so callers can cancel long-running operations.
//
// ArgoCD commands that interact with the cluster require the KUBECONFIG
// environment variable to be set to a temporary kubeconfig file. The kubeconfig
// parameter in Login and ClusterAdd is used at runtime by the Exec runner; the
// FakeRunner records only the argv and ignores the environment variable.
type Argocd struct {
	R Runner
}

// Login runs `argocd login --core` with KUBECONFIG=kubeconfig in the
// environment. This is required by argocd before issuing cluster operations.
func (a Argocd) Login(ctx context.Context, kubeconfig string) error {
	return a.R.RunEnv(ctx, []string{"KUBECONFIG=" + kubeconfig}, "argocd", "login", "--core")
}

// ClusterAdd runs `argocd cluster add <cluster> -y` with KUBECONFIG=kubeconfig
// in the environment, returning the command's combined output. The -y flag skips
// the interactive confirmation prompt. The output is returned even on error so
// the caller can match argocd's "NOAUTH" message (a known transient failure).
func (a Argocd) ClusterAdd(ctx context.Context, kubeconfig, cluster string) (string, error) {
	return a.R.OutputEnv(ctx, []string{"KUBECONFIG=" + kubeconfig}, "argocd", "cluster", "add", cluster, "-y")
}
