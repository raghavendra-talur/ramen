// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"time"
)

// Kubectl wraps a Runner to issue kubectl CLI commands. Cluster selection is by
// --context <name>, which minikube sets to the profile name, so callers pass
// the profile name as kubeContext. All methods take a context so callers can
// cancel long-running operations.
type Kubectl struct {
	R Runner
}

// Apply runs `kubectl --context <kubeContext> apply <args...>`.
func (k Kubectl) Apply(ctx context.Context, kubeContext string, args ...string) error {
	all := append([]string{"--context", kubeContext, "apply"}, args...)
	return k.R.Run(ctx, "kubectl", all...)
}

// ApplyKustomization runs `kubectl --context <kubeContext> apply -k <dir>`.
func (k Kubectl) ApplyKustomization(ctx context.Context, kubeContext, dir string) error {
	return k.R.Run(ctx, "kubectl", "--context", kubeContext, "apply", "-k", dir)
}

// WaitRollout runs:
//
//	kubectl --context <kubeContext> -n <namespace> rollout status <resource> --timeout <Ns>
//
// Timeout is formatted as "<seconds>s".
func (k Kubectl) WaitRollout(ctx context.Context, kubeContext, namespace, resource string, timeout time.Duration) error {
	return k.R.Run(ctx, "kubectl",
		"--context", kubeContext,
		"-n", namespace,
		"rollout", "status", resource,
		"--timeout", formatTimeout(timeout),
	)
}

// WaitCondition runs:
//
//	kubectl --context <kubeContext> -n <namespace> wait <resource> --for=condition=<condition> --timeout <Ns>
//
// Timeout is formatted as "<seconds>s".
func (k Kubectl) WaitCondition(ctx context.Context, kubeContext, namespace, resource, condition string, timeout time.Duration) error {
	return k.R.Run(ctx, "kubectl",
		"--context", kubeContext,
		"-n", namespace,
		"wait", resource,
		"--for=condition="+condition,
		"--timeout", formatTimeout(timeout),
	)
}

// Get runs `kubectl --context <kubeContext> -n <namespace> get <args...>` and
// returns the command output.
func (k Kubectl) Get(ctx context.Context, kubeContext, namespace string, args ...string) (string, error) {
	all := append([]string{"--context", kubeContext, "-n", namespace, "get"}, args...)
	return k.R.Output(ctx, "kubectl", all...)
}

// formatTimeout converts a time.Duration to a kubectl-compatible timeout string
// of the form "<seconds>s". Fractional seconds are truncated.
func formatTimeout(d time.Duration) string {
	return fmt.Sprintf("%ds", int(d.Seconds()))
}
