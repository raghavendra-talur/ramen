// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"fmt"
	"time"
)

// ApplyFile runs `kubectl --context <kubeContext> apply --filename <path>`.
func (k Kubectl) ApplyFile(ctx context.Context, kubeContext, path string) error {
	return k.R.Run(ctx, "kubectl", "--context", kubeContext, "apply", "--filename", path)
}

// ApplyKustomizeDir runs `kubectl --context <kubeContext> apply --kustomize <dir>`.
func (k Kubectl) ApplyKustomizeDir(ctx context.Context, kubeContext, dir string) error {
	return k.R.Run(ctx, "kubectl", "--context", kubeContext, "apply", "--kustomize", dir)
}

// ApplyServerSideFile runs:
//
//	kubectl --context <kubeContext> apply --server-side=true --filename <path>
func (k Kubectl) ApplyServerSideFile(ctx context.Context, kubeContext, path string) error {
	return k.R.Run(ctx, "kubectl", "--context", kubeContext, "apply", "--server-side=true", "--filename", path)
}

// ApplyServerSideKustomizeDir runs:
//
//	kubectl --context <kubeContext> apply --server-side=true --kustomize <dir>
//
// This is needed for large CRD bundles (e.g. OLM) where the annotation
// exceeds the 262144-byte limit that client-side apply enforces.
func (k Kubectl) ApplyServerSideKustomizeDir(ctx context.Context, kubeContext, dir string) error {
	return k.R.Run(ctx, "kubectl", "--context", kubeContext, "apply", "--server-side=true", "--kustomize", dir)
}

// ApplyStdin runs `kubectl --context <kubeContext> apply --filename -` with manifest
// supplied on stdin.
func (k Kubectl) ApplyStdin(ctx context.Context, kubeContext string, manifest []byte) error {
	return k.R.RunStdin(ctx, string(manifest), "kubectl", "--context", kubeContext, "apply", "--filename", "-")
}

// ApplyStdinNamespace runs:
//
//	kubectl --context <kubeContext> apply --filename - --namespace=<namespace>
//
// with manifest supplied on stdin. This is used when the Python source passes
// --namespace=<ns> as an explicit flag to kubectl apply (e.g. rbd-mirror secret).
func (k Kubectl) ApplyStdinNamespace(ctx context.Context, kubeContext, namespace string, manifest []byte) error {
	return k.R.RunStdin(ctx, string(manifest), "kubectl",
		"--context", kubeContext,
		"apply",
		"--filename", "-",
		"--namespace="+namespace,
	)
}

// WaitFor runs:
//
//	kubectl --context <kubeContext> [-n <namespace>] wait <target...> --for=<forExpr> --timeout <Ns>
//
// Pass an empty namespace to omit the -n flag. forExpr is e.g. "condition=established",
// "create", or "jsonpath={.status.phase}=Running".
func (k Kubectl) WaitFor(ctx context.Context, kubeContext, namespace, forExpr string, timeout time.Duration, target ...string) error {
	args := []string{"--context", kubeContext}
	if namespace != "" {
		args = append(args, "-n", namespace)
	}
	args = append(args, "wait")
	args = append(args, target...)
	args = append(args, "--for="+forExpr, "--timeout", formatTimeout(timeout))
	return k.R.Run(ctx, "kubectl", args...)
}

// WaitForFile runs:
//
//	kubectl --context <kubeContext> wait --for=<forExpr> --filename <path> --timeout <Ns>
func (k Kubectl) WaitForFile(ctx context.Context, kubeContext, forExpr, path string, timeout time.Duration) error {
	return k.R.Run(ctx, "kubectl",
		"--context", kubeContext,
		"wait",
		"--for="+forExpr,
		"--filename", path,
		"--timeout", formatTimeout(timeout),
	)
}

// RolloutStatus runs:
//
//	kubectl --context <kubeContext> -n <namespace> rollout status <resource> --timeout <Ns>
func (k Kubectl) RolloutStatus(ctx context.Context, kubeContext, namespace, resource string, timeout time.Duration) error {
	return k.R.Run(ctx, "kubectl",
		"--context", kubeContext,
		"-n", namespace,
		"rollout", "status", resource,
		"--timeout", formatTimeout(timeout),
	)
}

// GetJSONPath runs:
//
//	kubectl --context <kubeContext> -n <namespace> get <resource> --output=jsonpath=<jsonpath>
//
// and returns the trimmed output.
func (k Kubectl) GetJSONPath(ctx context.Context, kubeContext, namespace, resource, jsonpath string) (string, error) {
	return k.R.Output(ctx, "kubectl",
		"--context", kubeContext,
		"-n", namespace,
		"get", resource,
		"--output=jsonpath="+jsonpath,
	)
}

// KubectlExec runs:
//
//	kubectl --context <kubeContext> -n <namespace> exec <resource> -- <cmd...>
//
// and returns the combined output.
func (k Kubectl) KubectlExec(ctx context.Context, kubeContext, namespace, resource string, cmd ...string) (string, error) {
	args := []string{"--context", kubeContext, "-n", namespace, "exec", resource, "--"}
	args = append(args, cmd...)
	return k.R.Output(ctx, "kubectl", args...)
}

// Patch runs:
//
//	kubectl --context <kubeContext> -n <namespace> patch <resource> --type=<patchType> --patch=<patch>
func (k Kubectl) Patch(ctx context.Context, kubeContext, namespace, resource, patchType, patch string) error {
	return k.R.Run(ctx, "kubectl",
		"--context", kubeContext,
		"-n", namespace,
		"patch", resource,
		"--type="+patchType,
		"--patch="+patch,
	)
}

// Annotate runs:
//
//	kubectl --context <kubeContext> annotate <resource> <annotation> --overwrite
func (k Kubectl) Annotate(ctx context.Context, kubeContext, resource, annotation string) error {
	return k.R.Run(ctx, "kubectl",
		"--context", kubeContext,
		"annotate", resource, annotation,
		"--overwrite",
	)
}

// Label runs:
//
//	kubectl --context <kubeContext> label <resource> <label> --overwrite
func (k Kubectl) Label(ctx context.Context, kubeContext, resource, label string) error {
	return k.R.Run(ctx, "kubectl",
		"--context", kubeContext,
		"label", resource, label,
		"--overwrite",
	)
}

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

// Config runs `kubectl config <args...>` and returns the combined output.
// Use this for kubeconfig manipulation such as use-context and set-context.
func (k Kubectl) Config(ctx context.Context, args ...string) (string, error) {
	all := append([]string{"config"}, args...)
	return k.R.Output(ctx, "kubectl", all...)
}

// formatTimeout converts a time.Duration to a kubectl-compatible timeout string
// of the form "<seconds>s". Fractional seconds are truncated.
func formatTimeout(d time.Duration) string {
	return fmt.Sprintf("%ds", int(d.Seconds()))
}
