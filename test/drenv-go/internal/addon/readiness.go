// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// Reality probes used as ensure gates so an already-installed addon is skipped
// on re-run ("addon/<name>: skipped, already satisfied") instead of replaying
// every kubectl apply/wait/rollout. Probes are conservative: any error or
// uncertainty reports "not ready" (never an error), so a gate only skips when
// the end-state is confidently present and otherwise lets the addon (re-)run.

import (
	"context"
	"strings"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

// jsonGet returns the resource's jsonpath value (trimmed), or "" on any error.
func jsonGet(ctx context.Context, k *cli.Kubectl, kubeContext, namespace, resource, jsonpath string) string {
	out, err := k.GetJSONPath(ctx, kubeContext, namespace, resource, jsonpath)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

// jsonPathEquals reports whether the resource's jsonpath value equals want.
func jsonPathEquals(ctx context.Context, k *cli.Kubectl, kubeContext, namespace, resource, jsonpath, want string) bool {
	return jsonGet(ctx, k, kubeContext, namespace, resource, jsonpath) == want
}

// deploymentAvailable reports whether the named Deployment's Available condition
// is True in the given context/namespace.
func deploymentAvailable(ctx context.Context, k *cli.Kubectl, kubeContext, namespace, name string) bool {
	return jsonPathEquals(ctx, k, kubeContext, namespace, "deploy/"+name,
		`{.status.conditions[?(@.type=="Available")].status}`, "True")
}

// daemonSetReady reports whether a DaemonSet has all desired pods ready (and at
// least one is scheduled).
func daemonSetReady(ctx context.Context, k *cli.Kubectl, kubeContext, namespace, name string) bool {
	desired := jsonGet(ctx, k, kubeContext, namespace, "daemonset/"+name, "{.status.desiredNumberScheduled}")
	ready := jsonGet(ctx, k, kubeContext, namespace, "daemonset/"+name, "{.status.numberReady}")
	return desired != "" && desired != "0" && desired == ready
}

// cephPhaseReady reports whether a ceph resource's .status.phase is "Ready".
func cephPhaseReady(ctx context.Context, k *cli.Kubectl, kubeContext, namespace, resource string) bool {
	return jsonPathEquals(ctx, k, kubeContext, namespace, resource, "{.status.phase}", "Ready")
}

// countLabeledResources returns how many resources of the given type match the
// label selector, or 0 on any error.
func countLabeledResources(ctx context.Context, k *cli.Kubectl, kubeContext, namespace, resource, selector string) int {
	out, err := k.Get(ctx, kubeContext, namespace, resource,
		"--selector="+selector, "--output=jsonpath={.items[*].metadata.name}")
	if err != nil {
		return 0
	}
	if strings.TrimSpace(out) == "" {
		return 0
	}
	return len(strings.Fields(out))
}

// gateDeploymentAvailable returns an ensure gate satisfied when the deployment is
// Available.
func gateDeploymentAvailable(k *cli.Kubectl, kubeContext, namespace, name string) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		return deploymentAvailable(ctx, k, kubeContext, namespace, name), nil
	}
}

// gateAllDeploymentsAvailable returns a gate satisfied only when every
// (kubeContext, namespace, name) deployment is Available. Used by addons that
// install the same workload across several clusters or namespaces.
func gateAllDeploymentsAvailable(k *cli.Kubectl, deps []deploymentRef) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		for _, dref := range deps {
			if !deploymentAvailable(ctx, k, dref.kubeContext, dref.namespace, dref.name) {
				return false, nil
			}
		}
		return true, nil
	}
}

// deploymentRef identifies a Deployment to probe.
type deploymentRef struct {
	kubeContext string
	namespace   string
	name        string
}

// resourceExists reports whether the resource is present (a cheap one-shot GET).
// A not-found or any error reports false, so callers fall back to waiting.
func resourceExists(ctx context.Context, k *cli.Kubectl, kubeContext, namespace, resource string) bool {
	_, err := k.Get(ctx, kubeContext, namespace, resource, "--output=name")
	return err == nil
}
