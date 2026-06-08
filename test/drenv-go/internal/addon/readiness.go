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

// deploymentAvailable reports whether the named Deployment's Available condition
// is True in the given context/namespace.
func deploymentAvailable(ctx context.Context, k *cli.Kubectl, kubeContext, namespace, name string) bool {
	out, err := k.GetJSONPath(ctx, kubeContext, namespace, "deploy/"+name,
		`{.status.conditions[?(@.type=="Available")].status}`)
	if err != nil {
		return false
	}
	return strings.TrimSpace(out) == "True"
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
