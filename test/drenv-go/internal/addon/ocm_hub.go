// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package addon: ocm/hub initialises the OCM hub and installs hub addons,
// mirroring addons/ocm/hub/start.py.
//
// Steps (serial):
//  1. clusteradm init --feature-gates=ManagedClusterAutoApproval=true --wait
//  2. clusteradm install hub-addon --names=application-manager
//  3. clusteradm install hub-addon --names=governance-policy-framework
//  4. For each deployment in open-cluster-management: wait --for=create + rollout status
//  5. For each deployment in open-cluster-management-hub: wait --for=create + rollout status

package addon

import (
	"context"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// ocmHubAddons lists the hub addon names to install, matching ADDONS in start.py.
var ocmHubAddons = []string{
	"application-manager",
	"governance-policy-framework",
}

// ocmHubDeployments mirrors the DEPLOYMENTS map in addons/ocm/hub/start.py.
// Order matters: open-cluster-management first, then open-cluster-management-hub.
var ocmHubDeployments = []struct {
	namespace   string
	deployments []string
}{
	{
		namespace: "open-cluster-management",
		deployments: []string{
			"cluster-manager",
			"governance-policy-addon-controller",
			"governance-policy-propagator",
			"multicluster-operators-appsub-summary",
			"multicluster-operators-channel",
			"multicluster-operators-placementrule",
			"multicluster-operators-subscription",
		},
	},
	{
		namespace: "open-cluster-management-hub",
		deployments: []string{
			"cluster-manager-placement-controller",
			"cluster-manager-registration-controller",
			"cluster-manager-registration-webhook",
			"cluster-manager-work-webhook",
		},
	},
}

const (
	ocmHubWaitTimeout    = 5 * time.Minute
	ocmHubRolloutTimeout = 5 * time.Minute
)

func init() {
	Register("ocm-hub", buildOCMHub)
}

func buildOCMHub(d Deps, cluster string, _ []string) ensure.Step {
	// Step 1: clusteradm init
	initStep := newApplyStep("clusteradm-init", func(ctx context.Context) error {
		return d.Clusteradm.Init(ctx, cluster, []string{"ManagedClusterAutoApproval=true"}, true)
	})

	// Steps 2+: clusteradm install hub-addon for each addon
	var addonInstallSteps []ensure.Step
	for _, name := range ocmHubAddons {
		n := name // capture
		addonInstallSteps = append(addonInstallSteps,
			newApplyStep("install-hub-addon/"+n, func(ctx context.Context) error {
				return d.Clusteradm.Install(ctx, cluster, "hub-addon", []string{n})
			}),
		)
	}

	// Wait steps: for each namespace/deployment, wait --for=create then rollout status
	var waitSteps []ensure.Step
	for _, ns := range ocmHubDeployments {
		namespace := ns.namespace
		for _, dep := range ns.deployments {
			depName := dep // capture
			resource := "deploy/" + depName
			waitSteps = append(waitSteps,
				newApplyStep("wait-create/"+namespace+"/"+depName, func(ctx context.Context) error {
					return d.K.WaitFor(ctx, cluster, namespace, "create", ocmHubWaitTimeout, resource)
				}),
				newApplyStep("rollout-status/"+namespace+"/"+depName, func(ctx context.Context) error {
					return d.K.RolloutStatus(ctx, cluster, namespace, resource, ocmHubRolloutTimeout)
				}),
			)
		}
	}

	steps := []ensure.Step{initStep}
	steps = append(steps, addonInstallSteps...)
	steps = append(steps, waitSteps...)

	return Serial("addon/ocm-hub", d.Opts, steps...)
}
