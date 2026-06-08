// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package addon: ocm/cluster joins a managed cluster to an OCM hub and enables
// addons, mirroring addons/ocm/cluster/start.py.
//
// This is a CROSS-CLUSTER addon: cluster param = the managed cluster context
// (clusterName), args[0] = the hub context.
//
// Steps (serial):
//  1. wait_for_hub: for each hub deployment WaitFor(create) + RolloutStatus on hub
//  2. join: clusteradm get token on hub; clusteradm join on cluster
//  3. wait_for_managed_cluster: on hub, WaitFor managedcluster create (180s),
//     hubAcceptsClient=true (60s), and 3 conditions HubAcceptedManagedCluster/
//     ManagedClusterJoined/ManagedClusterConditionAvailable (60s each)
//  4. label: kubectl label managedclusters/<cluster> name=<cluster> on hub
//  5. enable_addons: clusteradm addon enable on hub
//  6. wait: for each addon deployment in open-cluster-management-agent-addon
//     on cluster: WaitFor(create) + RolloutStatus

package addon

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// ocmClusterAddons matches the ADDONS tuple in addons/ocm/cluster/start.py.
var ocmClusterAddons = []struct {
	name       string
	deployment string
}{
	{"application-manager", "application-manager"},
	{"governance-policy-framework", "governance-policy-framework"},
	{"config-policy-controller", "config-policy-controller"},
}

const ocmClusterAddonsNamespace = "open-cluster-management-agent-addon"

// ocmClusterHubDeployments mirrors HUB_DEPLOYMENTS in addons/ocm/cluster/start.py.
var ocmClusterHubDeployments = []struct {
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
	ocmManagedClusterCreateTimeout    = 180 * time.Second
	ocmManagedClusterConditionTimeout = 60 * time.Second
	ocmClusterWaitTimeout             = 5 * time.Minute
	ocmClusterRolloutTimeout          = 5 * time.Minute
)

// hubTokenOutput is the JSON structure returned by `clusteradm get token --output=json`.
type hubTokenOutput struct {
	HubToken     string `json:"hub-token"`
	HubAPIServer string `json:"hub-apiserver"`
}

func init() {
	Register("ocm-cluster", buildOCMCluster)
}

func buildOCMCluster(d Deps, cluster string, args []string) ensure.Step {
	if len(args) < 1 {
		panic("ocm/cluster: args must contain at least one element (hub context)")
	}
	hub := args[0]

	// --- wait_for_hub steps ---
	// Mirror Python wait_for_hub: wait for each namespace to exist before
	// waiting for the deployments within it.
	var hubWaitSteps []ensure.Step
	for _, ns := range ocmClusterHubDeployments {
		namespace := ns.namespace // capture
		// Wait for the namespace to be created before probing deployments inside it.
		// Mirrors: kubectl.wait("namespace/<ns>", "--for=create", context=hub)
		hubWaitSteps = append(hubWaitSteps,
			newApplyStep("wait-hub-namespace/"+namespace, func(ctx context.Context) error {
				return d.K.WaitFor(ctx, hub, "", "create", ocmClusterWaitTimeout, "namespace/"+namespace)
			}),
		)
		for _, dep := range ns.deployments {
			depName := dep // capture
			resource := "deploy/" + depName
			hubWaitSteps = append(hubWaitSteps,
				newApplyStep("wait-hub-create/"+namespace+"/"+depName, func(ctx context.Context) error {
					return d.K.WaitFor(ctx, hub, namespace, "create", ocmClusterWaitTimeout, resource)
				}),
				newApplyStep("rollout-hub/"+namespace+"/"+depName, func(ctx context.Context) error {
					return d.K.RolloutStatus(ctx, hub, namespace, resource, ocmClusterRolloutTimeout)
				}),
			)
		}
	}

	// --- join step ---
	// Done=false always: Get + parse + Join must run every time.
	joinStep := newApplyStep("join", func(ctx context.Context) error {
		out, err := d.Clusteradm.Get(ctx, hub, "token", "json")
		if err != nil {
			return fmt.Errorf("ocm/cluster: clusteradm get token on %s: %w", hub, err)
		}
		var info hubTokenOutput
		if err := json.Unmarshal([]byte(out), &info); err != nil {
			return fmt.Errorf("ocm/cluster: parse hub token output: %w", err)
		}
		return d.Clusteradm.Join(ctx, cluster, info.HubToken, info.HubAPIServer, cluster)
	})

	// --- wait_for_managed_cluster steps ---
	managedCluster := "managedcluster/" + cluster
	waitMCCreate := newApplyStep("wait-mc-create", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, hub, "", "create", ocmManagedClusterCreateTimeout, managedCluster)
	})
	waitMCHubAccepts := newApplyStep("wait-mc-hubAcceptsClient", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, hub, "", "jsonpath={.spec.hubAcceptsClient}=true",
			ocmManagedClusterConditionTimeout, managedCluster)
	})
	conditions := []string{
		"HubAcceptedManagedCluster",
		"ManagedClusterJoined",
		"ManagedClusterConditionAvailable",
	}
	var condSteps []ensure.Step
	for _, cond := range conditions {
		c := cond // capture
		condSteps = append(condSteps, newApplyStep("wait-mc-condition/"+c, func(ctx context.Context) error {
			return d.K.WaitFor(ctx, hub, "", "condition="+c,
				ocmManagedClusterConditionTimeout, managedCluster)
		}))
	}

	// --- label step ---
	labelStep := newApplyStep("label-cluster", func(ctx context.Context) error {
		return d.K.Label(ctx, hub, "managedclusters/"+cluster, "name="+cluster)
	})

	// --- enable_addons step ---
	addonNames := make([]string, len(ocmClusterAddons))
	for i, a := range ocmClusterAddons {
		addonNames[i] = a.name
	}
	enableAddonsStep := newApplyStep("enable-addons", func(ctx context.Context) error {
		return d.Clusteradm.Addon(ctx, hub, "enable", addonNames, []string{cluster})
	})

	// --- wait for addon deployments on the managed cluster ---
	var addonWaitSteps []ensure.Step
	for _, a := range ocmClusterAddons {
		dep := a.deployment // capture
		resource := "deploy/" + dep
		addonWaitSteps = append(addonWaitSteps,
			newApplyStep("wait-addon-create/"+dep, func(ctx context.Context) error {
				return d.K.WaitFor(ctx, cluster, ocmClusterAddonsNamespace, "create", ocmClusterWaitTimeout, resource)
			}),
			newApplyStep("rollout-addon/"+dep, func(ctx context.Context) error {
				return d.K.RolloutStatus(ctx, cluster, ocmClusterAddonsNamespace, resource, ocmClusterRolloutTimeout)
			}),
		)
	}

	// Assemble all steps in order matching the Python deploy() then wait() functions.
	steps := make([]ensure.Step, 0, len(hubWaitSteps)+1+4+len(condSteps)+2+len(addonWaitSteps))
	steps = append(steps, hubWaitSteps...)
	steps = append(steps, joinStep)
	steps = append(steps, waitMCCreate, waitMCHubAccepts)
	steps = append(steps, condSteps...)
	steps = append(steps, labelStep, enableAddonsStep)
	steps = append(steps, addonWaitSteps...)

	return gatedAddon("addon/ocm-cluster", d.Opts, ocmClusterReady(d.K, hub, cluster), steps...)
}

// ocmClusterReady is satisfied when the managed cluster is Available on the hub
// and every cluster addon deployment is rolled out on the managed cluster.
func ocmClusterReady(k *cli.Kubectl, hub, cluster string) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		avail := jsonPathEquals(ctx, k, hub, "", "managedcluster/"+cluster,
			`{.status.conditions[?(@.type=="ManagedClusterConditionAvailable")].status}`, "True")
		if !avail {
			return false, nil
		}
		for _, a := range ocmClusterAddons {
			if !deploymentAvailable(ctx, k, cluster, ocmClusterAddonsNamespace, a.deployment) {
				return false, nil
			}
		}
		return true, nil
	}
}
