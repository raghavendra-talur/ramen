// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rook-cluster deploys the Rook Ceph cluster and waits until it, and the CSI
// driver it publishes, are ready — mirroring addons/rook/cluster/__init__.py
// start() for the Rook 1.20 CSI-operator layout.
//
// Steps (serial):
//  1. kubectl apply -k <AddonsDir>/rook/cluster
//  2. kubectl wait cephcluster/my-cluster --for=create -n rook-ceph (300s)
//  3. kubectl wait cephcluster/my-cluster --for=jsonpath={.status.phase}=Ready
//     -n rook-ceph (600s)
//  4. wait_for_csi_plugins: for each CSI ctrlplugin deployment, exec into its
//     plugin container and poll /etc/ceph-csi-config/config.json until it lists
//     ceph monitors (120s deadline, 5s poll). CephCluster Ready does not imply
//     the CSI plugins have the monitor list yet; provisioning before then fails.
//  5. wait_for_csiaddons_nodes: for each CSIAddonsNode, wait --for=create then
//     --for=jsonpath={.status.state}=Connected, retrying the Connected wait up
//     to 3 times within a shared 300s deadline (the csi-addons sidecar may
//     delete+recreate the node when the driver pod restarts).

package addon

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	rookClusterReadyTimeout  = 600 * time.Second
	rookCSIRolloutTimeout    = 300 * time.Second // generic rook rollout wait (rook-toolbox)
	rookCSIAddonsNodeTimeout = 300 * time.Second // CSIADDONS_TIMEOUT
	rookCSIAddonsAttempts    = 3                 // CSIADDONS_ATTEMPTS
	rookCSIPluginTimeout     = 120 * time.Second // CSI_PLUGIN_TIMEOUT
	rookCSIPluginPoll        = 5 * time.Second   // CSI_PLUGIN_POLL
)

// rookCSIPlugin describes a CSI ctrlplugin deployment and the container inside
// it whose ceph-csi config carries the monitor list (CSI_PLUGINS in Python).
type rookCSIPlugin struct {
	deploy    string
	container string
}

var csiPlugins = []rookCSIPlugin{
	{deploy: "deploy/rook-ceph.rbd.csi.ceph.com-ctrlplugin", container: "csi-rbdplugin"},
	{deploy: "deploy/rook-ceph.cephfs.csi.ceph.com-ctrlplugin", container: "csi-cephfsplugin"},
}

// csiAddonsNodeSuffixes lists the CSIAddonsNode name suffixes to wait for
// (CSIADDONS_NODES in Python). The full name is "<cluster>-rook-ceph-<suffix>".
var csiAddonsNodeSuffixes = []string{
	"daemonset-rook-ceph.rbd.csi.ceph.com-nodeplugin-csi-addons",
	"deployment-rook-ceph.rbd.csi.ceph.com-ctrlplugin",
	"deployment-rook-ceph.cephfs.csi.ceph.com-ctrlplugin",
}

// readCSIConfigCmd reads the ceph-csi config projected into the plugin pod,
// tolerating the file being absent until kubelet projects the ConfigMap.
const readCSIConfigCmd = "if [ -f /etc/ceph-csi-config/config.json ]; then cat /etc/ceph-csi-config/config.json; fi"

func init() {
	Register("rook-cluster", buildRookCluster)
}

func buildRookCluster(d Deps, cluster string, _ []string) ensure.Step {
	// Step 1: apply the rendered cluster manifest
	applyCluster := applyEmbedded("apply-rook-cluster", d, cluster, "rook-cluster.yaml")

	// Step 2: wait for cephcluster resource to be created
	waitCephClusterCreate := newApplyStep("wait-cephcluster-create", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, cluster, "rook-ceph", "create",
			rookDefaultWaitTimeout, "cephcluster/my-cluster")
	})

	// Step 3: wait for cephcluster to be Ready
	waitCephClusterReady := newApplyStep("wait-cephcluster-ready", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, cluster, "rook-ceph",
			"jsonpath={.status.phase}=Ready",
			rookClusterReadyTimeout, "cephcluster/my-cluster")
	})

	// Step 4: wait until each CSI plugin can read the cluster's ceph monitors.
	waitCSIPlugins := newApplyStep("wait-csi-plugins", func(ctx context.Context) error {
		return waitForCSIPlugins(ctx, d.K, cluster)
	})

	// Step 5: wait for each CSIAddonsNode to be created then Connected.
	var csiAddonsSteps []ensure.Step
	for _, suffix := range csiAddonsNodeSuffixes {
		node := fmt.Sprintf("%s-rook-ceph-%s", cluster, suffix)
		resource := "csiaddonsnodes.csiaddons.openshift.io/" + node
		csiAddonsSteps = append(csiAddonsSteps,
			newApplyStep("wait-csiaddon/"+node, func(ctx context.Context) error {
				return waitCSIAddonsNode(ctx, d.K, cluster, resource)
			}),
		)
	}

	steps := []ensure.Step{
		applyCluster, waitCephClusterCreate, waitCephClusterReady, waitCSIPlugins,
	}
	steps = append(steps, csiAddonsSteps...)

	return gatedAddon("addon/rook-cluster", d.Opts, rookClusterReady(d.K, cluster), steps...)
}

// waitForCSIPlugins polls each CSI plugin's ceph-csi config until it lists
// monitors, mirroring Python's wait_for_csi_plugins. Each plugin gets its own
// deadline (CSI_PLUGIN_TIMEOUT), matching Python's per-plugin loop.
func waitForCSIPlugins(ctx context.Context, k *cli.Kubectl, cluster string) error {
	for _, plugin := range csiPlugins {
		deadline := time.Now().Add(rookCSIPluginTimeout)
		for {
			if csiPluginHasMonitors(ctx, k, cluster, plugin) {
				break
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("timeout waiting for ceph monitors in %q", plugin.deploy)
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(rookCSIPluginPoll):
			}
		}
	}
	return nil
}

// csiPluginHasMonitors reports whether the plugin's ceph-csi config lists at
// least one monitor. Any read/parse failure reports false so the caller keeps
// polling, matching Python's tolerant _csi_plugin_has_monitors.
func csiPluginHasMonitors(ctx context.Context, k *cli.Kubectl, cluster string, plugin rookCSIPlugin) bool {
	out, err := k.ExecContainer(ctx, cluster, "rook-ceph", plugin.deploy, plugin.container,
		"sh", "-c", readCSIConfigCmd)
	if err != nil {
		return false
	}
	return len(cephMonitors(out)) > 0
}

// cephMonitors extracts the monitor list from a ceph-csi config.json payload,
// returning nil for empty/malformed input (mirrors Python's _monitors).
func cephMonitors(data string) []string {
	data = strings.TrimSpace(data)
	if data == "" {
		return nil
	}
	var cfg []struct {
		Monitors []string `json:"monitors"`
	}
	if err := json.Unmarshal([]byte(data), &cfg); err != nil || len(cfg) == 0 {
		return nil
	}
	return cfg[0].Monitors
}

// waitCSIAddonsNode waits for a CSIAddonsNode to be created, then for its
// status.state to become Connected, retrying the Connected wait within a shared
// deadline. The csi-addons sidecar can delete+recreate the node when the driver
// pod restarts, so a single wait may hit NotFound; Python retries the same way.
func waitCSIAddonsNode(ctx context.Context, k *cli.Kubectl, cluster, resource string) error {
	deadline := time.Now().Add(rookCSIAddonsNodeTimeout)

	var lastErr error
	for attempt := 1; attempt <= rookCSIAddonsAttempts; attempt++ {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			if lastErr != nil {
				return lastErr
			}
			return fmt.Errorf("timeout waiting for %s", resource)
		}

		if err := k.WaitFor(ctx, cluster, "rook-ceph", "create", remaining, resource); err != nil {
			lastErr = err
			continue
		}

		remaining = time.Until(deadline)
		if remaining <= 0 {
			return fmt.Errorf("timeout waiting for %s", resource)
		}

		if err := k.WaitFor(ctx, cluster, "rook-ceph",
			"jsonpath={.status.state}=Connected", remaining, resource); err != nil {
			lastErr = err
			continue
		}
		return nil
	}
	return lastErr
}

// rookClusterReady is satisfied when the CephCluster is Ready, both CSI
// ctrlplugin deployments are Available, and every CSIAddonsNode is Connected —
// a conservative proxy for the full end-state the steps wait for.
func rookClusterReady(k *cli.Kubectl, cluster string) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		if !cephPhaseReady(ctx, k, cluster, "rook-ceph", "cephcluster/my-cluster") {
			return false, nil
		}
		for _, p := range csiPlugins {
			// p.deploy is "deploy/<name>"; deploymentAvailable takes the bare name.
			name := strings.TrimPrefix(p.deploy, "deploy/")
			if !deploymentAvailable(ctx, k, cluster, "rook-ceph", name) {
				return false, nil
			}
		}
		for _, suffix := range csiAddonsNodeSuffixes {
			node := fmt.Sprintf("%s-rook-ceph-%s", cluster, suffix)
			res := "csiaddonsnodes.csiaddons.openshift.io/" + node
			if !jsonPathEquals(ctx, k, cluster, "rook-ceph", res, "{.status.state}", "Connected") {
				return false, nil
			}
		}
		return true, nil
	}
}
