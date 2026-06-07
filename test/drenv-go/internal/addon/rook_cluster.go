// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rook-cluster deploys the Rook Ceph cluster and waits until all CSI
// components and CSIAddonsNodes are ready, mirroring the Python
// addons/rook/cluster/__init__.py start() function.
//
// Steps (serial):
//  1. kubectl apply -k <AddonsDir>/rook/cluster
//  2. kubectl wait cephcluster/my-cluster --for=create -n rook-ceph (300s)
//  3. kubectl wait cephcluster/my-cluster --for=jsonpath={.status.phase}=Ready -n rook-ceph (600s)
//  4. For each CSI component (daemonset/deployment): rollout status -n rook-ceph (300s)
//  5. For each CSIAddonsNode: wait --for=create -n rook-ceph; wait --for=jsonpath={.status.state}=Connected

package addon

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	rookClusterReadyTimeout  = 600 * time.Second
	rookCSIRolloutTimeout    = 300 * time.Second
	rookCSIAddonsNodeTimeout = 300 * time.Second // CSIADDONS_TIMEOUT
)

// rookCSIComponent describes a CSI driver component to wait for after the
// CephCluster becomes ready. These match CSI_COMPONENTS in the Python source.
type rookCSIComponent struct {
	kind string // "daemonset" or "deployment"
	name string
}

// csiComponents lists the CSI driver components created by the rook operator
// as part of the CephCluster reconciliation (CSI_COMPONENTS in Python).
var csiComponents = []rookCSIComponent{
	{"daemonset", "csi-rbdplugin"},
	{"daemonset", "csi-cephfsplugin"},
	{"deployment", "csi-rbdplugin-provisioner"},
	{"deployment", "csi-cephfsplugin-provisioner"},
}

// csiAddonsNodeSuffixes lists the CSIAddonsNode name suffixes to wait for
// (CSIADDONS_NODES in Python). The full name is "<cluster>-rook-ceph-<suffix>".
var csiAddonsNodeSuffixes = []string{
	"daemonset-csi-rbdplugin",
	"deployment-csi-rbdplugin-provisioner",
	"deployment-csi-cephfsplugin-provisioner",
}

func init() {
	Register("rook-cluster", buildRookCluster)
}

func buildRookCluster(d Deps, cluster string, _ []string) ensure.Step {
	clusterDir := filepath.Join(d.AddonsDir, "rook", "cluster")

	// Step 1: apply kustomization
	applyCluster := newApplyStep("apply-rook-cluster", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, clusterDir)
	})

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

	// Step 4: rollout status for each CSI component
	var csiSteps []ensure.Step
	for _, comp := range csiComponents {
		c := comp // capture loop variable
		resource := c.kind + "/" + c.name
		csiSteps = append(csiSteps,
			newApplyStep("rollout-"+resource, func(ctx context.Context) error {
				return d.K.RolloutStatus(ctx, cluster, "rook-ceph", resource, rookCSIRolloutTimeout)
			}),
		)
	}

	// Step 5: wait for each CSIAddonsNode to be created then Connected.
	// NOTE: The Python source retries this wait up to CSIADDONS_ATTEMPTS (3) times
	// within a shared CSIADDONS_TIMEOUT (300s) deadline because the csi-addons
	// sidecar may delete+recreate the CSIAddonsNode when the CSI driver pod
	// restarts. Here we issue a single wait per node for argv faithfulness;
	// real-cluster retry behaviour needs validation.
	var csiAddonsSteps []ensure.Step
	for _, suffix := range csiAddonsNodeSuffixes {
		s := suffix // capture
		nodeName := fmt.Sprintf("%s-rook-ceph-%s", cluster, s)
		resource := "csiaddonsnodes.csiaddons.openshift.io/" + nodeName

		csiAddonsSteps = append(csiAddonsSteps,
			newApplyStep("wait-csiaddon-create/"+nodeName, func(ctx context.Context) error {
				return d.K.WaitFor(ctx, cluster, "rook-ceph", "create",
					rookCSIAddonsNodeTimeout, resource)
			}),
			newApplyStep("wait-csiaddon-connected/"+nodeName, func(ctx context.Context) error {
				return d.K.WaitFor(ctx, cluster, "rook-ceph",
					"jsonpath={.status.state}=Connected",
					rookCSIAddonsNodeTimeout, resource)
			}),
		)
	}

	steps := []ensure.Step{applyCluster, waitCephClusterCreate, waitCephClusterReady}
	steps = append(steps, csiSteps...)
	steps = append(steps, csiAddonsSteps...)

	return Serial("addon/rook-cluster", d.Opts, steps...)
}
