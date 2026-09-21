// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rook-operator deploys the Rook Ceph operator and waits until it is ready,
// mirroring the Python addons/rook/operator/__init__.py start() function.
//
// The Python addon applies two separate kustomizations under start-data/ (the
// operator/ directory itself has no kustomization.yaml):
//
//	deploy_deps():
//	 1. kubectl apply -k <AddonsDir>/rook/operator/start-data/deps
//	 2. kubectl wait crd/<csi-crd> --for=condition=established  (per CSI CRD)
//	 3. kubectl rollout status deploy/ceph-csi-controller-manager -n rook-ceph
//	deploy_operator():
//	 4. kubectl apply -k <AddonsDir>/rook/operator/start-data/operator
//	 5. kubectl rollout status deploy/rook-ceph-operator -n rook-ceph (600s)
//	 6. kubectl wait pod --selector=app=rook-ceph-operator
//	    --for=jsonpath={.status.phase}=Running -n rook-ceph
//
// operator.yaml includes OperatorConfig and Driver CRs whose CRDs ship in the
// deps (csi-operator.yaml), so the CSI CRDs must be established before the
// second apply.

package addon

import (
	"context"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	rookOperatorRolloutTimeout = 600 * time.Second
	rookDefaultWaitTimeout     = 300 * time.Second // kubectl.py _DEFAULT_TIMEOUT
)

// csiCRDs must be established before the operator kustomization is applied,
// mirroring CSI_CRDS in the Python addon.
var csiCRDs = []string{
	"operatorconfigs.csi.ceph.io",
	"drivers.csi.ceph.io",
}

func init() {
	Register("rook-operator", buildRookOperator)
}

func buildRookOperator(d Deps, cluster string, _ []string) ensure.Step {
	applyDeps := applyEmbedded("apply-rook-operator-deps", d, cluster, "rook-operator-deps.yaml")

	waitCSICRDs := newApplyStep("wait-csi-crds-established", func(ctx context.Context) error {
		for _, crd := range csiCRDs {
			if err := d.K.WaitFor(ctx, cluster, "", "condition=established",
				rookDefaultWaitTimeout, "crd/"+crd); err != nil {
				return err
			}
		}
		return nil
	})

	waitCSIRollout := newApplyStep("wait-ceph-csi-controller-rollout", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "rook-ceph", "deploy/ceph-csi-controller-manager", rookDefaultWaitTimeout)
	})

	applyOperator := applyEmbedded("apply-rook-operator", d, cluster, "rook-operator.yaml")

	waitRollout := newApplyStep("wait-rook-operator-rollout", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "rook-ceph", "deploy/rook-ceph-operator", rookOperatorRolloutTimeout)
	})

	waitRunning := newApplyStep("wait-rook-operator-running", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, cluster, "rook-ceph",
			"jsonpath={.status.phase}=Running",
			rookDefaultWaitTimeout, // Python default 300s
			"pod", "--selector=app=rook-ceph-operator",
		)
	})

	return gatedAddon("addon/rook-operator", d.Opts,
		gateDeploymentAvailable(d.K, cluster, "rook-ceph", "rook-ceph-operator"),
		applyDeps, waitCSICRDs, waitCSIRollout, applyOperator, waitRollout, waitRunning,
	)
}
