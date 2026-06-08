// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rook-operator deploys the Rook Ceph operator and waits until it is ready,
// mirroring the Python addons/rook/operator/__init__.py start() function.
//
// Steps (serial):
//  1. kubectl apply -k <AddonsDir>/rook/operator
//  2. kubectl rollout status rook-ceph deploy/rook-ceph-operator (600s)
//  3. kubectl wait pod --selector=app=rook-ceph-operator
//     --for=jsonpath={.status.phase}=Running -n rook-ceph

package addon

import (
	"context"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	rookOperatorRolloutTimeout = 600 * time.Second
	rookDefaultWaitTimeout     = 300 * time.Second // kubectl.py _DEFAULT_TIMEOUT
)

func init() {
	Register("rook-operator", buildRookOperator)
}

func buildRookOperator(d Deps, cluster string, _ []string) ensure.Step {
	operatorDir := filepath.Join(d.AddonsDir, "rook", "operator")

	applyOperator := newApplyStep("apply-rook-operator", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, operatorDir)
	})

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
		applyOperator, waitRollout, waitRunning,
	)
}
