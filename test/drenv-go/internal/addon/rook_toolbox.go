// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rook-toolbox deploys the Rook Ceph toolbox and waits until it is ready,
// mirroring the Python addons/rook/toolbox/__init__.py start() function.
//
// Steps (serial):
//  1. kubectl apply -k <AddonsDir>/rook/toolbox
//  2. kubectl rollout status rook-ceph deploy/rook-ceph-tools (300s)
//  3. kubectl exec deploy/rook-ceph-tools -n rook-ceph -- ceph status (logged)

package addon

import (
	"context"
	"log"
	"path/filepath"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

func init() {
	Register("rook-toolbox", buildRookToolbox)
}

func buildRookToolbox(d Deps, cluster string, _ []string) ensure.Step {
	toolboxDir := filepath.Join(d.AddonsDir, "rook", "toolbox")

	applyToolbox := newApplyStep("apply-rook-toolbox", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, toolboxDir)
	})

	waitRollout := newApplyStep("wait-rook-toolbox-rollout", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "rook-ceph", "deploy/rook-ceph-tools", rookCSIRolloutTimeout)
	})

	// Run `ceph status` via kubectl exec and log the output. Done=false always
	// so the exec runs every time (matches Python which always prints status).
	cephStatus := newApplyStep("ceph-status", func(ctx context.Context) error {
		out, err := d.K.KubectlExec(ctx, cluster, "rook-ceph", "deploy/rook-ceph-tools", "ceph", "status")
		if err != nil {
			return err
		}
		log.Printf("ceph status (%s):\n%s", cluster, out)
		return nil
	})

	return Serial("addon/rook-toolbox", d.Opts,
		applyToolbox, waitRollout, cephStatus,
	)
}
