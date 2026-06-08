// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// external-snapshotter installs the Kubernetes external-snapshotter CRDs and
// snapshot-controller, mirroring the Python addons/external_snapshotter/start.py.
//
// Steps (serial):
//  1. apply -k <AddonsDir>/external_snapshotter/start-data/crds
//  2. kubectl wait --for=condition=established crd --all  (all-CRDs established wait)
//  3. apply -k <AddonsDir>/external_snapshotter/start-data/controller
//  4. rollout status kube-system deploy/snapshot-controller

import (
	"context"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	externalSnapshotterRolloutTimeout = 5 * time.Minute
	externalSnapshotterWaitTimeout    = 5 * time.Minute
)

func init() {
	Register("external-snapshotter", buildExternalSnapshotter)
}

func buildExternalSnapshotter(d Deps, cluster string, _ []string) ensure.Step {
	crdsDir := filepath.Join(d.AddonsDir, "external_snapshotter", "start-data", "crds")
	controllerDir := filepath.Join(d.AddonsDir, "external_snapshotter", "start-data", "controller")

	applyCRDs := newApplyStep("apply-crds", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, crdsDir)
	})

	waitCRDs := newApplyStep("wait-crds-established", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, cluster, "", "condition=established",
			externalSnapshotterWaitTimeout, "crd", "--all")
	})

	applyController := newApplyStep("apply-controller", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, controllerDir)
	})

	waitController := newApplyStep("wait-snapshot-controller", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "kube-system", "deploy/snapshot-controller",
			externalSnapshotterRolloutTimeout)
	})

	return gatedAddon("addon/external-snapshotter", d.Opts,
		gateDeploymentAvailable(d.K, cluster, "kube-system", "snapshot-controller"),
		applyCRDs, waitCRDs, applyController, waitController,
	)
}
