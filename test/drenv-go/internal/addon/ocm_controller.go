// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// ocm-controller deploys the OCM controller, mirroring the Python
// addons/ocm/controller/start.py.
//
// The addon is registered under the name "ocm-controller" to match the addon
// name used in the environment files.
//
// Steps (serial):
//  1. apply -k <AddonsDir>/ocm/controller/start-data
//  2. rollout status open-cluster-management deploy/ocm-controller

import (
	"context"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const ocmControllerRolloutTimeout = 5 * time.Minute

func init() {
	Register("ocm-controller", buildOCMController)
}

func buildOCMController(d Deps, cluster string, _ []string) ensure.Step {
	startData := filepath.Join(d.AddonsDir, "ocm", "controller", "start-data")

	apply := newApplyStep("apply", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, startData)
	})

	waitRollout := newApplyStep("wait-ocm-controller", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "open-cluster-management",
			"deploy/ocm-controller", ocmControllerRolloutTimeout)
	})

	return gatedAddon("addon/ocm-controller", d.Opts,
		gateDeploymentAvailable(d.K, cluster, "open-cluster-management", "ocm-controller"),
		apply, waitRollout)
}
