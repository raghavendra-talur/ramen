// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// csi-addons deploys the CSI-Addons controller, mirroring the Python
// addons/csi_addons/start.py.
//
// Steps (serial):
//  1. apply -k <AddonsDir>/csi_addons/start-data
//  2. rollout status csi-addons-system deployment/csi-addons-controller-manager

import (
	"context"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const csiAddonsRolloutTimeout = 5 * time.Minute

func init() {
	Register("csi-addons", buildCSIAddons)
}

func buildCSIAddons(d Deps, cluster string, _ []string) ensure.Step {
	startData := filepath.Join(d.AddonsDir, "csi_addons", "start-data")

	apply := newApplyStep("apply", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, startData)
	})

	waitRollout := newApplyStep("wait-controller-manager", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "csi-addons-system",
			"deployment/csi-addons-controller-manager", csiAddonsRolloutTimeout)
	})

	return Serial("addon/csi-addons", d.Opts, apply, waitRollout)
}
