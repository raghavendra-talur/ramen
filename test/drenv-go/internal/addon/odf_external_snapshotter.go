// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// odf-external-snapshotter installs the ODF variant of the external-snapshotter
// CRDs, mirroring the Python addons/odf_external_snapshotter/start.py.
//
// Steps (serial):
//  1. apply -k <AddonsDir>/odf_external_snapshotter/start-data/crds
//  2. kubectl wait --for=condition=established crd --all

import (
	"context"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const odfExternalSnapshotterWaitTimeout = 5 * time.Minute

func init() {
	Register("odf-external-snapshotter", buildODFExternalSnapshotter)
}

func buildODFExternalSnapshotter(d Deps, cluster string, _ []string) ensure.Step {
	crdsDir := filepath.Join(d.AddonsDir, "odf_external_snapshotter", "start-data", "crds")

	applyCRDs := newApplyStep("apply-crds", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, crdsDir)
	})

	waitCRDs := newApplyStep("wait-crds-established", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, cluster, "", "condition=established",
			odfExternalSnapshotterWaitTimeout, "crd", "--all")
	})

	return Serial("addon/odf-external-snapshotter", d.Opts,
		applyCRDs, waitCRDs,
	)
}
