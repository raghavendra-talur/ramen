// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// olm installs the Operator Lifecycle Manager, mirroring the Python
// addons/olm/start.py.
//
// Steps (serial):
//  1. apply --server-side=true -k <AddonsDir>/olm/start-data/crds
//     (server-side apply avoids annotation-too-long errors on large CRDs)
//  2. kubectl wait --for=condition=established crd --all
//  3. apply -k <AddonsDir>/olm/start-data/operators
//  4. rollout status olm deploy/olm-operator
//  5. rollout status olm deploy/catalog-operator
//  6. kubectl wait csv/packageserver --for=create -n olm
//  7. kubectl wait csv/packageserver --for=jsonpath={.status.phase}=Succeeded -n olm
//  8. rollout status olm deploy/packageserver

import (
	"context"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	olmWaitTimeout    = 10 * time.Minute
	olmRolloutTimeout = 10 * time.Minute
)

func init() {
	Register("olm", buildOLM)
}

func buildOLM(d Deps, cluster string, _ []string) ensure.Step {
	crdsDir := filepath.Join(d.AddonsDir, "olm", "start-data", "crds")
	operatorsDir := filepath.Join(d.AddonsDir, "olm", "start-data", "operators")

	applyCRDs := newApplyStep("apply-crds-server-side", func(ctx context.Context) error {
		return d.K.ApplyServerSideKustomizeDir(ctx, cluster, crdsDir)
	})

	waitCRDs := newApplyStep("wait-crds-established", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, cluster, "", "condition=established",
			olmWaitTimeout, "crd", "--all")
	})

	applyOperators := newApplyStep("apply-operators", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, operatorsDir)
	})

	waitOLMOperator := newApplyStep("wait-olm-operator", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "olm", "deploy/olm-operator", olmRolloutTimeout)
	})

	waitCatalogOperator := newApplyStep("wait-catalog-operator", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "olm", "deploy/catalog-operator", olmRolloutTimeout)
	})

	waitPackageServerCreate := newApplyStep("wait-packageserver-create", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, cluster, "olm", "create", olmWaitTimeout, "csv/packageserver")
	})

	waitPackageServerSucceeded := newApplyStep("wait-packageserver-succeeded", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, cluster, "olm",
			"jsonpath={.status.phase}=Succeeded",
			olmWaitTimeout, "csv/packageserver")
	})

	waitPackageServerRollout := newApplyStep("wait-packageserver-rollout", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "olm", "deploy/packageserver", olmRolloutTimeout)
	})

	// Gate on the packageserver Deployment (the terminal component): if it is
	// Available, olm-operator and catalog-operator are necessarily up too.
	return gatedAddon("addon/olm", d.Opts,
		gateDeploymentAvailable(d.K, cluster, "olm", "packageserver"),
		applyCRDs,
		waitCRDs,
		applyOperators,
		waitOLMOperator,
		waitCatalogOperator,
		waitPackageServerCreate,
		waitPackageServerSucceeded,
		waitPackageServerRollout,
	)
}
