// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// recipe installs the Recipe CRD, mirroring the Python addons/recipe/start.py.
//
// Steps (serial):
//  1. apply -k <AddonsDir>/recipe/start-data

import (
	"context"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

func init() {
	Register("recipe", buildRecipe)
}

func buildRecipe(d Deps, cluster string, _ []string) ensure.Step {
	apply := applyEmbedded("apply", d, cluster, "recipe.yaml")

	// Gate on the Recipe CRD being Established: without a gate an apply step
	// is never done, so check could not report recipe ready.
	return gatedAddon("addon/recipe", d.Opts, gateRecipeCRDEstablished(d.K, cluster), apply)
}

func gateRecipeCRDEstablished(k *cli.Kubectl, cluster string) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		return jsonPathEquals(ctx, k, cluster, "", "crd/recipes.ramendr.openshift.io",
			`{.status.conditions[?(@.type=="Established")].status}`, "True"), nil
	}
}
