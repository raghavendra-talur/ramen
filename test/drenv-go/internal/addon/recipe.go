// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// recipe installs the Recipe CRD, mirroring the Python addons/recipe/start.py.
//
// Steps (serial):
//  1. apply -k <AddonsDir>/recipe/start-data

import (
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

func init() {
	Register("recipe", buildRecipe)
}

func buildRecipe(d Deps, cluster string, _ []string) ensure.Step {
	apply := applyEmbedded("apply", d, cluster, "recipe.yaml")

	return Serial("addon/recipe", d.Opts, apply)
}
