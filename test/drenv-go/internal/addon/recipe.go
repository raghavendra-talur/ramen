// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// recipe installs the Recipe CRD, mirroring the Python addons/recipe/start.py.
//
// Steps (serial):
//  1. apply -k <AddonsDir>/recipe/start-data

import (
	"context"
	"path/filepath"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

func init() {
	Register("recipe", buildRecipe)
}

func buildRecipe(d Deps, cluster string, _ []string) ensure.Step {
	startData := filepath.Join(d.AddonsDir, "recipe", "start-data")

	apply := newApplyStep("apply", func(ctx context.Context) error {
		return d.K.ApplyKustomizeDir(ctx, cluster, startData)
	})

	return Serial("addon/recipe", d.Opts, apply)
}
