// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

// addOnlyFlag registers the repeatable, comma-separated --only selector flag.
func addOnlyFlag(cmd *cobra.Command, only *[]string) {
	cmd.Flags().StringArrayVar(only, "only", nil,
		"limit to selected addons: NAME, NAME@PROFILE or NAME@global "+
			"(repeatable, comma-separated); keeps the clusters they need")
}

// newPlan builds the start plan for env, reduced to the --only selectors.
func newPlan(env *envfile.Env, sel build.ProviderSelector, deps addon.Deps, opts ensure.Options,
	only []string,
) (*build.Plan, error) {
	sels, err := build.ParseSelectors(only)
	if err != nil {
		return nil, err
	}
	return build.NewPlan(env, sel, deps, opts).Only(sels)
}
