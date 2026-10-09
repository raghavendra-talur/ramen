// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/report"
)

func newCheckCommand() *cobra.Command {
	var asJSON bool
	var addonsDir string
	var only []string
	o := report.DefaultCheckOptions()

	cmd := &cobra.Command{
		Use:   "check",
		Short: "Report the readiness of every cluster and addon without changing anything",
		Long: `Build the same step tree as 'start' and evaluate each cluster and addon
step's readiness probe (Done) without ever acting (Do). Addons on a cluster
that is not running are skipped rather than probed. The command exits 0
whenever the report is produced, regardless of readiness.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			r := runnerFor(asJSON)
			opts := ensure.DefaultOptions()
			plan, err := newPlan(env, newProviderSelectorWith(r, ""), newDeps(r, env, addonsDir, opts), opts, only)
			if err != nil {
				return err
			}

			c := report.BuildCheck(cmd.Context(), plan, o)
			if asJSON {
				return writeJSON(cmd.OutOrStdout(), c)
			}
			return report.WriteCheckText(cmd.OutOrStdout(), c)
		},
	}

	cmd.Flags().BoolVar(&asJSON, "json", false, "print a JSON document instead of text")
	cmd.Flags().StringVar(&addonsDir, "addons-dir", "",
		"path to the addons directory (default: <envfile-dir>/../drenv/addons)")
	cmd.Flags().DurationVar(&o.Timeout, "timeout", o.Timeout, "timeout for each readiness probe")
	cmd.Flags().IntVar(&o.Parallel, "parallel", o.Parallel, "maximum profiles (and probes per profile) checked concurrently")
	addOnlyFlag(cmd, &only)
	return cmd
}
