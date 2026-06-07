// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

func newDeleteCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "delete",
		Short: "Ensure all clusters in the environment are absent",
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			opts := ensure.DefaultOptions()
			opts.Reporter = ensure.ConsoleReporter{W: cmd.OutOrStdout()}

			step := build.Delete(env, newProviderSelector(), opts)
			_, err = ensure.Ensure(cmd.Context(), step, opts)
			return err
		},
	}
}
