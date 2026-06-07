// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"

	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

func newStopCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "stop",
		Short: "Ensure all clusters in the environment are stopped",
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			prov := newMinikubeProvider()
			opts := ensure.DefaultOptions()
			opts.Reporter = ensure.ConsoleReporter{W: os.Stdout}

			step := build.Stop(env, prov, opts)
			_, err = ensure.Ensure(cmd.Context(), step, opts)
			return err
		},
	}
}
