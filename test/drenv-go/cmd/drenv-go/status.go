// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

func newStatusCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "status",
		Short: "Print the lifecycle status of every cluster in the environment",
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			prov := newMinikubeProvider()

			// Print cluster status for each profile.
			for _, prof := range env.Profiles {
				st, err := prov.Status(cmd.Context(), prof.Name)
				if err != nil {
					fmt.Fprintf(cmd.OutOrStdout(), "cluster/%s: error: %v\n", prof.Name, err)
					continue
				}
				fmt.Fprintf(cmd.OutOrStdout(), "cluster/%s: %s\n", prof.Name, st)
			}

			// Print the full env tree (profiles → workers → addons) as a
			// read-only structural view of what would be applied on start.
			fmt.Fprintln(cmd.OutOrStdout())
			fmt.Fprintln(cmd.OutOrStdout(), "Environment tree:")
			fmt.Fprint(cmd.OutOrStdout(), envfile.Tree(env))
			return nil
		},
	}
}
