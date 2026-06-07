// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

func newDumpCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "dump",
		Short: "Print the expanded environment as YAML",
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			out, err := yaml.Marshal(env)
			if err != nil {
				return fmt.Errorf("marshal env: %w", err)
			}
			fmt.Fprint(cmd.OutOrStdout(), string(out))
			return nil
		},
	}
}
