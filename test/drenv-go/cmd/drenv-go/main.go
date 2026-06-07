// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// envfilePath is bound to the persistent --envfile flag and consumed by subcommands.
var envfilePath string

func main() {
	root := &cobra.Command{
		Use:   "drenv-go",
		Short: "Ensure-model test environment manager (parallel Go rewrite of drenv)",
		// RunE enables full help output (including flags) when no subcommand is given.
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
	}
	root.PersistentFlags().StringVar(&envfilePath, "envfile", "", "path to the environment file")

	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
