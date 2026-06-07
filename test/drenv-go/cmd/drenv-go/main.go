// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
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
	root.AddCommand(newStatusCommand())
	root.AddCommand(newStartCommand())
	root.AddCommand(newStopCommand())
	root.AddCommand(newDeleteCommand())
	root.AddCommand(newLoadCommand())
	root.AddCommand(newSuspendCommand())
	root.AddCommand(newResumeCommand())
	root.AddCommand(newDumpCommand())

	// cobra already prints the error (and usage) to stderr; just set the exit code.
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}
