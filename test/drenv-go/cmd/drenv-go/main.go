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
	root := newRootCommand()
	root.AddCommand(newAddonsCommand())
	root.AddCommand(newStatusCommand())
	root.AddCommand(newCheckCommand())
	root.AddCommand(newStartCommand())
	root.AddCommand(newStopCommand())
	root.AddCommand(newDeleteCommand())
	root.AddCommand(newLoadCommand())
	root.AddCommand(newSuspendCommand())
	root.AddCommand(newResumeCommand())
	root.AddCommand(newDumpCommand())
	root.AddCommand(newGatherCommand())

	// cobra already prints the error (and usage) to stderr; just set the exit code.
	if err := root.Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCommand() *cobra.Command {
	root := &cobra.Command{
		Use:   "drenv-go",
		Short: "Ensure-model test environment manager (parallel Go rewrite of drenv)",
		// RunE enables full help output (including flags) when no subcommand is given.
		RunE: func(cmd *cobra.Command, args []string) error {
			return cmd.Help()
		},
		// Flags and args are valid by now, so a later error is a runtime
		// failure: print the error alone, not the usage that buries it.
		PersistentPreRun: func(cmd *cobra.Command, args []string) {
			cmd.SilenceUsage = true
		},
	}
	// --envfile is required per command (via loadEnv), not globally: commands
	// such as `addons` do not read an environment.
	root.PersistentFlags().StringVar(&envfilePath, "envfile", "", "path to the environment file")

	return root
}
