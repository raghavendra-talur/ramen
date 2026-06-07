// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package main — gather command.
//
// This command dumps the state of every cluster in the environment to a
// directory using `kubectl cluster-info dump`. It is the Go equivalent of the
// Python `drenv gather` operation (test/drenv/__main__.py do_gather).
//
// The Python implementation uses the kubectl-gather plugin; drenv-go uses the
// built-in `kubectl cluster-info dump` which requires no extra plugin while
// providing equivalent coverage (all namespaces, YAML output).
package main

import (
	"errors"
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func newGatherCommand() *cobra.Command {
	var directory string

	cmd := &cobra.Command{
		Use:   "gather",
		Short: "Gather cluster state into a directory for offline analysis",
		Long: `Dump the state of every cluster in the environment using kubectl cluster-info dump.

For each profile, runs:

    kubectl --context <profile> cluster-info dump \
        --output-directory=<dir>/<profile> \
        --all-namespaces \
        --output=yaml

Equivalent to the Python 'drenv gather' command.`,
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			dir := directory
			if dir == "" {
				dir = "gather." + env.Name
			}

			k := &cli.Kubectl{R: cli.Exec{}}
			ctx := cmd.Context()

			var errs []error
			for _, prof := range env.Profiles {
				outDir := filepath.Join(dir, prof.Name)
				fmt.Fprintf(cmd.OutOrStdout(), "gathering %s → %s\n", prof.Name, outDir)
				if err := k.ClusterInfoDump(ctx, prof.Name, outDir); err != nil {
					errs = append(errs, fmt.Errorf("profile %s: %w", prof.Name, err))
				}
			}
			return errors.Join(errs...)
		},
	}

	cmd.Flags().StringVarP(&directory, "directory", "d", "",
		`directory for gathered data (default "gather.<envname>")`)
	return cmd
}
