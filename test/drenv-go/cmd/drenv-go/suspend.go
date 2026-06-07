// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package main — suspend command.
//
// Design note: suspend does NOT use the ensure model. minikube pause is a
// one-shot operation (it freezes cgroup processes); there is no stable
// "already paused" status exposed by `minikube status`, so there is no
// meaningful Done pre-check. Plain iteration over profiles with error
// aggregation is the right approach here.
package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

func newSuspendCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "suspend",
		Short: "Pause all clusters in the environment",
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			sel := newProviderSelector()
			ctx := cmd.Context()

			var errs []error
			for _, prof := range env.Profiles {
				fmt.Fprintf(cmd.OutOrStdout(), "suspending %s\n", prof.Name)
				if err := sel(prof).Suspend(ctx, prof.Name); err != nil {
					errs = append(errs, fmt.Errorf("profile %s: %w", prof.Name, err))
				}
			}
			return errors.Join(errs...)
		},
	}
}
