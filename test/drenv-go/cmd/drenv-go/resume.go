// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package main — resume command.
//
// Design note: resume does NOT use the ensure model. minikube unpause is a
// one-shot operation that lifts the cgroup freeze applied by pause; there is
// no stable "already unpaused" status distinct from "running" in
// `minikube status`, so there is no meaningful Done pre-check. Plain
// iteration over profiles with error aggregation is the right approach here.
package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

func newResumeCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "resume",
		Short: "Unpause all clusters in the environment",
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			prov := newMinikubeProvider()
			ctx := cmd.Context()

			var errs []error
			for _, prof := range env.Profiles {
				fmt.Fprintf(cmd.OutOrStdout(), "resuming %s\n", prof.Name)
				if err := prov.Resume(ctx, prof.Name); err != nil {
					errs = append(errs, fmt.Errorf("profile %s: %w", prof.Name, err))
				}
			}
			return errors.Join(errs...)
		},
	}
}
