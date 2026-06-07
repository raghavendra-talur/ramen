// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package main — load command.
//
// Design note: load does NOT use the ensure model. Loading an image into a
// cluster is not idempotent in a meaningful way: there is no cheap "is this
// image already loaded?" pre-check that avoids a potentially expensive
// network transfer, and minikube itself does not expose one. A plain iteration
// over profiles with error aggregation is cleaner and honest here.
package main

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"
)

func newLoadCommand() *cobra.Command {
	var image string

	cmd := &cobra.Command{
		Use:   "load",
		Short: "Load a container image into all clusters in the environment",
		RunE: func(cmd *cobra.Command, args []string) error {
			if image == "" {
				return fmt.Errorf("--image is required")
			}

			env, err := loadEnv()
			if err != nil {
				return err
			}

			prov := newMinikubeProvider()
			ctx := cmd.Context()

			var errs []error
			for _, prof := range env.Profiles {
				fmt.Fprintf(cmd.OutOrStdout(), "loading image %s into %s\n", image, prof.Name)
				if err := prov.LoadImage(ctx, prof.Name, image); err != nil {
					errs = append(errs, fmt.Errorf("profile %s: %w", prof.Name, err))
				}
			}
			return errors.Join(errs...)
		},
	}

	cmd.Flags().StringVar(&image, "image", "", "container image reference to load (required)")
	return cmd
}
