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
		Short: "Parse the envfile and print its tree",
		RunE: func(cmd *cobra.Command, args []string) error {
			if envfilePath == "" {
				return fmt.Errorf("--envfile is required")
			}
			env, err := envfile.Load(envfilePath)
			if err != nil {
				return err
			}
			fmt.Print(envfile.Tree(env))
			return nil
		},
	}
}
