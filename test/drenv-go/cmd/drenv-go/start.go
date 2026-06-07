// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/build"
	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// newMinikubeProvider returns a MinikubeProvider backed by the real minikube CLI.
func newMinikubeProvider() provider.MinikubeProvider {
	return provider.MinikubeProvider{MK: &cli.Minikube{R: cli.Exec{}}}
}

// loadEnv loads the environment from envfilePath, returning an error if the
// path is empty or the file cannot be parsed.
func loadEnv() (*envfile.Env, error) {
	if envfilePath == "" {
		return nil, fmt.Errorf("--envfile is required")
	}
	return envfile.Load(envfilePath)
}

func newStartCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "start",
		Short: "Ensure all clusters in the environment are running",
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			prov := newMinikubeProvider()
			opts := ensure.DefaultOptions()
			opts.Reporter = ensure.ConsoleReporter{W: os.Stdout}

			step := build.Start(env, prov, opts)
			_, err = ensure.Ensure(cmd.Context(), step, opts)
			return err
		},
	}
}
