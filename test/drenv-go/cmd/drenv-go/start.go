// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
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

// defaultAddonsDir returns the path to test/drenv/addons relative to the
// envfile location, following the repository layout:
//
//	test/envs/<env>.yaml  →  test/drenv/addons
func defaultAddonsDir() string {
	if envfilePath == "" {
		return ""
	}
	// envfile is typically at test/envs/<name>.yaml; addons live at
	// test/drenv/addons — two levels up from the envfile, then drenv/addons.
	envDir := filepath.Dir(envfilePath)
	return filepath.Join(envDir, "..", "drenv", "addons")
}

func newStartCommand() *cobra.Command {
	var addonsDir string

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Ensure all clusters in the environment are running",
		RunE: func(cmd *cobra.Command, args []string) error {
			env, err := loadEnv()
			if err != nil {
				return err
			}

			prov := newMinikubeProvider()
			opts := ensure.DefaultOptions()
			opts.Reporter = ensure.ConsoleReporter{W: cmd.OutOrStdout()}

			dir := addonsDir
			if dir == "" {
				dir = defaultAddonsDir()
			}

			r := cli.Exec{}
			deps := addon.Deps{
				K:          &cli.Kubectl{R: r},
				MK:         &cli.Minikube{R: r},
				Helm:       &cli.Helm{R: r},
				Clusteradm: &cli.Clusteradm{R: r},
				Subctl:     &cli.Subctl{R: r},
				MC:         &cli.MC{R: r},
				Velero:     &cli.Velero{R: r},
				Argocd:     &cli.Argocd{R: r},
				AddonsDir:  dir,
				EnvName:    env.Name,
				Opts:       opts,
			}

			step := build.Start(env, prov, deps, opts)
			_, err = ensure.Ensure(cmd.Context(), step, opts)
			return err
		},
	}

	cmd.Flags().StringVar(&addonsDir, "addons-dir", "",
		"path to the addons directory (default: <envfile-dir>/../drenv/addons)")
	return cmd
}
