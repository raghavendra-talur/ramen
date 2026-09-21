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
	"github.com/ramendr/ramen/test/drenv-go/internal/e2econfig"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// newProviderSelector returns a build.ProviderSelector that picks
// ExternalProvider for profiles with External==true and MinikubeProvider for
// all others, using real CLI clients. dnsMode ("auto"/"static"/"host"; "" means
// auto) is forwarded to minikube providers and only affects `start`; other
// commands pass "" since they never create clusters.
func newProviderSelector(dnsMode string) build.ProviderSelector {
	r := cli.Exec{}
	mk := &cli.Minikube{R: r}
	k := &cli.Kubectl{R: r}
	return func(prof envfile.Profile) provider.Provider {
		return provider.For(prof, mk, k, dnsMode)
	}
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
	var dnsMode string

	cmd := &cobra.Command{
		Use:   "start",
		Short: "Ensure all clusters in the environment are running",
		RunE: func(cmd *cobra.Command, args []string) error {
			switch dnsMode {
			case "auto", "static", "host":
			default:
				return fmt.Errorf("invalid --dns-mode %q: must be auto, static, or host", dnsMode)
			}

			env, err := loadEnv()
			if err != nil {
				return err
			}

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

			step := build.Start(env, newProviderSelector(dnsMode), deps, opts)
			if _, err := ensure.Ensure(cmd.Context(), step, opts); err != nil {
				return err
			}

			// When the env declares a ramen topology, dump the kubeconfig
			// layout that the e2e framework and localrun consume, mirroring
			// the Python `drenv start` (test/drenv/ramen.py dump_e2e_config).
			if env.Ramen != nil {
				baseDir, err := e2econfig.ConfigDir(env.Name)
				if err != nil {
					return err
				}

				fmt.Fprintf(cmd.OutOrStdout(), "[%s] Dumping ramen e2e config to %q\n", env.Name, baseDir)

				if err := e2econfig.Dump(cmd.Context(), deps.K, baseDir, *env.Ramen); err != nil {
					return fmt.Errorf("dump e2e config: %w", err)
				}
			}

			return nil
		},
	}

	cmd.Flags().StringVar(&addonsDir, "addons-dir", "",
		"path to the addons directory (default: <envfile-dir>/../drenv/addons)")
	cmd.Flags().StringVar(&dnsMode, "dns-mode", "auto",
		"DNS configuration mode: 'auto' detects managed Macs and uses 'static' "+
			"if needed; 'static' configures public DNS servers (8.8.8.8, 1.1.1.1); "+
			"'host' uses the host resolver (minikube default, may not work on "+
			"managed Macs)")
	return cmd
}
