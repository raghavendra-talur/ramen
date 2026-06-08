// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package e2econfig writes the kubeconfig layout that the ramen e2e framework
// and the localrun tool consume after an environment with a ramen topology has
// been started. It is the Go port of test/drenv/ramen.py dump_e2e_config, which
// the Python `drenv start` runs whenever the envfile has a `ramen` section.
package e2econfig

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"

	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

// e2eNames are the fixed e2e-framework cluster identifiers, matched positionally
// against [hub, clusters...]. This mirrors the hardcoded ["hub", "c1", "c2"]
// list in the Python dump_e2e_config; extra clusters beyond these slots are
// truncated, exactly as the Python zip() does.
var e2eNames = []string{"hub", "c1", "c2"}

// ConfigViewer is the subset of cli.Kubectl that Dump needs: it runs
// `kubectl config <args...>` and returns the command output. *cli.Kubectl
// satisfies this interface.
type ConfigViewer interface {
	Config(ctx context.Context, args ...string) (string, error)
}

// ConfigDir returns the per-environment drenv configuration directory,
// ~/.config/drenv/<name>, matching Python's drenv.config_dir.
func ConfigDir(name string) (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("cannot determine home directory: %w", err)
	}

	return filepath.Join(home, ".config", "drenv", name), nil
}

// e2eCluster is one entry in the e2e config's clusters map.
type e2eCluster struct {
	Kubeconfig string `yaml:"kubeconfig"`
}

// e2eConfig is the document written to <baseDir>/config.yaml.
type e2eConfig struct {
	Clusters map[string]e2eCluster `yaml:"clusters"`
}

// Dump writes the e2e/localrun kubeconfig layout under baseDir:
//
//	<baseDir>/kubeconfigs/<cluster>  one self-contained kubeconfig per cluster
//	<baseDir>/config.yaml            e2e cluster map (hub/c1/c2 → kubeconfig path)
//
// Each kubeconfig is produced with `kubectl config view --flatten --minify
// --context=<cluster>` so it is self-contained and scoped to that cluster. The
// per-cluster files are named by their real cluster name (e.g. hub, dr1, dr2),
// which is the path localrun expects; config.yaml maps the e2e names (hub, c1,
// c2) onto those files for the e2e framework. This is the faithful Go port of
// test/drenv/ramen.py dump_e2e_config.
func Dump(ctx context.Context, k ConfigViewer, baseDir string, r envfile.Ramen) error {
	kubeconfigsDir := filepath.Join(baseDir, "kubeconfigs")
	if err := os.MkdirAll(kubeconfigsDir, 0o755); err != nil {
		return fmt.Errorf("create kubeconfigs dir %q: %w", kubeconfigsDir, err)
	}

	clusters := append([]string{r.Hub}, r.Clusters...)

	cfg := e2eConfig{Clusters: make(map[string]e2eCluster, len(e2eNames))}

	for i, e2eName := range e2eNames {
		if i >= len(clusters) {
			break
		}

		clusterName := clusters[i]
		if clusterName == "" {
			continue
		}

		data, err := k.Config(ctx, "view", "--flatten", "--minify", "--context="+clusterName)
		if err != nil {
			return fmt.Errorf("export kubeconfig for cluster %q: %w", clusterName, err)
		}

		path := filepath.Join(kubeconfigsDir, clusterName)
		if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
			return fmt.Errorf("write kubeconfig %q: %w", path, err)
		}

		cfg.Clusters[e2eName] = e2eCluster{Kubeconfig: path}
	}

	out, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal e2e config: %w", err)
	}

	configPath := filepath.Join(baseDir, "config.yaml")
	if err := os.WriteFile(configPath, out, 0o644); err != nil { //nolint:gosec // config.yaml holds only kubeconfig paths, not secrets
		return fmt.Errorf("write e2e config %q: %w", configPath, err)
	}

	return nil
}
