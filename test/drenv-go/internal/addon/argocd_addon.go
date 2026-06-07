// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package addon: argocd deploys ArgoCD on the hub cluster and registers member
// clusters, mirroring addons/argocd/start.py.
//
// This is a GLOBAL worker addon: cluster param is "" and targets come from args:
//   args[0]  = hub cluster context
//   args[1:] = member cluster contexts to add to ArgoCD
//
// Deploy step:
//   kubectl apply --kustomize <AddonsDir>/argocd/start-data --namespace argocd
//   (equivalent to Python's kubectl.apply("--filename", cached_yaml, "--namespace", "argocd"))
//
// Wait step:
//   kubectl wait deploy --all --for=condition=Available --namespace=argocd
//
// Add-clusters step (per member):
//   1. kubectl config view --flatten --output=yaml  → write to temp kubeconfig
//   2. kubectl config use-context <hub> --kubeconfig <tmpkc>
//   3. kubectl config set-context --current --namespace=argocd --kubeconfig=<tmpkc>
//   4. argocd login --core     (KUBECONFIG=<tmpkc>)
//   5. argocd cluster add <member> -y  (KUBECONFIG=<tmpkc>)
//
// NOTE: The temp-kubeconfig setup (steps 1–3) writes a kubeconfig to a
// temporary directory determined at runtime. Unit tests assert only the argv
// for kubectl config calls and argocd calls. Real-cluster validation is
// required to confirm that the KUBECONFIG environment variable is correctly
// passed to argocd. The "NOAUTH" error from argocd cluster add after
// argocd login --core is silently ignored, following the upstream Python
// workaround for https://github.com/argoproj/argo-cd/issues/18464.

package addon

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

func init() {
	Register("argocd", buildArgocd)
}

func buildArgocd(d Deps, _ string, args []string) ensure.Step {
	if len(args) < 1 {
		panic("argocd: args must contain at least one element (hub context)")
	}
	hub := args[0]
	members := args[1:]
	startDataDir := filepath.Join(d.AddonsDir, "argocd", "start-data")

	// deploy_argocd: apply kustomize dir with namespace
	deployStep := newApplyStep("apply-argocd", func(ctx context.Context) error {
		return d.K.Apply(ctx, hub,
			"--kustomize", startDataDir,
			"--namespace", "argocd",
		)
	})

	// wait_for_deployments
	waitStep := newApplyStep("wait-argocd-deployments", func(ctx context.Context) error {
		return d.K.WaitFor(ctx, hub, "argocd", "condition=Available",
			argocdWaitTimeout, "deploy", "--all",
		)
	})

	// add_clusters: one step per member cluster
	var clusterSteps []ensure.Step
	for _, member := range members {
		m := member // capture
		clusterSteps = append(clusterSteps,
			newApplyStep("add-cluster/"+m, func(ctx context.Context) error {
				return argocdAddCluster(ctx, d, hub, m)
			}),
		)
	}

	steps := []ensure.Step{deployStep, waitStep}
	steps = append(steps, clusterSteps...)
	return Serial("addon/argocd", d.Opts, steps...)
}

// argocdConfigDir returns the directory for per-environment argocd files:
//
//	~/.config/drenv/<EnvName>/argocd/
func argocdConfigDir(envName string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "~"
	}
	return filepath.Join(home, ".config", "drenv", envName, "argocd")
}

// argocdAddCluster mirrors the add_clusters block in start.py:
//  1. kubectl config view --flatten → write to temp kubeconfig
//  2. kubectl config use-context <hub> --kubeconfig <kc>
//  3. kubectl config set-context --current --namespace=argocd --kubeconfig=<kc>
//  4. argocd login --core  (KUBECONFIG=<kc>)
//  5. argocd cluster add <cluster> -y  (KUBECONFIG=<kc>)
func argocdAddCluster(ctx context.Context, d Deps, hub, cluster string) error {
	// Build the path for the per-env kubeconfig.
	configDir := argocdConfigDir(d.EnvName)
	if err := os.MkdirAll(configDir, 0o755); err != nil {
		return fmt.Errorf("argocd: create config dir: %w", err)
	}
	kubeconfig := filepath.Join(configDir, "kubeconfig")

	// Step 1: export flattened kubeconfig
	flat, err := d.K.Config(ctx, "view", "--flatten", "--output=yaml")
	if err != nil {
		return fmt.Errorf("argocd: kubectl config view: %w", err)
	}
	if err := os.WriteFile(kubeconfig, []byte(flat), 0o600); err != nil {
		return fmt.Errorf("argocd: write kubeconfig: %w", err)
	}

	// Step 2: use-context hub in the temp kubeconfig
	if _, err := d.K.Config(ctx, "use-context", hub, "--kubeconfig", kubeconfig); err != nil {
		return fmt.Errorf("argocd: kubectl config use-context: %w", err)
	}

	// Step 3: set-context --current --namespace=argocd
	if _, err := d.K.Config(ctx,
		"set-context", "--current",
		"--namespace=argocd",
		"--kubeconfig="+kubeconfig,
	); err != nil {
		return fmt.Errorf("argocd: kubectl config set-context: %w", err)
	}

	// Step 4: argocd login --core
	if err := d.Argocd.Login(ctx, kubeconfig); err != nil {
		return fmt.Errorf("argocd: login: %w", err)
	}

	// Step 5: argocd cluster add <cluster> -y
	// Mirror Python: ignore exit code 20 (NOAUTH), a known argocd bug after
	// "argocd login --core". See https://github.com/argoproj/argo-cd/issues/18464.
	// We cannot match the "NOAUTH" string (RunEnv does not capture stdout/stderr),
	// so we match on exit code 20 alone — faithful as possible given the seam.
	if err := d.Argocd.ClusterAdd(ctx, kubeconfig, cluster); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && exitErr.ExitCode() == 20 {
			// Suppress: known argocd NOAUTH transient error.
			return nil
		}
		return fmt.Errorf("argocd: cluster add %s: %w", cluster, err)
	}

	return nil
}

const argocdWaitTimeout = 5 * time.Minute
