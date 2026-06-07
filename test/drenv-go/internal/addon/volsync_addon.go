// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package addon: volsync installs VolSync via Helm on one or more clusters,
// mirroring addons/volsync/start.py.
//
// This is a GLOBAL worker addon: cluster param is "" and target clusters come
// from args.
//
// Steps (serial):
//  1. helm repo add --force-update backube https://backube.github.io/helm-charts/
//  2. For each cluster in args: helm upgrade --install volsync backube/volsync
//     --kube-context <cluster> --create-namespace --namespace volsync-system
//  3. For each cluster in args: kubectl rollout status volsync-system deploy/volsync

package addon

import (
	"context"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	volsyncNamespace      = "volsync-system"
	volsyncDeployment     = "volsync"
	volsyncRepo           = "backube"
	volsyncRepoURL        = "https://backube.github.io/helm-charts/"
	volsyncChart          = "backube/volsync"
	volsyncRolloutTimeout = 5 * time.Minute
)

func init() {
	Register("volsync", buildVolsync)
}

func buildVolsync(d Deps, _ string, args []string) ensure.Step {
	clusters := args

	addRepo := newApplyStep("helm-repo-add", func(ctx context.Context) error {
		return d.Helm.RepoAdd(ctx, volsyncRepo, volsyncRepoURL)
	})

	var installSteps []ensure.Step
	for _, cluster := range clusters {
		c := cluster // capture
		installSteps = append(installSteps, newApplyStep("helm-install/"+c, func(ctx context.Context) error {
			return d.Helm.UpgradeInstall(ctx,
				volsyncDeployment,
				volsyncChart,
				c,
				"--create-namespace",
				"--namespace", volsyncNamespace,
			)
		}))
	}

	var waitSteps []ensure.Step
	for _, cluster := range clusters {
		c := cluster // capture
		waitSteps = append(waitSteps, newApplyStep("rollout-status/"+c, func(ctx context.Context) error {
			return d.K.RolloutStatus(ctx, c, volsyncNamespace, "deploy/"+volsyncDeployment, volsyncRolloutTimeout)
		}))
	}

	steps := []ensure.Step{addRepo}
	steps = append(steps, installSteps...)
	steps = append(steps, waitSteps...)

	return Serial("addon/volsync", d.Opts, steps...)
}
