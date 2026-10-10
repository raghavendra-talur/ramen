// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// minio deploys MinIO object storage and configures the mc client, mirroring
// the Python addons/minio/start.py.
//
// Steps (serial):
//  1. kubectl apply --filename <AddonsDir>/minio/start-data/minio.yaml
//  2. rollout status minio deployment/minio
//  3. mc alias set <cluster> <minioServiceURL> minio minio123
//  4. mc mb --ignore-existing <cluster>/bucket

import (
	"context"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	minioRolloutTimeout = 5 * time.Minute
	minioBucket         = "bucket"
	minioAccessKey      = "minio"
	minioSecretKey      = "minio123"
	minioAliasAttempts  = 5
	minioAliasDelay     = 2 * time.Second
	minioStatTimeout    = 30 * time.Second
)

func init() {
	Register("minio", buildMinio)
}

func buildMinio(d Deps, cluster string, _ []string) ensure.Step {
	minioYAML := filepath.Join(d.AddonsDir, "minio", "start-data", "minio.yaml")

	applyMinio := newApplyStep("apply", func(ctx context.Context) error {
		return d.K.ApplyFile(ctx, cluster, minioYAML)
	})

	waitRollout := newApplyStep("wait-rollout", func(ctx context.Context) error {
		return d.K.RolloutStatus(ctx, cluster, "minio", "deployment/minio", minioRolloutTimeout)
	})

	setAlias := newApplyStep("mc-set-alias", func(ctx context.Context) error {
		url, err := MinioServiceURL(ctx, d.K, cluster)
		if err != nil {
			return err
		}

		return setMinioAlias(ctx, d, cluster, url)
	})

	makeBucket := newApplyStep("mc-make-bucket", func(ctx context.Context) error {
		return d.MC.MakeBucket(ctx, cluster+"/"+minioBucket, true)
	})

	// Gate on what the addon produces: the Deployment Available and the bucket
	// reachable through the mc alias. An alias left by a previous cluster
	// points at a dead address, so the steps re-run and refresh it; they are
	// idempotent.
	return gatedAddon("addon/minio", d.Opts,
		gateMinioReady(d, cluster),
		applyMinio, waitRollout, setAlias, makeBucket,
	)
}

// setMinioAlias retries mc alias set with a doubling delay, matching Python's
// minio start: the Deployment is Available before minio serves on its
// NodePort, so the first attempts can be refused.
func setMinioAlias(ctx context.Context, d Deps, cluster, url string) error {
	delay := d.Opts.VerifyInterval
	if delay <= 0 {
		delay = minioAliasDelay
	}

	var err error
	for attempt := 1; attempt <= minioAliasAttempts; attempt++ {
		if err = d.MC.SetAlias(ctx, cluster, url, minioAccessKey, minioSecretKey); err == nil {
			return nil
		}

		if attempt == minioAliasAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		delay *= 2
	}

	return err
}

// gateMinioReady reports whether the minio Deployment is Available and its
// bucket is reachable through the cluster's mc alias.
func gateMinioReady(d Deps, cluster string) func(context.Context) (bool, error) {
	return func(ctx context.Context) (bool, error) {
		if !deploymentAvailable(ctx, d.K, cluster, "minio", "minio") {
			return false, nil
		}

		ctx, cancel := context.WithTimeout(ctx, minioStatTimeout)
		defer cancel()

		return d.MC.Stat(ctx, cluster+"/"+minioBucket) == nil, nil
	}
}
