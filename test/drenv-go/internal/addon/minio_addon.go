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
		return d.MC.SetAlias(ctx, cluster, url, minioAccessKey, minioSecretKey)
	})

	makeBucket := newApplyStep("mc-make-bucket", func(ctx context.Context) error {
		return d.MC.MakeBucket(ctx, cluster+"/"+minioBucket, true)
	})

	return Serial("addon/minio", d.Opts,
		applyMinio, waitRollout, setAlias, makeBucket,
	)
}
