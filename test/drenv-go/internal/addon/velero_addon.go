// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package addon: velero deploys the Velero backup operator configured to use a
// MinIO S3 backend, mirroring addons/velero/start.py.
//
// Steps (single apply step):
//  1. Resolve the MinIO service URL from the cluster.
//  2. Run velero install with the exact flags from start.py.

package addon

import (
	"context"
	"path/filepath"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	veleroImage   = "quay.io/prd/velero:v1.16.1"
	veleroPlugins = "quay.io/prd/velero-plugin-for-aws:v1.12.0,quay.io/kubevirt/kubevirt-velero-plugin:v0.8.0"
	veleroBucket  = "bucket"
)

func init() {
	Register("velero", buildVelero)
}

func buildVelero(d Deps, cluster string, _ []string) ensure.Step {
	credentialsFile := filepath.Join(d.AddonsDir, "velero", "start-data", "credentials.conf")

	install := newApplyStep("install", func(ctx context.Context) error {
		url, err := MinioServiceURL(ctx, d.K, cluster)
		if err != nil {
			return err
		}
		return d.Velero.Install(ctx,
			"--provider=aws",
			"--image="+veleroImage,
			"--plugins="+veleroPlugins,
			"--bucket="+veleroBucket,
			"--secret-file="+credentialsFile,
			"--use-volume-snapshots=false",
			"--backup-location-config=region=minio,s3ForcePathStyle=true,s3Url="+url,
			"--kubecontext="+cluster,
			"--wait",
		)
	})

	// Gate: skip when velero is already deployed (its install --wait leaves
	// deploy/velero Available in the velero namespace).
	return gatedAddon("addon/velero", d.Opts,
		gateDeploymentAvailable(d.K, cluster, "velero", "velero"),
		install)
}
