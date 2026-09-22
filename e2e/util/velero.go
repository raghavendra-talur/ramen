// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"fmt"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/e2e/config"
	"github.com/ramendr/ramen/e2e/types"
)

const (
	// Owner labels Ramen stamps on every Velero Backup CR it creates for a VRG.
	// Kept in sync with internal/controller/util/labels.go.
	labelOwnerNamespaceName = "ramendr.openshift.io/owner-namespace-name"
	labelOwnerName          = "ramendr.openshift.io/owner-name"
)

// veleroBackupListGVK identifies the Velero Backup list without depending on the
// velero apis module, which is not a dependency of the e2e module.
var veleroBackupListGVK = schema.GroupVersionKind{
	Group:   "velero.io",
	Version: "v1",
	Kind:    "BackupList",
}

// veleroNamespace returns the namespace where Velero runs, which depends on the distro.
func veleroNamespace(cfg *config.Config) string {
	if cfg.Distro == config.DistroOcp {
		return "openshift-adp"
	}

	return "velero"
}

// WaitForVeleroBackupsDeleted waits until no Velero Backup CRs owned by the VRG
// identified by ownerNamespace/ownerName remain on the cluster.
//
// When a Primary VRG is demoted to Secondary during failover or relocate, Ramen
// deletes the local Velero Backup CRs it left behind while Primary. It only
// removes the Kubernetes CRs and never the shared object-storage copies, since a
// new backup may already exist there under the same name from the peer cluster.
// This verifies that the local cleanup completed on the demoted cluster.
func WaitForVeleroBackupsDeleted(
	ctx types.Context, cluster *types.Cluster, ownerNamespace, ownerName string,
) error {
	log := ctx.Logger()
	namespace := veleroNamespace(ctx.Config())
	matching := client.MatchingLabels{
		labelOwnerNamespaceName: ownerNamespace,
		labelOwnerName:          ownerName,
	}
	start := time.Now()

	log.Debugf("Waiting until Velero backups owned by %q/%q are deleted in cluster %q",
		ownerNamespace, ownerName, cluster.Name)

	for {
		backups := &unstructured.UnstructuredList{}
		backups.SetGroupVersionKind(veleroBackupListGVK)

		if err := cluster.Client.List(ctx.Context(), backups,
			client.InNamespace(namespace), matching); err != nil {
			return fmt.Errorf("failed to list velero backups in cluster %q: %w", cluster.Name, err)
		}

		if len(backups.Items) == 0 {
			log.Debugf("Velero backups owned by %q/%q deleted in cluster %q in %.3f seconds",
				ownerNamespace, ownerName, cluster.Name, time.Since(start).Seconds())

			return nil
		}

		if err := Sleep(ctx.Context(), time.Second); err != nil {
			return fmt.Errorf("velero backups owned by %q/%q not deleted in cluster %q: %w",
				ownerNamespace, ownerName, cluster.Name, err)
		}
	}
}
