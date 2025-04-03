// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
)

// ReplicationSourceHandler defines operations for managing ReplicationSource resources
type ReplicationSourceHandler interface {
	// ReconcileReplicationSource creates or updates a ReplicationSource, handles final sync if needed
	// Returns:
	// - finalSyncComplete: true if final sync was requested and completed successfully
	// - *ReplicationSource: the reconciled ReplicationSource resource (nil if not created/updated)
	// - error: any error that occurred during reconciliation
	ReconcileReplicationSource(ctx context.Context, 
		rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec, 
		runFinalSync bool) (finalSyncComplete bool, rs *volsyncv1alpha1.ReplicationSource, err error)

	// GetReplicationSource retrieves a ReplicationSource by name and namespace
	GetReplicationSource(ctx context.Context, name, namespace string) (*volsyncv1alpha1.ReplicationSource, error)

	// DeleteReplicationSource deletes a ReplicationSource associated with a PVC
	DeleteReplicationSource(ctx context.Context, pvcName, pvcNamespace string) error

	// IsFinalSyncComplete determines if a final sync on a ReplicationSource has completed
	IsFinalSyncComplete(rs *volsyncv1alpha1.ReplicationSource) bool
}

// ReplicationDestinationHandler defines operations for managing ReplicationDestination resources
type ReplicationDestinationHandler interface {
	// ReconcileReplicationDestination creates or updates a ReplicationDestination
	// Returns the reconciled ReplicationDestination resource or an error
	ReconcileReplicationDestination(ctx context.Context, 
		rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec) (*volsyncv1alpha1.ReplicationDestination, error)

	// GetReplicationDestination retrieves a ReplicationDestination by name and namespace
	GetReplicationDestination(ctx context.Context, name, namespace string) (*volsyncv1alpha1.ReplicationDestination, error)

	// DeleteReplicationDestination deletes a ReplicationDestination associated with a PVC
	DeleteReplicationDestination(ctx context.Context, pvcName, pvcNamespace string) error

	// IsReplicationDestinationReady determines if a ReplicationDestination is ready for syncing
	IsReplicationDestinationReady(rd *volsyncv1alpha1.ReplicationDestination) bool

	// GetLatestImage retrieves the latest snapshot image from a ReplicationDestination
	GetLatestImage(ctx context.Context, pvcName, pvcNamespace string) (*corev1.TypedLocalObjectReference, error)
}

// VolSyncResourceManager combines both ReplicationSource and ReplicationDestination management
type VolSyncResourceManager interface {
	ReplicationSourceHandler
	ReplicationDestinationHandler

	// ValidatePVC determines if a PVC is suitable for VolSync operations
	ValidatePVC(ctx context.Context, pvcNamespacedName types.NamespacedName, mustExist bool) (bool, error)

	// IsPVCInUse determines if a PVC is currently mounted to a pod
	IsPVCInUse(ctx context.Context, pvcNamespacedName types.NamespacedName, requireMounted bool) (bool, error)

	// CleanupResourcesNotInSpec removes VolSync resources that aren't in the current spec list
	CleanupResourcesNotInSpec(ctx context.Context, 
		rsSpecList []ramendrv1alpha1.VolSyncReplicationSourceSpec,
		rdSpecList []ramendrv1alpha1.VolSyncReplicationDestinationSpec) error
}