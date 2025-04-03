// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package volsync

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/go-logr/logr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	"github.com/ramendr/ramen/internal/controller"
)

// VolSyncResourceManagerImpl implements the VolSyncResourceManager interface
type VolSyncResourceManagerImpl struct {
	handler *VSHandler
	ctx     context.Context
	log     logr.Logger
}

// NewVolSyncResourceManager creates a new VolSync resource manager
func NewVolSyncResourceManager(
	ctx context.Context,
	client client.Client,
	log logr.Logger,
	owner metav1.Object,
	asyncSpec *ramendrv1alpha1.VRGAsyncSpec,
	defaultCephFSCSIDriverName string,
	volSyncDestinationCopyMethod string,
	adminNamespaceVRG bool,
) controller.VolSyncResourceManager {
	handler := NewVSHandler(
		ctx,
		client,
		log,
		owner,
		asyncSpec,
		defaultCephFSCSIDriverName,
		volSyncDestinationCopyMethod,
		adminNamespaceVRG,
	)

	return &VolSyncResourceManagerImpl{
		handler: handler,
		ctx:     ctx,
		log:     log,
	}
}

// ReconcileReplicationSource implements ReplicationSourceHandler
func (v *VolSyncResourceManagerImpl) ReconcileReplicationSource(
	ctx context.Context,
	rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec,
	runFinalSync bool,
) (bool, *volsyncv1alpha1.ReplicationSource, error) {
	return v.handler.ReconcileRS(rsSpec, runFinalSync)
}

// GetReplicationSource implements ReplicationSourceHandler
func (v *VolSyncResourceManagerImpl) GetReplicationSource(
	ctx context.Context,
	name, namespace string,
) (*volsyncv1alpha1.ReplicationSource, error) {
	return v.handler.GetRS(name, namespace)
}

// DeleteReplicationSource implements ReplicationSourceHandler
func (v *VolSyncResourceManagerImpl) DeleteReplicationSource(
	ctx context.Context,
	pvcName, pvcNamespace string,
) error {
	return v.handler.DeleteRS(pvcName, pvcNamespace)
}

// IsFinalSyncComplete implements ReplicationSourceHandler
func (v *VolSyncResourceManagerImpl) IsFinalSyncComplete(rs *volsyncv1alpha1.ReplicationSource) bool {
	return isFinalSyncComplete(rs, v.log)
}

// ReconcileReplicationDestination implements ReplicationDestinationHandler
func (v *VolSyncResourceManagerImpl) ReconcileReplicationDestination(
	ctx context.Context,
	rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec,
) (*volsyncv1alpha1.ReplicationDestination, error) {
	return v.handler.ReconcileRD(rdSpec)
}

// GetReplicationDestination implements ReplicationDestinationHandler
func (v *VolSyncResourceManagerImpl) GetReplicationDestination(
	ctx context.Context,
	name, namespace string,
) (*volsyncv1alpha1.ReplicationDestination, error) {
	return v.handler.GetRD(name, namespace)
}

// DeleteReplicationDestination implements ReplicationDestinationHandler
func (v *VolSyncResourceManagerImpl) DeleteReplicationDestination(
	ctx context.Context,
	pvcName, pvcNamespace string,
) error {
	return v.handler.DeleteRD(pvcName, pvcNamespace)
}

// IsReplicationDestinationReady implements ReplicationDestinationHandler
func (v *VolSyncResourceManagerImpl) IsReplicationDestinationReady(rd *volsyncv1alpha1.ReplicationDestination) bool {
	return RDStatusReady(rd, v.log)
}

// GetLatestImage implements ReplicationDestinationHandler
func (v *VolSyncResourceManagerImpl) GetLatestImage(
	ctx context.Context,
	pvcName, pvcNamespace string,
) (*corev1.TypedLocalObjectReference, error) {
	return v.handler.GetRDLatestImage(pvcName, pvcNamespace)
}

// ValidatePVC implements VolSyncResourceManager
func (v *VolSyncResourceManagerImpl) ValidatePVC(
	ctx context.Context,
	pvcNamespacedName types.NamespacedName,
	mustExist bool,
) (bool, error) {
	return v.handler.PVCExistsAndBound(pvcNamespacedName, mustExist)
}

// IsPVCInUse implements VolSyncResourceManager
func (v *VolSyncResourceManagerImpl) IsPVCInUse(
	ctx context.Context,
	pvcNamespacedName types.NamespacedName,
	requireMounted bool,
) (bool, error) {
	return v.handler.PVCExistsAndInUse(pvcNamespacedName, requireMounted)
}

// CleanupResourcesNotInSpec implements VolSyncResourceManager
func (v *VolSyncResourceManagerImpl) CleanupResourcesNotInSpec(
	ctx context.Context,
	rsSpecList []ramendrv1alpha1.VolSyncReplicationSourceSpec,
	rdSpecList []ramendrv1alpha1.VolSyncReplicationDestinationSpec,
) error {
	// Convert between the spec formats
	pvcNames := make([]types.NamespacedName, 0, len(rsSpecList)+len(rdSpecList))
	
	// Add RSs to the list
	for _, rsSpec := range rsSpecList {
		pvcNames = append(pvcNames, types.NamespacedName{
			Name:      rsSpec.ProtectedPVC.Name,
			Namespace: rsSpec.ProtectedPVC.Namespace,
		})
	}
	
	// Add RDs to the list
	for _, rdSpec := range rdSpecList {
		pvcNames = append(pvcNames, types.NamespacedName{
			Name:      rdSpec.ProtectedPVC.Name,
			Namespace: rdSpec.ProtectedPVC.Namespace,
		})
	}
	
	// Cleanup any RS not in the list
	if err := v.cleanupRSNotInList(ctx, pvcNames); err != nil {
		return fmt.Errorf("error cleaning up RS resources: %w", err)
	}
	
	// Cleanup any RD not in the list
	if err := v.handler.CleanupRDNotInSpecList(rdSpecList); err != nil {
		return fmt.Errorf("error cleaning up RD resources: %w", err)
	}
	
	return nil
}

// cleanupRSNotInList cleans up ReplicationSources not in the list of PVC names
func (v *VolSyncResourceManagerImpl) cleanupRSNotInList(
	ctx context.Context, 
	pvcNames []types.NamespacedName,
) error {
	// Get a map of namespaces to check
	namespaces := make(map[string]bool)
	for _, pvc := range pvcNames {
		namespaces[pvc.Namespace] = true
	}
	
	// Check each namespace
	for namespace := range namespaces {
		rsList, err := v.handler.ListRSByOwner(namespace)
		if err != nil {
			return err
		}
		
		for i := range rsList.Items {
			rs := &rsList.Items[i]
			
			// Skip if this RS is for a PVC in our list
			found := false
			for _, pvc := range pvcNames {
				rsName := getReplicationSourceName(pvc.Name)
				if rs.Name == rsName && rs.Namespace == pvc.Namespace {
					found = true
					break
				}
			}
			
			if !found {
				// Delete the RS since it's not in our spec list
				if err := ctrl.SetControllerReference(v.handler.owner, rs, v.handler.client.Scheme()); err != nil {
					v.log.Error(err, "unable to set controller reference", "rs", rs.Name)
					continue
				}
				
				if err := v.handler.client.Delete(ctx, rs); err != nil {
					v.log.Error(err, "failed to delete ReplicationSource", "rs", rs.Name)
					continue
				}
				
				v.log.Info("Deleted ReplicationSource not in spec list", "rs", rs.Name)
			}
		}
	}
	
	return nil
}

// GetLogger gets the logger instance
func (v *VolSyncResourceManagerImpl) GetLogger() logr.Logger {
	return v.log
}

// Ensure VolSyncResourceManagerImpl implements the VolSyncResourceManager interface
var _ controller.VolSyncResourceManager = &VolSyncResourceManagerImpl{}