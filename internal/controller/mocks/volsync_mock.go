// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package mocks

import (
	"context"
	"fmt"
	"sync"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	"github.com/ramendr/ramen/internal/controller"
)

// MockVolSyncResourceManager implements VolSyncResourceManager interface for testing
type MockVolSyncResourceManager struct {
	mu sync.RWMutex

	// Storage for resources
	replicationSources      map[string]*volsyncv1alpha1.ReplicationSource      // key: namespace/name
	replicationDestinations map[string]*volsyncv1alpha1.ReplicationDestination // key: namespace/name

	// Mock configuration
	PVCsInUse                   map[string]bool // key: namespace/name
	PVCsExistence               map[string]bool // key: namespace/name
	ForceFinalSyncComplete      bool
	ForceReplicationDestReady   bool
	FailReconcileRS             bool
	FailReconcileRD             bool
	FailDelete                  bool
	FailGet                     bool
	FailValidate                bool
	ReturnedLatestImage         *corev1.TypedLocalObjectReference
	ReconcileRSCallCount        int
	ReconcileRDCallCount        int
	DeleteRSCallCount           int
	DeleteRDCallCount           int
	GetRSCallCount              int
	GetRDCallCount              int
	ValidatePVCCallCount        int
	IsPVCInUseCallCount         int
	CleanupResourcesCallCount   int
	LastRSSpec                  *ramendrv1alpha1.VolSyncReplicationSourceSpec
	LastRDSpec                  *ramendrv1alpha1.VolSyncReplicationDestinationSpec
	LastRunFinalSync            bool
	IsFinalSyncCompleteCallCount int
}

// NewMockVolSyncResourceManager creates a new mock manager
func NewMockVolSyncResourceManager() *MockVolSyncResourceManager {
	return &MockVolSyncResourceManager{
		replicationSources:      make(map[string]*volsyncv1alpha1.ReplicationSource),
		replicationDestinations: make(map[string]*volsyncv1alpha1.ReplicationDestination),
		PVCsInUse:               make(map[string]bool),
		PVCsExistence:           make(map[string]bool),
	}
}

// key returns a map key for namespace/name
func key(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// ReconcileReplicationSource implements ReplicationSourceHandler
func (m *MockVolSyncResourceManager) ReconcileReplicationSource(
	ctx context.Context,
	rsSpec ramendrv1alpha1.VolSyncReplicationSourceSpec,
	runFinalSync bool,
) (bool, *volsyncv1alpha1.ReplicationSource, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ReconcileRSCallCount++
	m.LastRSSpec = &rsSpec
	m.LastRunFinalSync = runFinalSync

	if m.FailReconcileRS {
		return false, nil, fmt.Errorf("mock reconcile RS failure")
	}

	// Create the resource if it doesn't exist
	rsName := fmt.Sprintf("rs-%s", rsSpec.ProtectedPVC.Name)
	k := key(rsSpec.ProtectedPVC.Namespace, rsName)

	if _, exists := m.replicationSources[k]; !exists {
		m.replicationSources[k] = &volsyncv1alpha1.ReplicationSource{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rsName,
				Namespace: rsSpec.ProtectedPVC.Namespace,
			},
			Spec: volsyncv1alpha1.ReplicationSourceSpec{
				SourcePVC: rsSpec.ProtectedPVC.Name,
			},
			Status: &volsyncv1alpha1.ReplicationSourceStatus{},
		}
	}

	if runFinalSync && m.ForceFinalSyncComplete {
		m.replicationSources[k].Status.LastManualSync = "vrg-final-sync"
		return true, m.replicationSources[k], nil
	}

	return false, m.replicationSources[k], nil
}

// GetReplicationSource implements ReplicationSourceHandler
func (m *MockVolSyncResourceManager) GetReplicationSource(
	ctx context.Context,
	name, namespace string,
) (*volsyncv1alpha1.ReplicationSource, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.GetRSCallCount++

	if m.FailGet {
		return nil, fmt.Errorf("mock get RS failure")
	}

	k := key(namespace, name)
	rs, exists := m.replicationSources[k]
	if !exists {
		return nil, fmt.Errorf("replication source %s not found", k)
	}

	return rs, nil
}

// DeleteReplicationSource implements ReplicationSourceHandler
func (m *MockVolSyncResourceManager) DeleteReplicationSource(
	ctx context.Context,
	pvcName, pvcNamespace string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DeleteRSCallCount++

	if m.FailDelete {
		return fmt.Errorf("mock delete RS failure")
	}

	rsName := fmt.Sprintf("rs-%s", pvcName)
	k := key(pvcNamespace, rsName)
	delete(m.replicationSources, k)

	return nil
}

// IsFinalSyncComplete implements ReplicationSourceHandler
func (m *MockVolSyncResourceManager) IsFinalSyncComplete(rs *volsyncv1alpha1.ReplicationSource) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.IsFinalSyncCompleteCallCount++

	if rs == nil || rs.Status == nil {
		return false
	}

	return rs.Status.LastManualSync == "vrg-final-sync" || m.ForceFinalSyncComplete
}

// ReconcileReplicationDestination implements ReplicationDestinationHandler
func (m *MockVolSyncResourceManager) ReconcileReplicationDestination(
	ctx context.Context,
	rdSpec ramendrv1alpha1.VolSyncReplicationDestinationSpec,
) (*volsyncv1alpha1.ReplicationDestination, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ReconcileRDCallCount++
	m.LastRDSpec = &rdSpec

	if m.FailReconcileRD {
		return nil, fmt.Errorf("mock reconcile RD failure")
	}

	// Create the resource if it doesn't exist
	rdName := fmt.Sprintf("rd-%s", rdSpec.ProtectedPVC.Name)
	k := key(rdSpec.ProtectedPVC.Namespace, rdName)

	if _, exists := m.replicationDestinations[k]; !exists {
		m.replicationDestinations[k] = &volsyncv1alpha1.ReplicationDestination{
			ObjectMeta: metav1.ObjectMeta{
				Name:      rdName,
				Namespace: rdSpec.ProtectedPVC.Namespace,
			},
			Status: &volsyncv1alpha1.ReplicationDestinationStatus{},
		}
	}

	// Ensure status is ready
	if m.ForceReplicationDestReady {
		m.replicationDestinations[k].Status.RsyncTLS = &volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus{
			Address: &volsyncv1alpha1.RsyncTLSAddress{
				Address: "example.com:8080",
			},
		}
	}

	return m.replicationDestinations[k], nil
}

// GetReplicationDestination implements ReplicationDestinationHandler
func (m *MockVolSyncResourceManager) GetReplicationDestination(
	ctx context.Context,
	name, namespace string,
) (*volsyncv1alpha1.ReplicationDestination, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.GetRDCallCount++

	if m.FailGet {
		return nil, fmt.Errorf("mock get RD failure")
	}

	k := key(namespace, name)
	rd, exists := m.replicationDestinations[k]
	if !exists {
		return nil, fmt.Errorf("replication destination %s not found", k)
	}

	return rd, nil
}

// DeleteReplicationDestination implements ReplicationDestinationHandler
func (m *MockVolSyncResourceManager) DeleteReplicationDestination(
	ctx context.Context,
	pvcName, pvcNamespace string,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.DeleteRDCallCount++

	if m.FailDelete {
		return fmt.Errorf("mock delete RD failure")
	}

	rdName := fmt.Sprintf("rd-%s", pvcName)
	k := key(pvcNamespace, rdName)
	delete(m.replicationDestinations, k)

	return nil
}

// IsReplicationDestinationReady implements ReplicationDestinationHandler
func (m *MockVolSyncResourceManager) IsReplicationDestinationReady(rd *volsyncv1alpha1.ReplicationDestination) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.ForceReplicationDestReady {
		return true
	}

	if rd == nil || rd.Status == nil || rd.Status.RsyncTLS == nil || rd.Status.RsyncTLS.Address == nil {
		return false
	}

	return true
}

// GetLatestImage implements ReplicationDestinationHandler
func (m *MockVolSyncResourceManager) GetLatestImage(
	ctx context.Context,
	pvcName, pvcNamespace string,
) (*corev1.TypedLocalObjectReference, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if m.FailGet {
		return nil, fmt.Errorf("mock get latest image failure")
	}

	return m.ReturnedLatestImage, nil
}

// ValidatePVC implements VolSyncResourceManager
func (m *MockVolSyncResourceManager) ValidatePVC(
	ctx context.Context,
	pvcNamespacedName types.NamespacedName,
	mustExist bool,
) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.ValidatePVCCallCount++

	if m.FailValidate {
		return false, fmt.Errorf("mock validate PVC failure")
	}

	k := key(pvcNamespacedName.Namespace, pvcNamespacedName.Name)
	exists, ok := m.PVCsExistence[k]
	
	if !ok {
		// Default to true if not specified
		exists = true
	}

	if mustExist && !exists {
		return false, nil
	}

	return exists, nil
}

// IsPVCInUse implements VolSyncResourceManager
func (m *MockVolSyncResourceManager) IsPVCInUse(
	ctx context.Context,
	pvcNamespacedName types.NamespacedName,
	requireMounted bool,
) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	m.IsPVCInUseCallCount++

	if m.FailValidate {
		return false, fmt.Errorf("mock IsPVCInUse failure")
	}

	k := key(pvcNamespacedName.Namespace, pvcNamespacedName.Name)
	inUse, ok := m.PVCsInUse[k]
	
	if !ok {
		// Default to false if not specified
		inUse = false
	}

	return inUse, nil
}

// CleanupResourcesNotInSpec implements VolSyncResourceManager
func (m *MockVolSyncResourceManager) CleanupResourcesNotInSpec(
	ctx context.Context,
	rsSpecList []ramendrv1alpha1.VolSyncReplicationSourceSpec,
	rdSpecList []ramendrv1alpha1.VolSyncReplicationDestinationSpec,
) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.CleanupResourcesCallCount++

	if m.FailDelete {
		return fmt.Errorf("mock cleanup resources failure")
	}

	return nil
}

// ResetCallCounts resets all call counters for testing
func (m *MockVolSyncResourceManager) ResetCallCounts() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.ReconcileRSCallCount = 0
	m.ReconcileRDCallCount = 0
	m.DeleteRSCallCount = 0
	m.DeleteRDCallCount = 0
	m.GetRSCallCount = 0
	m.GetRDCallCount = 0
	m.ValidatePVCCallCount = 0
	m.IsPVCInUseCallCount = 0
	m.CleanupResourcesCallCount = 0
	m.IsFinalSyncCompleteCallCount = 0
	m.LastRSSpec = nil
	m.LastRDSpec = nil
}

// SetPVCInUse sets whether a specific PVC is in use
func (m *MockVolSyncResourceManager) SetPVCInUse(namespace, name string, inUse bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PVCsInUse[key(namespace, name)] = inUse
}

// SetPVCExists sets whether a specific PVC exists
func (m *MockVolSyncResourceManager) SetPVCExists(namespace, name string, exists bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.PVCsExistence[key(namespace, name)] = exists
}

// EnsureMockVolSyncResourceManagerHasRequiredMethods ensures the mock implements all interface methods
var _ controller.VolSyncResourceManager = &MockVolSyncResourceManager{}