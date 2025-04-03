// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package volsync_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"k8s.io/apimachinery/pkg/types"

	ramendrv1alpha1 "github.com/ramendr/ramen/api/v1alpha1"
	"github.com/ramendr/ramen/internal/controller/mocks"
)

func TestMockVolSyncResourceManager(t *testing.T) {
	// Create a new mock manager
	mockManager := mocks.NewMockVolSyncResourceManager()

	// Set up the mock configuration
	mockManager.ForceReplicationDestReady = true
	mockManager.ForceFinalSyncComplete = true
	mockManager.SetPVCInUse("test-namespace", "test-pvc", true)
	mockManager.SetPVCExists("test-namespace", "test-pvc", true)

	// Sample context for tests
	ctx := context.Background()

	// Test case 1: ReconcileReplicationSource and IsFinalSyncComplete
	t.Run("ReplicationSource", func(t *testing.T) {
		// Sample spec
		rsSpec := ramendrv1alpha1.VolSyncReplicationSourceSpec{
			ProtectedPVC: ramendrv1alpha1.ProtectedPVC{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		}

		// Call the method
		finalSyncComplete, rs, err := mockManager.ReconcileReplicationSource(ctx, rsSpec, true)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, rs)
		assert.True(t, finalSyncComplete)
		assert.Equal(t, 1, mockManager.ReconcileRSCallCount)
		assert.Equal(t, &rsSpec, mockManager.LastRSSpec)
		assert.True(t, mockManager.LastRunFinalSync)

		// Test IsFinalSyncComplete
		result := mockManager.IsFinalSyncComplete(rs)
		assert.True(t, result)
		assert.Equal(t, 1, mockManager.IsFinalSyncCompleteCallCount)
	})

	// Test case 2: ReconcileReplicationDestination and IsReplicationDestinationReady
	t.Run("ReplicationDestination", func(t *testing.T) {
		// Sample spec
		rdSpec := ramendrv1alpha1.VolSyncReplicationDestinationSpec{
			ProtectedPVC: ramendrv1alpha1.ProtectedPVC{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		}

		// Call the method
		rd, err := mockManager.ReconcileReplicationDestination(ctx, rdSpec)

		// Verify results
		assert.NoError(t, err)
		assert.NotNil(t, rd)
		assert.Equal(t, 1, mockManager.ReconcileRDCallCount)
		assert.Equal(t, &rdSpec, mockManager.LastRDSpec)

		// Test IsReplicationDestinationReady
		result := mockManager.IsReplicationDestinationReady(rd)
		assert.True(t, result)
	})

	// Test case 3: PVC validation
	t.Run("PVC Validation", func(t *testing.T) {
		pvcNamespacedName := types.NamespacedName{
			Name:      "test-pvc",
			Namespace: "test-namespace",
		}

		// Check if PVC exists
		exists, err := mockManager.ValidatePVC(ctx, pvcNamespacedName, true)
		assert.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, 1, mockManager.ValidatePVCCallCount)

		// Check if PVC is in use
		inUse, err := mockManager.IsPVCInUse(ctx, pvcNamespacedName, false)
		assert.NoError(t, err)
		assert.True(t, inUse)
		assert.Equal(t, 1, mockManager.IsPVCInUseCallCount)
	})

	// Test case 4: Deletion operations
	t.Run("Deletion Operations", func(t *testing.T) {
		// Delete ReplicationSource
		err := mockManager.DeleteReplicationSource(ctx, "test-pvc", "test-namespace")
		assert.NoError(t, err)
		assert.Equal(t, 1, mockManager.DeleteRSCallCount)

		// Delete ReplicationDestination
		err = mockManager.DeleteReplicationDestination(ctx, "test-pvc", "test-namespace")
		assert.NoError(t, err)
		assert.Equal(t, 1, mockManager.DeleteRDCallCount)
	})

	// Test case 5: Error simulation
	t.Run("Error Simulation", func(t *testing.T) {
		// Configure mock to fail
		mockManager.FailReconcileRS = true
		mockManager.FailReconcileRD = true
		mockManager.FailDelete = true
		mockManager.FailGet = true
		mockManager.FailValidate = true

		// Sample specs
		rsSpec := ramendrv1alpha1.VolSyncReplicationSourceSpec{
			ProtectedPVC: ramendrv1alpha1.ProtectedPVC{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		}
		rdSpec := ramendrv1alpha1.VolSyncReplicationDestinationSpec{
			ProtectedPVC: ramendrv1alpha1.ProtectedPVC{
				Name:      "test-pvc",
				Namespace: "test-namespace",
			},
		}

		// Test failure scenarios
		_, _, err := mockManager.ReconcileReplicationSource(ctx, rsSpec, false)
		assert.Error(t, err)

		_, err = mockManager.ReconcileReplicationDestination(ctx, rdSpec)
		assert.Error(t, err)

		err = mockManager.DeleteReplicationSource(ctx, "test-pvc", "test-namespace")
		assert.Error(t, err)

		_, err = mockManager.GetReplicationSource(ctx, "rs-test-pvc", "test-namespace")
		assert.Error(t, err)

		_, err = mockManager.ValidatePVC(ctx, types.NamespacedName{Name: "test-pvc", Namespace: "test-namespace"}, true)
		assert.Error(t, err)
	})

	// Reset call counts between test cases
	mockManager.ResetCallCounts()
}
