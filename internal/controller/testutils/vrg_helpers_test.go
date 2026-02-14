// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

func TestVRGBuilder(t *testing.T) {
	tests := []struct {
		name             string
		builderFn        func() *VRGBuilder
		expectedName     string
		expectedNS       string
		expectedState    ramen.ReplicationState
		expectedProfiles []string
		hasAsync         bool
		hasSync          bool
	}{
		{
			name: "basic VRG",
			builderFn: func() *VRGBuilder {
				return NewVRGBuilder("test-vrg", "test-ns")
			},
			expectedName:  "test-vrg",
			expectedNS:    "test-ns",
			expectedState: ramen.Primary,
		},
		{
			name: "VRG with S3 profiles",
			builderFn: func() *VRGBuilder {
				return NewVRGBuilder("vrg1", "ns1").
					WithS3Profiles([]string{"profile1", "profile2"})
			},
			expectedName:     "vrg1",
			expectedNS:       "ns1",
			expectedState:    ramen.Primary,
			expectedProfiles: []string{"profile1", "profile2"},
		},
		{
			name: "VRG with async spec",
			builderFn: func() *VRGBuilder {
				return NewVRGBuilder("async-vrg", "ns").
					WithAsyncSpec("5m")
			},
			expectedName:  "async-vrg",
			expectedNS:    "ns",
			expectedState: ramen.Primary,
			hasAsync:      true,
		},
		{
			name: "secondary VRG with sync",
			builderFn: func() *VRGBuilder {
				return NewVRGBuilder("sync-vrg", "ns").
					WithReplicationState(ramen.Secondary).
					WithSyncSpec()
			},
			expectedName:  "sync-vrg",
			expectedNS:    "ns",
			expectedState: ramen.Secondary,
			hasSync:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vrg := tt.builderFn().Build()

			if vrg.Name != tt.expectedName {
				t.Errorf("expected name %s, got %s", tt.expectedName, vrg.Name)
			}

			if vrg.Namespace != tt.expectedNS {
				t.Errorf("expected namespace %s, got %s", tt.expectedNS, vrg.Namespace)
			}

			if vrg.Spec.ReplicationState != tt.expectedState {
				t.Errorf("expected state %s, got %s", tt.expectedState, vrg.Spec.ReplicationState)
			}

			if len(tt.expectedProfiles) > 0 {
				if len(vrg.Spec.S3Profiles) != len(tt.expectedProfiles) {
					t.Errorf("expected %d profiles, got %d", len(tt.expectedProfiles), len(vrg.Spec.S3Profiles))
				}
			}

			if tt.hasAsync && vrg.Spec.Async == nil {
				t.Error("expected async spec to be set")
			}

			if tt.hasSync && vrg.Spec.Sync == nil {
				t.Error("expected sync spec to be set")
			}
		})
	}
}

func TestVRGStatusBuilder(t *testing.T) {
	tests := []struct {
		name           string
		builderFn      func() *VRGStatusBuilder
		expectedState  ramen.State
		expectedPVCs   int
		conditionCount int
	}{
		{
			name: "basic status",
			builderFn: func() *VRGStatusBuilder {
				return NewVRGStatusBuilder()
			},
			expectedState: ramen.PrimaryState,
		},
		{
			name: "secondary state with conditions",
			builderFn: func() *VRGStatusBuilder {
				return NewVRGStatusBuilder().
					WithState(ramen.SecondaryState).
					WithCondition("DataReady", metav1.ConditionTrue, "Ready", "Data is ready")
			},
			expectedState:  ramen.SecondaryState,
			conditionCount: 1,
		},
		{
			name: "with protected PVCs",
			builderFn: func() *VRGStatusBuilder {
				return NewVRGStatusBuilder().
					WithProtectedPVC("pvc1", "ns1", "sc1", true).
					WithProtectedPVC("pvc2", "ns1", "sc1", false)
			},
			expectedState: ramen.PrimaryState,
			expectedPVCs:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := tt.builderFn().Build()

			if status.State != tt.expectedState {
				t.Errorf("expected state %s, got %s", tt.expectedState, status.State)
			}

			if len(status.Conditions) != tt.conditionCount {
				t.Errorf("expected %d conditions, got %d", tt.conditionCount, len(status.Conditions))
			}

			if len(status.ProtectedPVCs) != tt.expectedPVCs {
				t.Errorf("expected %d protected PVCs, got %d", tt.expectedPVCs, len(status.ProtectedPVCs))
			}
		})
	}
}
