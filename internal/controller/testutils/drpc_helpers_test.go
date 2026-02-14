// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

func TestDRPCBuilder(t *testing.T) {
	tests := []struct {
		name               string
		builderFn          func() *DRPCBuilder
		expectedName       string
		expectedNS         string
		expectedPolicy     string
		expectedPlacement  string
		expectedAction     ramen.DRAction
		expectedPreferred  string
		expectedFailover   string
	}{
		{
			name: "basic DRPC",
			builderFn: func() *DRPCBuilder {
				return NewDRPCBuilder("test-drpc", "test-ns")
			},
			expectedName: "test-drpc",
			expectedNS:   "test-ns",
		},
		{
			name: "DRPC with policy and placement",
			builderFn: func() *DRPCBuilder {
				return NewDRPCBuilder("drpc1", "ns1").
					WithDRPolicyRef("policy1").
					WithPlacementRef("placement1")
			},
			expectedName:      "drpc1",
			expectedNS:        "ns1",
			expectedPolicy:    "policy1",
			expectedPlacement: "placement1",
		},
		{
			name: "DRPC with failover action",
			builderFn: func() *DRPCBuilder {
				return NewDRPCBuilder("failover-drpc", "ns").
					WithAction(ramen.ActionFailover).
					WithPreferredCluster("cluster1").
					WithFailoverCluster("cluster2")
			},
			expectedName:      "failover-drpc",
			expectedNS:        "ns",
			expectedAction:    ramen.ActionFailover,
			expectedPreferred: "cluster1",
			expectedFailover:  "cluster2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			drpc := tt.builderFn().Build()

			if drpc.Name != tt.expectedName {
				t.Errorf("expected name %s, got %s", tt.expectedName, drpc.Name)
			}

			if drpc.Namespace != tt.expectedNS {
				t.Errorf("expected namespace %s, got %s", tt.expectedNS, drpc.Namespace)
			}

			if tt.expectedPolicy != "" && drpc.Spec.DRPolicyRef.Name != tt.expectedPolicy {
				t.Errorf("expected policy %s, got %s", tt.expectedPolicy, drpc.Spec.DRPolicyRef.Name)
			}

			if tt.expectedPlacement != "" && drpc.Spec.PlacementRef.Name != tt.expectedPlacement {
				t.Errorf("expected placement %s, got %s", tt.expectedPlacement, drpc.Spec.PlacementRef.Name)
			}

			if drpc.Spec.Action != tt.expectedAction {
				t.Errorf("expected action %s, got %s", tt.expectedAction, drpc.Spec.Action)
			}

			if drpc.Spec.PreferredCluster != tt.expectedPreferred {
				t.Errorf("expected preferred %s, got %s", tt.expectedPreferred, drpc.Spec.PreferredCluster)
			}

			if drpc.Spec.FailoverCluster != tt.expectedFailover {
				t.Errorf("expected failover %s, got %s", tt.expectedFailover, drpc.Spec.FailoverCluster)
			}
		})
	}
}

func TestDRPCStatusBuilder(t *testing.T) {
	tests := []struct {
		name              string
		builderFn         func() *DRPCStatusBuilder
		expectedPhase     ramen.DRState
		expectedCluster   string
		conditionCount    int
	}{
		{
			name: "basic status",
			builderFn: func() *DRPCStatusBuilder {
				return NewDRPCStatusBuilder()
			},
			expectedPhase: ramen.Deployed,
		},
		{
			name: "relocating status",
			builderFn: func() *DRPCStatusBuilder {
				return NewDRPCStatusBuilder().
					WithPhase(ramen.Relocating).
					WithPreferredDecision("cluster1")
			},
			expectedPhase:   ramen.Relocating,
			expectedCluster: "cluster1",
		},
		{
			name: "status with conditions",
			builderFn: func() *DRPCStatusBuilder {
				return NewDRPCStatusBuilder().
					WithCondition("Available", metav1.ConditionTrue, "Ready", "").
					WithCondition("PeerReady", metav1.ConditionTrue, "Ready", "")
			},
			expectedPhase:  ramen.Deployed,
			conditionCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			status := tt.builderFn().Build()

			if status.Phase != tt.expectedPhase {
				t.Errorf("expected phase %s, got %s", tt.expectedPhase, status.Phase)
			}

			if tt.expectedCluster != "" && status.PreferredDecision.ClusterName != tt.expectedCluster {
				t.Errorf("expected cluster %s, got %s", tt.expectedCluster, status.PreferredDecision.ClusterName)
			}

			if len(status.Conditions) != tt.conditionCount {
				t.Errorf("expected %d conditions, got %d", tt.conditionCount, len(status.Conditions))
			}
		})
	}
}
