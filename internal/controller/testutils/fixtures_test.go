// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"testing"

	csiaddonsv1alpha1 "github.com/csi-addons/kubernetes-csi-addons/api/csiaddons/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ocmv1 "open-cluster-management.io/api/cluster/v1"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

func TestDRClusterBuilder(t *testing.T) {
	drcluster := NewDRClusterBuilder("test-cluster").
		WithS3Profile("test-profile").
		WithRegion("us-west-2").
		WithCIDRs([]string{"10.0.0.0/8"}).
		WithClusterFence(ramen.ClusterFenceStateFenced).
		Build()

	if drcluster.Name != "test-cluster" {
		t.Errorf("expected name 'test-cluster', got '%s'", drcluster.Name)
	}

	if drcluster.Spec.S3ProfileName != "test-profile" {
		t.Errorf("expected S3ProfileName 'test-profile', got '%s'", drcluster.Spec.S3ProfileName)
	}

	if drcluster.Spec.Region != "us-west-2" {
		t.Errorf("expected Region 'us-west-2', got '%s'", drcluster.Spec.Region)
	}

	if len(drcluster.Spec.CIDRs) != 1 || drcluster.Spec.CIDRs[0] != "10.0.0.0/8" {
		t.Errorf("expected CIDRs ['10.0.0.0/8'], got %v", drcluster.Spec.CIDRs)
	}

	if drcluster.Spec.ClusterFence != ramen.ClusterFenceStateFenced {
		t.Errorf("expected ClusterFence 'Fenced', got '%s'", drcluster.Spec.ClusterFence)
	}
}

func TestDRClusterBuilderWithAnnotations(t *testing.T) {
	// Test custom annotations
	drcluster := NewDRClusterBuilder("test-cluster").
		WithAnnotation("custom-key", "custom-value").
		WithLabel("env", "test").
		Build()

	if drcluster.Annotations["custom-key"] != "custom-value" {
		t.Errorf("expected annotation 'custom-key=custom-value', got '%s'", drcluster.Annotations["custom-key"])
	}

	if drcluster.Labels["env"] != "test" {
		t.Errorf("expected label 'env=test', got '%s'", drcluster.Labels["env"])
	}

	// Test storage annotations helper
	drclusterWithStorage := NewDRClusterBuilder("storage-cluster").
		WithStorageAnnotations("secret-name", "secret-ns", "cluster-id", "csi.driver.com").
		Build()

	expectedAnnotations := map[string]string{
		DRClusterStorageSecretNameAnnotation:      "secret-name",
		DRClusterStorageSecretNamespaceAnnotation: "secret-ns",
		DRClusterStorageClusterIDAnnotation:       "cluster-id",
		DRClusterStorageDriverAnnotation:          "csi.driver.com",
	}

	for key, expected := range expectedAnnotations {
		if drclusterWithStorage.Annotations[key] != expected {
			t.Errorf("expected annotation '%s=%s', got '%s'", key, expected, drclusterWithStorage.Annotations[key])
		}
	}

	// Test default storage annotations
	drclusterDefault := NewDRClusterBuilder("default-storage-cluster").
		WithDefaultStorageAnnotations().
		Build()

	if drclusterDefault.Annotations[DRClusterStorageSecretNameAnnotation] != "tmp" {
		t.Error("expected default storage secret name annotation to be 'tmp'")
	}
}

func TestDRPolicyBuilder(t *testing.T) {
	drpolicy := NewDRPolicyBuilder("test-policy").
		WithDRClusters([]string{"cluster1", "cluster2"}).
		WithSchedulingInterval("10m").
		Build()

	if drpolicy.Name != "test-policy" {
		t.Errorf("expected name 'test-policy', got '%s'", drpolicy.Name)
	}

	if len(drpolicy.Spec.DRClusters) != 2 {
		t.Errorf("expected 2 DRClusters, got %d", len(drpolicy.Spec.DRClusters))
	}

	if drpolicy.Spec.SchedulingInterval != "10m" {
		t.Errorf("expected SchedulingInterval '10m', got '%s'", drpolicy.Spec.SchedulingInterval)
	}
}

func TestStorageClassBuilder(t *testing.T) {
	sc := NewStorageClassBuilder("test-sc").
		WithProvisioner("csi.example.com").
		WithStorageID("storage-1").
		WithLabel("env", "test").
		Build()

	if sc.Name != "test-sc" {
		t.Errorf("expected name 'test-sc', got '%s'", sc.Name)
	}

	if sc.Provisioner != "csi.example.com" {
		t.Errorf("expected Provisioner 'csi.example.com', got '%s'", sc.Provisioner)
	}

	if sc.Labels[StorageIDLabel] != "storage-1" {
		t.Errorf("expected StorageID label 'storage-1', got '%s'", sc.Labels[StorageIDLabel])
	}

	if sc.Labels["env"] != "test" {
		t.Errorf("expected label 'env=test', got '%s'", sc.Labels["env"])
	}
}

func TestNetworkFenceBuilder(t *testing.T) {
	nf := NewNetworkFenceBuilder("cluster0").
		WithCIDRs([]string{"192.168.1.0/24"}).
		WithFenceState(csiaddonsv1alpha1.Unfenced).
		WithFenceClassName("test-class").
		Build()

	expectedName := "network-fence-cluster0"
	if nf.Name != expectedName {
		t.Errorf("expected name '%s', got '%s'", expectedName, nf.Name)
	}

	if nf.Spec.FenceState != csiaddonsv1alpha1.Unfenced {
		t.Errorf("expected FenceState 'Unfenced', got '%s'", nf.Spec.FenceState)
	}

	if nf.Spec.NetworkFenceClassName != "test-class" {
		t.Errorf("expected NetworkFenceClassName 'test-class', got '%s'", nf.Spec.NetworkFenceClassName)
	}
}

func TestManagedClusterBuilder(t *testing.T) {
	mc := NewManagedClusterBuilder("test-mc").
		WithHubAcceptsClient(true).
		WithJoined(true).
		WithClusterID("cluster-123").
		Build()

	if mc.Name != "test-mc" {
		t.Errorf("expected name 'test-mc', got '%s'", mc.Name)
	}

	if !mc.Spec.HubAcceptsClient {
		t.Error("expected HubAcceptsClient to be true")
	}

	// Check status is set for joined cluster
	found := false

	for _, cond := range mc.Status.Conditions {
		if cond.Type == ocmv1.ManagedClusterConditionJoined && cond.Status == metav1.ConditionTrue {
			found = true

			break
		}
	}

	if !found {
		t.Error("expected joined condition to be true")
	}

	// Check cluster ID claim
	clusterIDFound := false

	for _, claim := range mc.Status.ClusterClaims {
		if claim.Name == "id.k8s.io" && claim.Value == "cluster-123" {
			clusterIDFound = true

			break
		}
	}

	if !clusterIDFound {
		t.Error("expected cluster ID claim 'cluster-123'")
	}
}
