// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"fmt"
	"strings"

	csiaddonsv1alpha1 "github.com/csi-addons/kubernetes-csi-addons/api/csiaddons/v1alpha1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ocmv1 "open-cluster-management.io/api/cluster/v1"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

const (
	// NetworkFencePrefix is the prefix for NetworkFence resources.
	NetworkFencePrefix = "network-fence"
	// StorageIDLabel is the label for storage ID.
	StorageIDLabel = "ramendr.openshift.io/storageid"
)

// DRClusterBuilder builds DRCluster test fixtures.
type DRClusterBuilder struct {
	name          string
	s3ProfileName string
	region        string
	cidrs         []string
	clusterFence  ramen.ClusterFenceState
}

// NewDRClusterBuilder creates a new DRClusterBuilder with defaults.
func NewDRClusterBuilder(name string) *DRClusterBuilder {
	return &DRClusterBuilder{
		name:   name,
		region: "us-east-1",
	}
}

// WithS3Profile sets the S3 profile name.
func (b *DRClusterBuilder) WithS3Profile(name string) *DRClusterBuilder {
	b.s3ProfileName = name

	return b
}

// WithRegion sets the region.
func (b *DRClusterBuilder) WithRegion(region string) *DRClusterBuilder {
	b.region = region

	return b
}

// WithCIDRs sets the CIDRs.
func (b *DRClusterBuilder) WithCIDRs(cidrs []string) *DRClusterBuilder {
	b.cidrs = cidrs

	return b
}

// WithClusterFence sets the cluster fence state.
func (b *DRClusterBuilder) WithClusterFence(state ramen.ClusterFenceState) *DRClusterBuilder {
	b.clusterFence = state

	return b
}

// Build creates the DRCluster.
func (b *DRClusterBuilder) Build() *ramen.DRCluster {
	return &ramen.DRCluster{
		ObjectMeta: metav1.ObjectMeta{Name: b.name},
		Spec: ramen.DRClusterSpec{
			S3ProfileName: b.s3ProfileName,
			Region:        ramen.Region(b.region),
			CIDRs:         b.cidrs,
			ClusterFence:  b.clusterFence,
		},
	}
}

// DRPolicyBuilder builds DRPolicy test fixtures.
type DRPolicyBuilder struct {
	name             string
	namespace        string
	drClusters       []string
	schedulingIntrvl string
}

// NewDRPolicyBuilder creates a new DRPolicyBuilder with defaults.
func NewDRPolicyBuilder(name string) *DRPolicyBuilder {
	return &DRPolicyBuilder{
		name:             name,
		schedulingIntrvl: "5m",
	}
}

// WithDRClusters sets the DR clusters.
func (b *DRPolicyBuilder) WithDRClusters(clusters []string) *DRPolicyBuilder {
	b.drClusters = clusters

	return b
}

// WithSchedulingInterval sets the scheduling interval.
func (b *DRPolicyBuilder) WithSchedulingInterval(interval string) *DRPolicyBuilder {
	b.schedulingIntrvl = interval

	return b
}

// Build creates the DRPolicy.
func (b *DRPolicyBuilder) Build() *ramen.DRPolicy {
	return &ramen.DRPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: b.name},
		Spec: ramen.DRPolicySpec{
			DRClusters:         b.drClusters,
			SchedulingInterval: b.schedulingIntrvl,
		},
	}
}

// StorageClassBuilder builds StorageClass test fixtures.
type StorageClassBuilder struct {
	name        string
	provisioner string
	storageID   string
	labels      map[string]string
}

// NewStorageClassBuilder creates a new StorageClassBuilder with defaults.
func NewStorageClassBuilder(name string) *StorageClassBuilder {
	return &StorageClassBuilder{
		name:        name,
		provisioner: "fake.ramen.com",
		labels:      make(map[string]string),
	}
}

// WithProvisioner sets the provisioner.
func (b *StorageClassBuilder) WithProvisioner(provisioner string) *StorageClassBuilder {
	b.provisioner = provisioner

	return b
}

// WithStorageID sets the storage ID label.
func (b *StorageClassBuilder) WithStorageID(storageID string) *StorageClassBuilder {
	b.storageID = storageID
	b.labels[StorageIDLabel] = storageID

	return b
}

// WithLabel adds a label.
func (b *StorageClassBuilder) WithLabel(key, value string) *StorageClassBuilder {
	b.labels[key] = value

	return b
}

// Build creates the StorageClass.
func (b *StorageClassBuilder) Build() *storagev1.StorageClass {
	return &storagev1.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind:       "StorageClass",
			APIVersion: "storage.k8s.io/v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:   b.name,
			Labels: b.labels,
		},
		Provisioner: b.provisioner,
	}
}

// NetworkFenceBuilder builds NetworkFence test fixtures.
type NetworkFenceBuilder struct {
	name            string
	cidrs           []string
	fenceState      csiaddonsv1alpha1.FenceState
	fenceClassName  string
	driverNamespace string
}

// NewNetworkFenceBuilder creates a new NetworkFenceBuilder with defaults.
func NewNetworkFenceBuilder(clusterName string) *NetworkFenceBuilder {
	return &NetworkFenceBuilder{
		name:       strings.Join([]string{NetworkFencePrefix, clusterName}, "-"),
		fenceState: csiaddonsv1alpha1.Fenced,
		cidrs:      []string{"198.51.100.17/24", "198.51.100.18/24"},
	}
}

// WithName sets the name explicitly.
func (b *NetworkFenceBuilder) WithName(name string) *NetworkFenceBuilder {
	b.name = name

	return b
}

// WithCIDRs sets the CIDRs.
func (b *NetworkFenceBuilder) WithCIDRs(cidrs []string) *NetworkFenceBuilder {
	b.cidrs = cidrs

	return b
}

// WithFenceState sets the fence state.
func (b *NetworkFenceBuilder) WithFenceState(state csiaddonsv1alpha1.FenceState) *NetworkFenceBuilder {
	b.fenceState = state

	return b
}

// WithFenceClassName sets the fence class name.
func (b *NetworkFenceBuilder) WithFenceClassName(className string) *NetworkFenceBuilder {
	b.fenceClassName = className

	return b
}

// Build creates the NetworkFence.
func (b *NetworkFenceBuilder) Build() *csiaddonsv1alpha1.NetworkFence {
	nf := &csiaddonsv1alpha1.NetworkFence{
		TypeMeta: metav1.TypeMeta{
			Kind:       "NetworkFence",
			APIVersion: "csiaddons.openshift.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{Name: b.name},
		Spec: csiaddonsv1alpha1.NetworkFenceSpec{
			Cidrs:      b.cidrs,
			FenceState: b.fenceState,
		},
	}

	if b.fenceClassName != "" {
		nf.Spec.NetworkFenceClassName = b.fenceClassName
	}

	return nf
}

// NetworkFenceClassBuilder builds NetworkFenceClass test fixtures.
type NetworkFenceClassBuilder struct {
	name        string
	provisioner string
	parameters  map[string]string
	storageID   string
}

// NewNetworkFenceClassBuilder creates a new NetworkFenceClassBuilder.
func NewNetworkFenceClassBuilder(name string) *NetworkFenceClassBuilder {
	return &NetworkFenceClassBuilder{
		name:        name,
		provisioner: "fake.ramen.com",
		parameters: map[string]string{
			"clusterID": "rook-ceph",
			"csiaddons.openshift.io/networkfence-secret-name":      "rook-csi-rbd-provisioner",
			"csiaddons.openshift.io/networkfence-secret-namespace": "rook-ceph",
		},
	}
}

// WithProvisioner sets the provisioner.
func (b *NetworkFenceClassBuilder) WithProvisioner(provisioner string) *NetworkFenceClassBuilder {
	b.provisioner = provisioner

	return b
}

// WithParameter adds a parameter.
func (b *NetworkFenceClassBuilder) WithParameter(key, value string) *NetworkFenceClassBuilder {
	b.parameters[key] = value

	return b
}

// WithStorageID sets the storage ID annotation.
func (b *NetworkFenceClassBuilder) WithStorageID(storageID string) *NetworkFenceClassBuilder {
	b.storageID = storageID

	return b
}

// Build creates the NetworkFenceClass.
func (b *NetworkFenceClassBuilder) Build() *csiaddonsv1alpha1.NetworkFenceClass {
	nfc := &csiaddonsv1alpha1.NetworkFenceClass{
		TypeMeta: metav1.TypeMeta{
			Kind:       "NetworkFenceClass",
			APIVersion: "csiaddons.openshift.io/v1alpha1",
		},
		ObjectMeta: metav1.ObjectMeta{Name: b.name},
		Spec: csiaddonsv1alpha1.NetworkFenceClassSpec{
			Parameters:  b.parameters,
			Provisioner: b.provisioner,
		},
	}

	if b.storageID != "" {
		nfc.Annotations = map[string]string{StorageIDLabel: b.storageID}
	}

	return nfc
}

// DRClusterConfigBuilder builds DRClusterConfig test fixtures.
type DRClusterConfigBuilder struct {
	name                 string
	clusterID            string
	storageClasses       []string
	networkFenceClasses  []string
	replicationSchedules []string
}

// NewDRClusterConfigBuilder creates a new DRClusterConfigBuilder.
func NewDRClusterConfigBuilder(name string) *DRClusterConfigBuilder {
	return &DRClusterConfigBuilder{
		name:      name,
		clusterID: fmt.Sprintf("%s-cid", name),
	}
}

// WithClusterID sets the cluster ID.
func (b *DRClusterConfigBuilder) WithClusterID(id string) *DRClusterConfigBuilder {
	b.clusterID = id

	return b
}

// WithStorageClasses sets the storage classes.
func (b *DRClusterConfigBuilder) WithStorageClasses(classes []string) *DRClusterConfigBuilder {
	b.storageClasses = classes

	return b
}

// WithNetworkFenceClasses sets the network fence classes.
func (b *DRClusterConfigBuilder) WithNetworkFenceClasses(classes []string) *DRClusterConfigBuilder {
	b.networkFenceClasses = classes

	return b
}

// WithReplicationSchedules sets the replication schedules.
func (b *DRClusterConfigBuilder) WithReplicationSchedules(schedules []string) *DRClusterConfigBuilder {
	b.replicationSchedules = schedules

	return b
}

// Build creates the DRClusterConfig.
func (b *DRClusterConfigBuilder) Build() *ramen.DRClusterConfig {
	drcc := &ramen.DRClusterConfig{
		ObjectMeta: metav1.ObjectMeta{Name: b.name},
		Spec: ramen.DRClusterConfigSpec{
			ClusterID:            b.clusterID,
			ReplicationSchedules: b.replicationSchedules,
		},
	}

	drcc.Status.StorageClasses = b.storageClasses
	drcc.Status.NetworkFenceClasses = b.networkFenceClasses

	return drcc
}

// ManagedClusterBuilder builds ManagedCluster test fixtures.
type ManagedClusterBuilder struct {
	name             string
	hubAcceptsClient bool
	joined           bool
	clusterID        string
}

// NewManagedClusterBuilder creates a new ManagedClusterBuilder.
func NewManagedClusterBuilder(name string) *ManagedClusterBuilder {
	return &ManagedClusterBuilder{
		name:             name,
		hubAcceptsClient: true,
		joined:           true,
		clusterID:        "fake",
	}
}

// WithHubAcceptsClient sets whether the hub accepts the client.
func (b *ManagedClusterBuilder) WithHubAcceptsClient(accepts bool) *ManagedClusterBuilder {
	b.hubAcceptsClient = accepts

	return b
}

// WithJoined sets whether the cluster is joined.
func (b *ManagedClusterBuilder) WithJoined(joined bool) *ManagedClusterBuilder {
	b.joined = joined

	return b
}

// WithClusterID sets the cluster ID.
func (b *ManagedClusterBuilder) WithClusterID(id string) *ManagedClusterBuilder {
	b.clusterID = id

	return b
}

// Build creates the ManagedCluster.
func (b *ManagedClusterBuilder) Build() *ocmv1.ManagedCluster {
	mc := &ocmv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: b.name},
		Spec:       ocmv1.ManagedClusterSpec{HubAcceptsClient: b.hubAcceptsClient},
	}

	if b.joined {
		mc.Status = ocmv1.ManagedClusterStatus{
			Conditions: []metav1.Condition{
				{
					Type:   ocmv1.ManagedClusterConditionJoined,
					Status: metav1.ConditionTrue,
					Reason: ocmv1.ManagedClusterConditionJoined,
				},
			},
			ClusterClaims: []ocmv1.ManagedClusterClaim{
				{Name: "id.k8s.io", Value: b.clusterID},
			},
		}
	}

	return mc
}
