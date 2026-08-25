// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"context"
	"fmt"
	groupsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ocmv1 "open-cluster-management.io/api/cluster/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// bootstrap seeds every object the operators expect to find. It runs before
// the manager processes start.
func bootstrap(ctx context.Context, hub *Cluster, managed []*Cluster, s3URL string) error {
	// Hub: system namespaces + one namespace per managed cluster (MW/MCV home).
	hubNamespaces := []string{RamenSystemNS, RamenOpsNS}
	for _, m := range managed {
		hubNamespaces = append(hubNamespaces, m.Name)
	}
	if err := createNamespaces(ctx, hub.Client, hubNamespaces...); err != nil {
		return err
	}

	if err := createS3Secret(ctx, hub.Client); err != nil {
		return err
	}
	if err := createConfigMap(ctx, hub.Client, "dr-hub", s3URL); err != nil {
		return err
	}

	for _, m := range managed {
		if err := createNamespaces(ctx, m.Client, RamenSystemNS, RamenOpsNS); err != nil {
			return err
		}
		if err := createS3Secret(ctx, m.Client); err != nil {
			return err
		}
		if err := createConfigMap(ctx, m.Client, "dr-cluster", s3URL); err != nil {
			return err
		}
		if err := createStorageClasses(ctx, m); err != nil {
			return err
		}
		if err := createManagedCluster(ctx, hub.Client, m.Name); err != nil {
			return err
		}
	}

	if err := createDRClusters(ctx, hub.Client, managed); err != nil {
		return err
	}

	return createDRPolicy(ctx, hub.Client, managed)
}

func createNamespaces(ctx context.Context, c client.Client, names ...string) error {
	for _, n := range names {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: n}}
		if err := client.IgnoreAlreadyExists(c.Create(ctx, ns)); err != nil {
			return err
		}
	}

	return nil
}

func createS3Secret(ctx context.Context, c client.Client) error {
	sec := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: S3SecretName, Namespace: RamenSystemNS},
		StringData: map[string]string{
			"AWS_ACCESS_KEY_ID":     S3AccessKey,
			"AWS_SECRET_ACCESS_KEY": S3SecretKey,
		},
	}

	return client.IgnoreAlreadyExists(c.Create(ctx, sec))
}

func createConfigMap(ctx context.Context, c client.Client, controllerType, s3URL string) error {
	cm, err := OperatorConfigMap(controllerType, s3URL)
	if err != nil {
		return err
	}

	return client.IgnoreAlreadyExists(c.Create(ctx, cm))
}

// createStorageClasses seeds the volrep storage/replication classes, labeled
// the way ramen's class matching and DRClusterConfig discovery require
// (mirrors ramen-mock's internal/mockenv/classes.go).
func createStorageClasses(ctx context.Context, m *Cluster) error {
	sc := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: StorageClassName,
			Labels: map[string]string{
				StorageIDLabel:     StorageID(m.Name),
				ReplicationIDLabel: ReplicationID,
			},
		},
		Provisioner: Provisioner,
	}
	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, sc)); err != nil {
		return err
	}

	vrc := &volrep.VolumeReplicationClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: VRClassName,
			Labels: map[string]string{
				StorageIDLabel:     StorageID(m.Name),
				ReplicationIDLabel: ReplicationID,
			},
			Annotations: map[string]string{"replication.storage.openshift.io/is-default-class": "true"},
		},
		Spec: volrep.VolumeReplicationClassSpec{
			Provisioner: Provisioner,
			Parameters:  map[string]string{"schedulingInterval": SchedulingInterval},
		},
	}

	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, vrc)); err != nil {
		return err
	}

	// The rbd consistency-group pair: the SC adds groupreplicationid, and
	// the VolumeGroupReplicationClass (matching labels, provisioner, and
	// scheduling interval) is what DRClusterConfig discovers to mark the
	// rbd-cg peer classes with Grouping.
	cgSC := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: CGStorageClassName,
			Labels: map[string]string{
				StorageIDLabel:          CGStorageID(m.Name),
				ReplicationIDLabel:      CGReplicationID,
				GroupReplicationIDLabel: GroupReplicationID,
			},
		},
		Provisioner: Provisioner,
	}
	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, cgSC)); err != nil {
		return err
	}

	// Non-offloaded grouping needs BOTH a VRClass and a VGRClass matching
	// the CG storageid.
	cgVRC := &volrep.VolumeReplicationClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: CGVRClassName,
			Labels: map[string]string{
				StorageIDLabel:     CGStorageID(m.Name),
				ReplicationIDLabel: CGReplicationID,
			},
		},
		Spec: volrep.VolumeReplicationClassSpec{
			Provisioner: Provisioner,
			Parameters:  map[string]string{"schedulingInterval": SchedulingInterval},
		},
	}
	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, cgVRC)); err != nil {
		return err
	}

	vgrc := &volrep.VolumeGroupReplicationClass{
		ObjectMeta: metav1.ObjectMeta{
			Name: VGRClassName,
			Labels: map[string]string{
				StorageIDLabel:          CGStorageID(m.Name),
				ReplicationIDLabel:      CGReplicationID,
				GroupReplicationIDLabel: GroupReplicationID,
			},
		},
		Spec: volrep.VolumeGroupReplicationClassSpec{
			Provisioner: Provisioner,
			Parameters: map[string]string{
				"schedulingInterval": SchedulingInterval,
				"replication.storage.openshift.io/group-replication-secret-name":      "mock-cg-secret",
				"replication.storage.openshift.io/group-replication-secret-namespace": "default",
			},
		},
	}
	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, vgrc)); err != nil {
		return err
	}

	// The cephfs pair: storageid-labeled StorageClass with NO replicationid
	// and no replication class — routing its PVCs to VolSync — plus the
	// VolumeSnapshotClass that provides the restore path.
	cephfsSC := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   CephFSStorageClassName,
			Labels: map[string]string{StorageIDLabel: CephFSStorageID(m.Name)},
		},
		Provisioner: CephFSProvisioner,
	}
	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, cephfsSC)); err != nil {
		return err
	}

	vsc := &snapv1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   CephFSVSClassName,
			Labels: map[string]string{StorageIDLabel: CephFSStorageID(m.Name)},
		},
		Driver:         CephFSProvisioner,
		DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
	}

	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, vsc)); err != nil {
		return err
	}

	// The cephfs consistency-group triple: its own storageid so the plain
	// cephfs peers stay ungrouped, plus the snapshot and group-snapshot
	// classes that make its peers report Grouping.
	cgFS := &storagev1.StorageClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   CGCephFSStorageClassName,
			Labels: map[string]string{StorageIDLabel: CGCephFSStorageID(m.Name)},
		},
		Provisioner: CephFSProvisioner,
	}
	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, cgFS)); err != nil {
		return err
	}

	cgVSC := &snapv1.VolumeSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   CGCephFSVSClassName,
			Labels: map[string]string{StorageIDLabel: CGCephFSStorageID(m.Name)},
		},
		Driver:         CephFSProvisioner,
		DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
	}
	if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, cgVSC)); err != nil {
		return err
	}

	vgsc := &groupsnapv1.VolumeGroupSnapshotClass{
		ObjectMeta: metav1.ObjectMeta{
			Name:   CephFSVGSClassName,
			Labels: map[string]string{StorageIDLabel: CGCephFSStorageID(m.Name)},
		},
		Driver:         CephFSProvisioner,
		DeletionPolicy: snapv1.VolumeSnapshotContentDelete,
	}

	return client.IgnoreAlreadyExists(m.Client.Create(ctx, vgsc))
}

// createManagedCluster registers the cluster on the hub with the status ramen
// requires: Joined condition and the id.k8s.io cluster claim
// (internal/controller/util/managedcluster.go).
func createManagedCluster(ctx context.Context, hubClient client.Client, name string) error {
	mc := &ocmv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       ocmv1.ManagedClusterSpec{HubAcceptsClient: true},
	}
	if err := client.IgnoreAlreadyExists(hubClient.Create(ctx, mc)); err != nil {
		return err
	}
	if err := hubClient.Get(ctx, client.ObjectKeyFromObject(mc), mc); err != nil {
		return err
	}

	mc.Status = ocmv1.ManagedClusterStatus{
		Conditions: []metav1.Condition{{
			Type: ocmv1.ManagedClusterConditionJoined, Status: metav1.ConditionTrue,
			Reason: "Joined", Message: "simtest", LastTransitionTime: metav1.Now(),
		}},
		ClusterClaims: []ocmv1.ManagedClusterClaim{{Name: "id.k8s.io", Value: name + "-cluster-id"}},
		Version:       ocmv1.ManagedClusterVersion{Kubernetes: "v1.33.0"},
	}

	return hubClient.Status().Update(ctx, mc)
}

func createDRClusters(ctx context.Context, hubClient client.Client, managed []*Cluster) error {
	for _, m := range managed {
		drc := &rmn.DRCluster{
			ObjectMeta: metav1.ObjectMeta{Name: m.Name},
			Spec:       rmn.DRClusterSpec{S3ProfileName: S3Profile(m.Name)},
		}
		if err := client.IgnoreAlreadyExists(hubClient.Create(ctx, drc)); err != nil {
			return err
		}
	}

	return nil
}

func createDRPolicy(ctx context.Context, hubClient client.Client, managed []*Cluster) error {
	names := make([]string, 0, len(managed))
	for _, m := range managed {
		names = append(names, m.Name)
	}

	if len(names) != 2 {
		return fmt.Errorf("drpolicy needs exactly 2 clusters, got %d", len(names))
	}

	pol := &rmn.DRPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: DRPolicyName},
		Spec: rmn.DRPolicySpec{
			DRClusters:         names,
			SchedulingInterval: SchedulingInterval,
		},
	}

	return client.IgnoreAlreadyExists(hubClient.Create(ctx, pol))
}
