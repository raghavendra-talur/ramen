// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

const (
	HubName = "hub"
	DR1Name = "dr1"
	DR2Name = "dr2"

	RamenSystemNS = "ramen-system" // POD_NAMESPACE for both operator types
	RamenOpsNS    = "ramen-ops"    // discovered-app management namespace

	DRPolicyName       = "dr-policy-1m"
	SchedulingInterval = "1m"

	StorageClassName = "mock-rbd"
	VRClassName      = "mock-rbd-vrc"
	Provisioner      = "mock.csi.ramen.io"
	ReplicationID    = "mock-replication-rbd"

	// The cephfs profile: a StorageClass with a VolumeSnapshotClass and
	// deliberately NO replication class — the absence routes PVCs of this
	// class to Ramen's VolSync path.
	CephFSStorageClassName = "mock-cephfs"
	CephFSVSClassName      = "mock-cephfs-vsc"
	CephFSProvisioner      = "mock.cephfs.csi.ramen.io"

	// The consistency-group stories. Each is an isolated storageid
	// universe: peer-class grouping matches group classes to
	// StorageClasses by storageid, so sharing ids with the plain stories
	// would flip their peers to Grouping too. mock-rbd-cg groups through
	// VolumeGroupReplication (SC + VRClass + VGRClass); mock-cephfs-cg
	// groups through VolumeGroupSnapshot on the public v1 API
	// (SC + VolumeSnapshotClass + VolumeGroupSnapshotClass).
	CGStorageClassName = "mock-rbd-cg"
	CGVRClassName      = "mock-rbd-cg-vrc"
	VGRClassName       = "mock-rbd-cg-vgrc"
	CGReplicationID    = "mock-replication-rbd-cg"
	GroupReplicationID = "mock-group-replication-rbd"

	CGCephFSStorageClassName = "mock-cephfs-cg"
	CGCephFSVSClassName      = "mock-cephfs-cg-vsc"
	CephFSVGSClassName       = "mock-cephfs-cg-vgsc"

	// Label keys ramen matches on (see internal/controller/volumereplicationgroup_controller.go).
	StorageIDLabel          = "ramendr.openshift.io/storageid"
	ReplicationIDLabel      = "ramendr.openshift.io/replicationid"
	GroupReplicationIDLabel = "ramendr.openshift.io/groupreplicationid"

	S3SecretName    = "ramen-s3-secret"
	S3AccessKey     = "simtest"
	S3SecretKey     = "simtest123"
	S3ProfilePrefix = "s3-" // profile name: s3-dr1, s3-dr2; bucket: bucket-dr1, ...

	HubConfigMapName = "ramen-hub-operator-config"
	DRConfigMapName  = "ramen-dr-cluster-operator-config"
	ConfigMapKey     = "ramen_manager_config.yaml"

	// OCM annotation that stops OCM from scheduling; ramen owns PlacementDecisions.
	OcmSchedulingDisable = "cluster.open-cluster-management.io/experimental-scheduling-disable"

	AppLabelKey = "appname" // pvcSelector label key, mirrors e2e
)

func StorageID(cluster string) string { return "mock-rbd-" + cluster }

func CephFSStorageID(cluster string) string { return "mock-cephfs-" + cluster }

func CGStorageID(cluster string) string { return "mock-rbd-cg-" + cluster }

func CGCephFSStorageID(cluster string) string { return "mock-cephfs-cg-" + cluster }
func S3Profile(cluster string) string         { return S3ProfilePrefix + cluster }
func S3Bucket(cluster string) string          { return "bucket-" + cluster }
