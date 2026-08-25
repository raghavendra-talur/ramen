// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// The repo's hack/test CRD for the public groupsnapshot.storage.k8s.io group
// serves only v1beta1, while the ramen binary's public VGS client is v1.
// Ramen prefers the public API whenever that CRD exists, so installing it
// makes the drclusterconfig informer cache-sync time out and kills the
// dr-cluster manager two minutes into every run. The world must therefore
// install everything from hack/test EXCEPT the public groupsnapshot CRDs,
// leaving the private openshift.io variant whose version matches.
func TestHackTestCRDPathsExcludePublicGroupSnapshot(t *testing.T) {
	paths, err := hackTestCRDPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no CRD paths returned from hack/test")
	}

	var sawPrivate, sawOther bool
	for _, p := range paths {
		base := filepath.Base(p)
		if strings.HasPrefix(base, "groupsnapshot.storage.k8s.io_") {
			t.Errorf("public groupsnapshot CRD must be excluded: %s", p)
		}
		if strings.HasPrefix(base, "groupsnapshot.storage.openshift.io_") {
			sawPrivate = true
		}
		if strings.HasPrefix(base, "recipes.ramendr.openshift.io") {
			sawOther = true
		}
	}
	if !sawPrivate {
		t.Error("private openshift.io groupsnapshot CRDs must remain")
	}
	if !sawOther {
		t.Error("unrelated hack/test CRDs (e.g. recipes) must remain")
	}
}

// The cephfs profile routes PVCs to the VolSync path by construction: its
// StorageClass carries a storageid label but deliberately NO replication
// class or replicationid — the absence is what makes Ramen pick VolSync —
// while a VolumeSnapshotClass with the matching storageid provides the
// restore path.
func TestCephFSClassesRouteToVolSync(t *testing.T) {
	s := NewScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	m := &Cluster{Name: DR1Name, Client: cl}

	if err := createStorageClasses(context.Background(), m); err != nil {
		t.Fatal(err)
	}

	sc := &storagev1.StorageClass{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: CephFSStorageClassName}, sc); err != nil {
		t.Fatalf("cephfs storage class: %v", err)
	}
	if sc.Labels[StorageIDLabel] != CephFSStorageID(DR1Name) {
		t.Fatalf("cephfs SC storageid label = %q", sc.Labels[StorageIDLabel])
	}
	if _, has := sc.Labels[ReplicationIDLabel]; has {
		t.Fatal("cephfs SC must NOT carry a replicationid label (that would route to VolRep)")
	}

	vsc := &snapv1.VolumeSnapshotClass{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: CephFSVSClassName}, vsc); err != nil {
		t.Fatalf("cephfs volume snapshot class: %v", err)
	}
	if vsc.Driver != CephFSProvisioner || vsc.Labels[StorageIDLabel] != CephFSStorageID(DR1Name) {
		t.Fatalf("vsclass driver/labels wrong: driver=%q labels=%v", vsc.Driver, vsc.Labels)
	}
}
