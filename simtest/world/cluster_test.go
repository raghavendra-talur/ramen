// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	groupsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// The world installs the public groupsnapshot.storage.k8s.io CRDs, and
// they MUST serve v1: ramen's public VGS client is v1 and ramen prefers the
// public API whenever that CRD exists, so a stale copy serving only v1beta1
// makes the drclusterconfig informer cache-sync time out and kills the
// dr-cluster manager two minutes into every run (the original matrix-killer
// this test guards against). The private openshift.io variant stays too, so
// public-first selection is a choice, not a default.
func TestHackTestCRDsIncludePublicGroupSnapshotV1(t *testing.T) {
	paths, err := hackTestCRDPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no CRD paths returned from hack/test")
	}

	var sawPublicVGS, sawPrivate bool

	for _, p := range paths {
		base := filepath.Base(p)
		if strings.HasPrefix(base, "groupsnapshot.storage.openshift.io_") {
			sawPrivate = true
		}

		if !strings.HasPrefix(base, "groupsnapshot.storage.k8s.io_") {
			continue
		}

		sawPublicVGS = true

		raw, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(string(raw), "name: v1\n") {
			t.Errorf("public groupsnapshot CRD %s does not serve v1; a stale copy kills the dr-cluster manager", base)
		}
	}

	if !sawPublicVGS {
		t.Error("public groupsnapshot.storage.k8s.io CRDs must be installed (CG VolSync runs on the public v1 API)")
	}
	if !sawPrivate {
		t.Error("private openshift.io groupsnapshot CRDs must remain")
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

// The CG storage stories are isolated storageid universes — peer-class
// grouping matches group classes to StorageClasses by storageid, so
// sharing storageids with the plain stories would silently flip THEIR
// peers to Grouping too (labeling plain rbd PVCs for CG then fails on the
// missing groupreplicationid). rbd-cg pairs SC + VRClass + VGRClass;
// cephfs-cg pairs SC + VolumeSnapshotClass + VolumeGroupSnapshotClass.
func TestCGClassesExist(t *testing.T) {
	s := NewScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	m := &Cluster{Name: DR1Name, Client: cl}

	if err := createStorageClasses(context.Background(), m); err != nil {
		t.Fatal(err)
	}

	sc := &storagev1.StorageClass{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: CGStorageClassName}, sc); err != nil {
		t.Fatalf("cg storage class: %v", err)
	}
	for label, want := range map[string]string{
		StorageIDLabel:          CGStorageID(DR1Name),
		ReplicationIDLabel:      CGReplicationID,
		GroupReplicationIDLabel: GroupReplicationID,
	} {
		if got := sc.Labels[label]; got != want {
			t.Fatalf("cg sc label %s = %q, want %q", label, got, want)
		}
	}

	vrc := &volrep.VolumeReplicationClass{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: CGVRClassName}, vrc); err != nil {
		t.Fatalf("cg vr class: %v", err)
	}
	if vrc.Labels[StorageIDLabel] != CGStorageID(DR1Name) || vrc.Labels[ReplicationIDLabel] != CGReplicationID {
		t.Fatalf("cg vr class labels: %v", vrc.Labels)
	}

	vgrc := &volrep.VolumeGroupReplicationClass{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: VGRClassName}, vgrc); err != nil {
		t.Fatalf("vgr class: %v", err)
	}
	if vgrc.Labels[GroupReplicationIDLabel] != GroupReplicationID ||
		vgrc.Labels[StorageIDLabel] != CGStorageID(DR1Name) {
		t.Fatalf("vgr class labels: %v", vgrc.Labels)
	}
	if vgrc.Spec.Provisioner != Provisioner ||
		vgrc.Spec.Parameters["schedulingInterval"] != SchedulingInterval {
		t.Fatalf("vgr class spec: %+v", vgrc.Spec)
	}

	cgfs := &storagev1.StorageClass{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: CGCephFSStorageClassName}, cgfs); err != nil {
		t.Fatalf("cg cephfs storage class: %v", err)
	}
	if cgfs.Labels[StorageIDLabel] != CGCephFSStorageID(DR1Name) {
		t.Fatalf("cg cephfs sc labels: %v", cgfs.Labels)
	}
	if _, has := cgfs.Labels[ReplicationIDLabel]; has {
		t.Fatal("cg cephfs sc must not carry replicationid (it routes to VolSync)")
	}

	vsc := &snapv1.VolumeSnapshotClass{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: CGCephFSVSClassName}, vsc); err != nil {
		t.Fatalf("cg cephfs vs class: %v", err)
	}
	if vsc.Driver != CephFSProvisioner || vsc.Labels[StorageIDLabel] != CGCephFSStorageID(DR1Name) {
		t.Fatalf("cg cephfs vs class: driver=%q labels=%v", vsc.Driver, vsc.Labels)
	}

	vgsc := &groupsnapv1.VolumeGroupSnapshotClass{}
	if err := cl.Get(context.Background(), types.NamespacedName{Name: CephFSVGSClassName}, vgsc); err != nil {
		t.Fatalf("vgs class: %v", err)
	}
	if vgsc.Driver != CephFSProvisioner || vgsc.Labels[StorageIDLabel] != CGCephFSStorageID(DR1Name) {
		t.Fatalf("vgs class: driver=%q labels=%v", vgsc.Driver, vgsc.Labels)
	}
}

// The plain stories must stay free of group classes: no VGRClass matches
// the plain rbd storageid and no VolumeGroupSnapshotClass matches the
// plain cephfs storageid, or their peer classes would report Grouping and
// change the non-CG baselines' semantics.
func TestPlainStoriesNotGrouped(t *testing.T) {
	s := NewScheme()
	cl := fake.NewClientBuilder().WithScheme(s).Build()
	m := &Cluster{Name: DR1Name, Client: cl}

	if err := createStorageClasses(context.Background(), m); err != nil {
		t.Fatal(err)
	}

	vgrcs := &volrep.VolumeGroupReplicationClassList{}
	if err := cl.List(context.Background(), vgrcs); err != nil {
		t.Fatal(err)
	}
	for i := range vgrcs.Items {
		if vgrcs.Items[i].Labels[StorageIDLabel] == StorageID(DR1Name) {
			t.Fatalf("VGRClass %s matches the plain rbd storageid", vgrcs.Items[i].Name)
		}
	}

	vgscs := &groupsnapv1.VolumeGroupSnapshotClassList{}
	if err := cl.List(context.Background(), vgscs); err != nil {
		t.Fatal(err)
	}
	for i := range vgscs.Items {
		if vgscs.Items[i].Labels[StorageIDLabel] == CephFSStorageID(DR1Name) {
			t.Fatalf("VGSClass %s matches the plain cephfs storageid", vgscs.Items[i].Name)
		}
	}
}
