// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	rmn "github.com/ramendr/ramen/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// envtest has no garbage collector, so the volsync actor's materialized
// artifacts — the mock destination PVC and the latestImage snapshot — must be
// swept by the janitor once their ReplicationDestination is gone; while the
// RD lives they must be left alone.
func TestJanitorSweepsOrphanedVolSyncArtifacts(t *testing.T) {
	liveRD := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "kept", Namespace: "app-ns"},
	}
	keptPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "mock-volsync-dst-kept", Namespace: "app-ns"},
	}
	keptSnap := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "mock-latestimage-kept", Namespace: "app-ns"},
	}
	orphanPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "mock-volsync-dst-gone", Namespace: "app-ns"},
	}
	orphanSnap := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "mock-latestimage-gone", Namespace: "app-ns"},
	}
	plainPVC := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "app-data", Namespace: "app-ns"},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithObjects(liveRD, keptPVC, keptSnap, orphanPVC, orphanSnap, plainPVC).Build()
	ctx := context.Background()

	sweep(ctx, cl, "dr1", newTestRuntime(t))

	for name, want := range map[string]bool{
		"mock-volsync-dst-kept": true,
		"mock-volsync-dst-gone": false,
		"app-data":              true,
	} {
		err := cl.Get(ctx, types.NamespacedName{Namespace: "app-ns", Name: name}, &corev1.PersistentVolumeClaim{})
		if got := err == nil; got != want {
			t.Errorf("pvc %s exists=%v want %v (err=%v)", name, got, want, err)
		}
	}

	for name, want := range map[string]bool{
		"mock-latestimage-kept": true,
		"mock-latestimage-gone": false,
	} {
		err := cl.Get(ctx, types.NamespacedName{Namespace: "app-ns", Name: name}, &snapv1.VolumeSnapshot{})
		if got := err == nil; got != want {
			t.Errorf("snapshot %s exists=%v want %v (err=%v)", name, got, want, err)
		}
	}
}

// The janitor also stands in for the garbage collector on the CG artifacts:
// a VolumeGroupReplicationContent (cluster-scoped, so it cannot be owned by
// its namespaced VGR) is swept once the VGR it references is gone, and a
// VGS member snapshot is swept once its owning VolumeGroupSnapshot is gone.
func TestJanitorSweepsOrphanedCGArtifacts(t *testing.T) {
	liveVGR := &volrep.VolumeGroupReplication{
		ObjectMeta: metav1.ObjectMeta{Name: "vgr-kept", Namespace: "app-ns"},
	}
	keptVGRC := &volrep.VolumeGroupReplicationContent{
		ObjectMeta: metav1.ObjectMeta{Name: "mock-vgrc-vgr-kept"},
		Spec: volrep.VolumeGroupReplicationContentSpec{
			VolumeGroupReplicationRef:    &corev1.ObjectReference{Name: "vgr-kept", Namespace: "app-ns"},
			VolumeGroupReplicationHandle: "h", Provisioner: "p", VolumeGroupReplicationClassName: "c",
		},
	}
	orphanVGRC := &volrep.VolumeGroupReplicationContent{
		ObjectMeta: metav1.ObjectMeta{Name: "mock-vgrc-vgr-gone"},
		Spec: volrep.VolumeGroupReplicationContentSpec{
			VolumeGroupReplicationRef:    &corev1.ObjectReference{Name: "vgr-gone", Namespace: "app-ns"},
			VolumeGroupReplicationHandle: "h", Provisioner: "p", VolumeGroupReplicationClassName: "c",
		},
	}
	orphanMember := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "mock-vgs-gone-cg-data", Namespace: "app-ns",
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "groupsnapshot.storage.k8s.io/v1", Kind: "VolumeGroupSnapshot",
				Name: "gone-cg", UID: "u1",
			}},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithObjects(liveVGR, keptVGRC, orphanVGRC, orphanMember).Build()
	ctx := context.Background()

	sweep(ctx, cl, "dr1", newTestRuntime(t))

	if err := cl.Get(ctx, types.NamespacedName{Name: "mock-vgrc-vgr-kept"},
		&volrep.VolumeGroupReplicationContent{}); err != nil {
		t.Fatalf("live VGR's content swept: %v", err)
	}
	if err := cl.Get(ctx, types.NamespacedName{Name: "mock-vgrc-vgr-gone"},
		&volrep.VolumeGroupReplicationContent{}); err == nil {
		t.Fatal("orphaned VGRC must be swept")
	}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: "app-ns", Name: "mock-vgs-gone-cg-data"},
		&snapv1.VolumeSnapshot{}); err == nil {
		t.Fatal("member snapshot of a deleted VGS must be swept")
	}
}

// Ramen deletes its final-sync mount jobs with Foreground propagation,
// which parks a foregroundDeletion finalizer on the Job for the garbage
// collector to clear — envtest runs none, so the janitor must clear it or
// the final sync waits on the deletion forever.
func TestJanitorClearsForegroundDeletionOnJobs(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "volsync-pvc-mount-data-for-finalsync", Namespace: "app-ns",
			Finalizers: []string{metav1.FinalizerDeleteDependents},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).WithObjects(job).Build()
	ctx := context.Background()

	if err := cl.Delete(ctx, job); err != nil {
		t.Fatal(err)
	}

	sweep(ctx, cl, "dr1", newTestRuntime(t))

	got := &batchv1.Job{}
	if err := cl.Get(ctx,
		types.NamespacedName{Namespace: "app-ns", Name: "volsync-pvc-mount-data-for-finalsync"}, got); err == nil {
		t.Fatalf("deleting job must go away once the finalizer is cleared, still has %v", got.Finalizers)
	}
}

func rgOwned(kind, name string) []metav1.OwnerReference {
	ctrl := true

	return []metav1.OwnerReference{{
		APIVersion: "ramendr.openshift.io/v1alpha1", Kind: kind, Name: name, UID: "u", Controller: &ctrl,
	}}
}

// ReplicationGroupSource/Destination controller-own their per-PVC
// ReplicationSources/Destinations; ramen deletes the parents and relies on
// the garbage collector to cascade — envtest runs none, so the janitor
// must sweep the orphaned children (found leaking by the cephfs-cg seed's
// per-app residue check).
func TestJanitorSweepsOrphanedRGChildren(t *testing.T) {
	liveRGS := &rmn.ReplicationGroupSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rgs-kept", Namespace: "app-ns"},
	}
	keptRS := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rs-kept", Namespace: "app-ns",
			OwnerReferences: rgOwned("ReplicationGroupSource", "rgs-kept")},
	}
	orphanRS := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rs-gone", Namespace: "app-ns",
			OwnerReferences: rgOwned("ReplicationGroupSource", "rgs-gone")},
	}
	orphanRD := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "rd-gone", Namespace: "app-ns",
			OwnerReferences: rgOwned("ReplicationGroupDestination", "rgd-gone")},
	}
	plainRS := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "rs-plain", Namespace: "app-ns"},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithObjects(liveRGS, keptRS, orphanRS, orphanRD, plainRS).Build()
	ctx := context.Background()

	sweep(ctx, cl, "dr1", newTestRuntime(t))

	for name, want := range map[string]bool{"rs-kept": true, "rs-gone": false, "rs-plain": true} {
		err := cl.Get(ctx, types.NamespacedName{Namespace: "app-ns", Name: name},
			&volsyncv1alpha1.ReplicationSource{})
		if got := err == nil; got != want {
			t.Errorf("rs %s exists=%v want %v", name, got, want)
		}
	}

	if err := cl.Get(ctx, types.NamespacedName{Namespace: "app-ns", Name: "rd-gone"},
		&volsyncv1alpha1.ReplicationDestination{}); err == nil {
		t.Error("rd owned by a deleted RGD must be swept")
	}
}
