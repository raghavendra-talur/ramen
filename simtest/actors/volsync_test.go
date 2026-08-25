// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"path/filepath"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	groupsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func volsyncScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := volsyncv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := snapv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := volrep.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := groupsnapv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}

	return s
}

func newTestRuntime(t *testing.T) *Runtime {
	t.Helper()

	log, err := NewEvLog(filepath.Join(t.TempDir(), "events.log"))
	if err != nil {
		t.Fatal(err)
	}

	return &Runtime{Store: NewStore(), Log: log}
}

func newRD() *volsyncv1alpha1.ReplicationDestination {
	capacity := resource.MustParse("1Gi")

	return &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "app-data", Namespace: "app-ns"},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			RsyncTLS: &volsyncv1alpha1.ReplicationDestinationRsyncTLSSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					AccessModes:             []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
					StorageClassName:        ptr.To("mock-cephfs"),
					VolumeSnapshotClassName: ptr.To("mock-cephfs-vsc"),
					Capacity:                &capacity,
				},
			},
		},
	}
}

// The RD fulfiller mirrors the real VolSync destination side in two steps:
// first materialize a destination PVC (distinct from the protected PVC name,
// which Ramen restores under) and requeue until the binder binds it, then
// snapshot it and publish the rsyncTLS address + latestImage status Ramen's
// failover restore path waits on.
func TestVolSyncRDFulfillsAfterPVCBinds(t *testing.T) {
	rd := newRD()
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&volsyncv1alpha1.ReplicationDestination{}).
		WithObjects(rd).Build()
	a := &volSyncActor{client: cl, cluster: "dr1", rt: newTestRuntime(t)}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "app-data"}}

	res, err := a.reconcileRD(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("first pass must requeue waiting for the destination PVC to bind")
	}

	pvc := &corev1.PersistentVolumeClaim{}
	pvcKey := types.NamespacedName{Namespace: "app-ns", Name: "mock-volsync-dst-app-data"}
	if err := cl.Get(ctx, pvcKey, pvc); err != nil {
		t.Fatalf("destination PVC: %v", err)
	}
	if got := *pvc.Spec.StorageClassName; got != "mock-cephfs" {
		t.Fatalf("dest PVC storage class = %q", got)
	}

	pvc.Status.Phase = corev1.ClaimBound
	if err := cl.Status().Update(ctx, pvc); err != nil {
		t.Fatal(err)
	}

	if _, err := a.reconcileRD(ctx, req); err != nil {
		t.Fatal(err)
	}

	snap := &snapv1.VolumeSnapshot{}
	snapKey := types.NamespacedName{Namespace: "app-ns", Name: "mock-latestimage-app-data"}
	if err := cl.Get(ctx, snapKey, snap); err != nil {
		t.Fatalf("latestImage snapshot: %v", err)
	}
	if got := *snap.Spec.Source.PersistentVolumeClaimName; got != "mock-volsync-dst-app-data" {
		t.Fatalf("snapshot source = %q", got)
	}
	if got := *snap.Spec.VolumeSnapshotClassName; got != "mock-cephfs-vsc" {
		t.Fatalf("snapshot class = %q", got)
	}

	got := &volsyncv1alpha1.ReplicationDestination{}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || got.Status.RsyncTLS == nil || got.Status.RsyncTLS.Address == nil {
		t.Fatalf("rsyncTLS address not published: %+v", got.Status)
	}
	if got.Status.LatestImage == nil || got.Status.LatestImage.Name != "mock-latestimage-app-data" {
		t.Fatalf("latestImage not published: %+v", got.Status)
	}
	if got.Status.LastSyncTime == nil {
		t.Fatal("LastSyncTime not published")
	}
}

// Ramen's relocate path waits for spec.trigger.manual to be echoed into
// status.lastManualSync (the final sync); the RS fulfiller must echo it and
// stay idempotent until the trigger changes.
func TestVolSyncRSEchoesManualTrigger(t *testing.T) {
	rs := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: "app-data", Namespace: "app-ns"},
		Spec: volsyncv1alpha1.ReplicationSourceSpec{
			Trigger: &volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: "final-sync-1"},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&volsyncv1alpha1.ReplicationSource{}).
		WithObjects(rs).Build()
	a := &volSyncActor{client: cl, cluster: "dr1", rt: newTestRuntime(t)}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "app-data"}}

	if _, err := a.reconcileRS(ctx, req); err != nil {
		t.Fatal(err)
	}

	got := &volsyncv1alpha1.ReplicationSource{}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || got.Status.LastSyncTime == nil {
		t.Fatal("LastSyncTime not published")
	}
	if got.Status.LastManualSync != "final-sync-1" {
		t.Fatalf("LastManualSync = %q", got.Status.LastManualSync)
	}
	// Real VolSync records the mover run; ramen dereferences it without a
	// nil check (vshandler rollbackToLastSnapshot), so a faithful fulfiller
	// must set it.
	if got.Status.LatestMoverStatus == nil {
		t.Fatal("LatestMoverStatus not published")
	}
}

// RGD-owned destinations use manual-trigger semantics: the RGD state
// machine sets spec.trigger.manual each sync round and treats the RD as
// completed only once status.lastManualSync echoes it, so the fulfiller
// must echo the trigger and re-fulfill when it changes.
func TestVolSyncRDEchoesManualTrigger(t *testing.T) {
	rd := newRD()
	rd.Spec.Trigger = &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: "sync-1"}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&volsyncv1alpha1.ReplicationDestination{}).
		WithObjects(rd).Build()
	a := &volSyncActor{client: cl, cluster: "dr1", rt: newTestRuntime(t)}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "app-data"}}

	if _, err := a.reconcileRD(ctx, req); err != nil {
		t.Fatal(err)
	}

	pvc := &corev1.PersistentVolumeClaim{}
	pvcKey := types.NamespacedName{Namespace: "app-ns", Name: "mock-volsync-dst-app-data"}
	if err := cl.Get(ctx, pvcKey, pvc); err != nil {
		t.Fatal(err)
	}

	pvc.Status.Phase = corev1.ClaimBound
	if err := cl.Status().Update(ctx, pvc); err != nil {
		t.Fatal(err)
	}

	if _, err := a.reconcileRD(ctx, req); err != nil {
		t.Fatal(err)
	}

	got := &volsyncv1alpha1.ReplicationDestination{}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || got.Status.LastManualSync != "sync-1" {
		t.Fatalf("LastManualSync not echoed: %+v", got.Status)
	}

	// A new trigger round must be re-fulfilled even though the RD was
	// already ready.
	got.Spec.Trigger.Manual = "sync-2"
	if err := cl.Update(ctx, got); err != nil {
		t.Fatal(err)
	}
	if _, err := a.reconcileRD(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.LastManualSync != "sync-2" {
		t.Fatalf("new trigger not echoed: %q", got.Status.LastManualSync)
	}
}

// A Silent fault against the volsync actor must stall fulfillment (requeue,
// not drop) so the fault window can starve Ramen of sync progress.
func TestVolSyncSilentPolicyStalls(t *testing.T) {
	rd := newRD()
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&volsyncv1alpha1.ReplicationDestination{}).
		WithObjects(rd).Build()
	rt := newTestRuntime(t)
	rt.Store.Set(VolSync("dr1"), Silent{})
	a := &volSyncActor{client: cl, cluster: "dr1", rt: rt}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "app-data"}}

	res, err := a.reconcileRD(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("silent policy must requeue")
	}

	pvc := &corev1.PersistentVolumeClaim{}
	pvcKey := types.NamespacedName{Namespace: "app-ns", Name: "mock-volsync-dst-app-data"}
	if err := cl.Get(context.Background(), pvcKey, pvc); err == nil {
		t.Fatal("silent policy must not create the destination PVC")
	}
}
