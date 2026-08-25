// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"testing"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// The snapshotter stands in for external-snapshotter: it marks snapshots
// ready with a bound content name and a restore size taken from the source
// PVC's request, which Ramen's VolSync restore path reads.
func TestSnapshotterFulfills(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "src-pvc", Namespace: "app-ns"},
		Spec: corev1.PersistentVolumeClaimSpec{
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("2Gi")},
			},
		},
	}
	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-a", Namespace: "app-ns"},
		Spec: snapv1.VolumeSnapshotSpec{
			Source: snapv1.VolumeSnapshotSource{PersistentVolumeClaimName: ptr.To("src-pvc")},
		},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&snapv1.VolumeSnapshot{}).
		WithObjects(pvc, vs).Build()
	a := &snapActor{client: cl, cluster: "dr1", rt: newTestRuntime(t)}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "snap-a"}}

	if _, err := a.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}

	got := &snapv1.VolumeSnapshot{}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || got.Status.ReadyToUse == nil || !*got.Status.ReadyToUse {
		t.Fatalf("not ready: %+v", got.Status)
	}
	if got.Status.BoundVolumeSnapshotContentName == nil || *got.Status.BoundVolumeSnapshotContentName == "" {
		t.Fatal("bound content name not set")
	}
	if got.Status.RestoreSize == nil || got.Status.RestoreSize.String() != "2Gi" {
		t.Fatalf("restore size = %v, want 2Gi", got.Status.RestoreSize)
	}
}

// The snapshotter is policy-gated under the same key as the volsync actor's
// Snap key so faults can hold a snapshot un-ready.
func TestSnapshotterSilentPolicyStalls(t *testing.T) {
	vs := &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "snap-a", Namespace: "app-ns"},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&snapv1.VolumeSnapshot{}).
		WithObjects(vs).Build()
	rt := newTestRuntime(t)
	rt.Store.Set(Snap("dr1"), Silent{})
	a := &snapActor{client: cl, cluster: "dr1", rt: rt}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "snap-a"}}

	res, err := a.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("silent policy must requeue")
	}

	got := &snapv1.VolumeSnapshot{}
	if err := cl.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status != nil && got.Status.ReadyToUse != nil {
		t.Fatal("silent policy must not fulfill the snapshot")
	}
}
