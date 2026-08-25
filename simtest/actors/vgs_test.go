// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"testing"

	groupsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func newVGS() *groupsnapv1.VolumeGroupSnapshot {
	return &groupsnapv1.VolumeGroupSnapshot{
		ObjectMeta: metav1.ObjectMeta{Name: "cg-snap", Namespace: "app-ns", UID: "vgs-uid-1"},
		Spec: groupsnapv1.VolumeGroupSnapshotSpec{
			Source: groupsnapv1.VolumeGroupSnapshotSource{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"grp": "cg1"}},
			},
			VolumeGroupSnapshotClassName: ptr.To("mock-cephfs-vgsc"),
		},
	}
}

// The VGS fulfiller stands in for the group-snapshot side of
// external-snapshotter: it creates one member VolumeSnapshot per selected
// PVC, owner-referenced to the group (ramen finds members by ownerRef, not
// label), and marks the group ready once every member is ready — member
// readiness itself comes from the snapshotter actor.
func TestVGSFulfillsAfterMembersReady(t *testing.T) {
	vgs := newVGS()
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&groupsnapv1.VolumeGroupSnapshot{}, &snapv1.VolumeSnapshot{}).
		WithObjects(vgs,
			labeledPVC("data-a", map[string]string{"grp": "cg1"}),
			labeledPVC("data-b", map[string]string{"grp": "cg1"}),
			labeledPVC("other", map[string]string{"grp": "cg2"}),
		).Build()
	a := &vgsActor{client: cl, cluster: "dr1", rt: newTestRuntime(t)}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "cg-snap"}}

	res, err := a.Reconcile(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("must requeue until member snapshots are ready")
	}

	members := &snapv1.VolumeSnapshotList{}
	if err := cl.List(ctx, members, client.InNamespace("app-ns")); err != nil {
		t.Fatal(err)
	}
	if len(members.Items) != 2 {
		t.Fatalf("got %d member snapshots, want 2", len(members.Items))
	}

	sources := map[string]bool{}

	for i := range members.Items {
		m := &members.Items[i]

		owner := metav1.GetControllerOfNoCopy(m)
		if owner == nil || owner.Kind != "VolumeGroupSnapshot" || owner.Name != "cg-snap" || owner.UID != "vgs-uid-1" {
			t.Fatalf("member %s owner = %+v, want the VGS", m.Name, owner)
		}
		if m.Spec.Source.PersistentVolumeClaimName == nil {
			t.Fatalf("member %s has no source PVC", m.Name)
		}

		sources[*m.Spec.Source.PersistentVolumeClaimName] = true

		// Snapshotter actor's job in the world; done by hand here.
		m.Status = &snapv1.VolumeSnapshotStatus{
			ReadyToUse:                     ptr.To(true),
			BoundVolumeSnapshotContentName: ptr.To("content-" + m.Name),
		}
		if err := cl.Status().Update(ctx, m); err != nil {
			t.Fatal(err)
		}
	}

	if !sources["data-a"] || !sources["data-b"] {
		t.Fatalf("member sources = %v, want the selected PVCs", sources)
	}

	if _, err := a.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}

	got := &groupsnapv1.VolumeGroupSnapshot{}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status == nil || got.Status.ReadyToUse == nil || !*got.Status.ReadyToUse {
		t.Fatalf("group not ready: %+v", got.Status)
	}
}

func TestVGSSilentPolicyStalls(t *testing.T) {
	vgs := newVGS()
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&groupsnapv1.VolumeGroupSnapshot{}).
		WithObjects(vgs, labeledPVC("data-a", map[string]string{"grp": "cg1"})).Build()
	rt := newTestRuntime(t)
	rt.Store.Set(Snap("dr1"), Silent{})
	a := &vgsActor{client: cl, cluster: "dr1", rt: rt}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "cg-snap"}}

	res, err := a.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("silent policy must requeue")
	}

	members := &snapv1.VolumeSnapshotList{}
	if err := cl.List(context.Background(), members, client.InNamespace("app-ns")); err != nil {
		t.Fatal(err)
	}
	if len(members.Items) != 0 {
		t.Fatal("silent policy must not create member snapshots")
	}
}
