// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"testing"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func labeledPVC(name string, labels map[string]string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app-ns", Labels: labels},
	}
}

func newVGR() *volrep.VolumeGroupReplication {
	return &volrep.VolumeGroupReplication{
		ObjectMeta: metav1.ObjectMeta{Name: "vgr-cg1-app1", Namespace: "app-ns", Generation: 2},
		Spec: volrep.VolumeGroupReplicationSpec{
			VolumeGroupReplicationClassName: "mock-rbd-vgrc",
			VolumeReplicationClassName:      "mock-rbd-vrc",
			ReplicationState:                volrep.Primary,
			Source: volrep.VolumeGroupReplicationSource{
				Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"grp": "cg1"}},
			},
		},
	}
}

// The VGR fulfiller stands in for the csi-addons VolumeGroupReplication
// controller: it creates the backing VolumeGroupReplicationContent and links
// it into the VGR spec, fulfills the shared VolumeReplication status
// contract, and maintains persistentVolumeClaimsRefList from the group
// selector — including removals, which ramen's unprotect flow blocks on.
func TestVGRFulfillsGroup(t *testing.T) {
	vgr := newVGR()
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&volrep.VolumeGroupReplication{}).
		WithObjects(vgr,
			labeledPVC("data-a", map[string]string{"grp": "cg1"}),
			labeledPVC("data-b", map[string]string{"grp": "cg1"}),
			labeledPVC("other", map[string]string{"grp": "cg2"}),
		).Build()
	a := &vgrActor{client: cl, cluster: "dr1", rt: newTestRuntime(t)}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "vgr-cg1-app1"}}

	if _, err := a.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}

	got := &volrep.VolumeGroupReplication{}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}

	if got.Spec.VolumeGroupReplicationContentName == "" {
		t.Fatal("VGRC not linked into the VGR spec")
	}

	vgrc := &volrep.VolumeGroupReplicationContent{}
	if err := cl.Get(ctx, types.NamespacedName{Name: got.Spec.VolumeGroupReplicationContentName}, vgrc); err != nil {
		t.Fatalf("VGRC not created: %v", err)
	}
	if vgrc.Spec.VolumeGroupReplicationHandle == "" {
		t.Fatal("VGRC needs a group handle")
	}

	if got.Status.State != volrep.PrimaryState || got.Status.ObservedGeneration != 2 {
		t.Fatalf("bad status: state=%s gen=%d", got.Status.State, got.Status.ObservedGeneration)
	}
	for typ, want := range map[string]metav1.ConditionStatus{
		volrep.ConditionValidated: metav1.ConditionTrue,
		volrep.ConditionCompleted: metav1.ConditionTrue,
		volrep.ConditionDegraded:  metav1.ConditionFalse,
		volrep.ConditionResyncing: metav1.ConditionFalse,
	} {
		c := findCond(got.Status.Conditions, typ)
		if c == nil || c.Status != want || c.ObservedGeneration != 2 {
			t.Fatalf("condition %s: got %+v want %s", typ, c, want)
		}
	}

	refs := map[string]bool{}
	for _, r := range got.Status.PersistentVolumeClaimsRefList {
		refs[r.Name] = true
	}

	if len(refs) != 2 || !refs["data-a"] || !refs["data-b"] {
		t.Fatalf("refList = %v, want exactly the selected PVCs", refs)
	}

	// A deselected/deleted PVC must leave the refList, or ramen's
	// unprotect flow waits forever.
	if err := cl.Delete(ctx, labeledPVC("data-b", nil)); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.PersistentVolumeClaimsRefList) != 1 ||
		got.Status.PersistentVolumeClaimsRefList[0].Name != "data-a" {
		t.Fatalf("refList after PVC delete = %v, want [data-a]", got.Status.PersistentVolumeClaimsRefList)
	}
}

// The VGR fulfiller shares the VolRep fault key: a volrep fault starves the
// grouped path exactly like the per-PVC path.
func TestVGRSilentPolicyStalls(t *testing.T) {
	vgr := newVGR()
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&volrep.VolumeGroupReplication{}).
		WithObjects(vgr).Build()
	rt := newTestRuntime(t)
	rt.Store.Set(VolRep("dr1"), Silent{})
	a := &vgrActor{client: cl, cluster: "dr1", rt: rt}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "vgr-cg1-app1"}}

	res, err := a.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("silent policy must requeue")
	}

	got := &volrep.VolumeGroupReplication{}
	if err := cl.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Spec.VolumeGroupReplicationContentName != "" || len(got.Status.Conditions) != 0 {
		t.Fatal("silent policy must not fulfill the VGR")
	}
}
