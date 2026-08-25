// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := rmn.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func waitObjects(t *testing.T, h *Hub, want int) []ObjectState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if objs := h.Snapshot().Objects; len(objs) >= want {
			return objs
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("hub never saw %d objects: %+v", want, h.Snapshot().Objects)
	return nil
}

func TestWatchDRPCFeedsHub(t *testing.T) {
	h := New()
	s := testScheme(t)
	wc := fake.NewClientBuilder().WithScheme(s).WithStatusSubresource(&rmn.DRPlacementControl{}).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchInto(ctx, h, wc, &rmn.DRPlacementControlList{}, extractDRPC("hub"))

	// Give the watch a moment to establish, then create.
	time.Sleep(100 * time.Millisecond)
	drpc := &rmn.DRPlacementControl{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ramen-ops", Name: "app-drpc"}}
	drpc.Spec.Action = rmn.ActionFailover
	drpc.Spec.FailoverCluster = "dr2"
	drpc.Spec.PreferredCluster = "dr1"
	if err := wc.Create(ctx, drpc); err != nil {
		t.Fatal(err)
	}
	drpc.Status.Phase = rmn.FailingOver
	drpc.Status.Progression = rmn.ProgressionWaitForReadiness
	if err := wc.Status().Update(ctx, drpc); err != nil {
		t.Fatal(err)
	}

	objs := waitObjects(t, h, 1)
	o := objs[0]
	if o.Kind != "DRPlacementControl" || o.Cluster != "hub" || o.Name != "app-drpc" {
		t.Fatalf("object: %+v", o)
	}
	// The status update must eventually be reflected.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		o = h.Snapshot().Objects[0]
		if o.Fields["phase"] == string(rmn.FailingOver) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if o.Fields["phase"] != string(rmn.FailingOver) ||
		o.Fields["progression"] != string(rmn.ProgressionWaitForReadiness) {
		t.Fatalf("fields: %+v", o.Fields)
	}

	// The stage's waiting-on spotlight needs the action's target clusters.
	if o.Fields["action"] != string(rmn.ActionFailover) ||
		o.Fields["failoverCluster"] != "dr2" || o.Fields["preferredCluster"] != "dr1" {
		t.Fatalf("spec fields missing for spotlight: %+v", o.Fields)
	}
}

func TestWatchPVCFeedsHub(t *testing.T) {
	h := New()
	s := testScheme(t)
	wc := fake.NewClientBuilder().WithScheme(s).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchInto(ctx, h, wc, &corev1.PersistentVolumeClaimList{}, extractPVC("dr1"))

	time.Sleep(100 * time.Millisecond)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data-0"}}
	if err := wc.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}

	objs := waitObjects(t, h, 1)
	if objs[0].Kind != "PersistentVolumeClaim" || objs[0].Cluster != "dr1" {
		t.Fatalf("object: %+v", objs[0])
	}
}

func TestWatchExitsOnContextDone(t *testing.T) {
	h := New()
	s := testScheme(t)
	wc := fake.NewClientBuilder().WithScheme(s).Build()
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		watchInto(ctx, h, wc, &corev1.PersistentVolumeClaimList{}, extractPVC("dr1"))
		close(done)
	}()

	// Give the watch a moment to establish.
	time.Sleep(100 * time.Millisecond)

	// Cancel context and wait for goroutine to exit.
	cancel()
	select {
	case <-done:
		// Goroutine exited as expected.
	case <-time.After(2 * time.Second):
		t.Fatal("watchInto did not exit when context was canceled")
	}
}

func TestWatchRemovesDeletedObject(t *testing.T) {
	h := New()
	s := testScheme(t)
	wc := fake.NewClientBuilder().WithScheme(s).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchInto(ctx, h, wc, &corev1.PersistentVolumeClaimList{}, extractPVC("dr1"))

	time.Sleep(100 * time.Millisecond)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data-0"}}
	if err := wc.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}
	waitObjects(t, h, 1)

	if err := wc.Delete(ctx, pvc); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(h.Snapshot().Objects) == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("deleted object still in snapshot: %+v", h.Snapshot().Objects)
}
