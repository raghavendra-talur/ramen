// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
	if err := volrep.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := volsyncv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := snapv1.AddToScheme(s); err != nil {
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

func TestWatchRecordsRawJSON(t *testing.T) {
	h := New()
	s := testScheme(t)
	wc := fake.NewClientBuilder().WithScheme(s).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchInto(ctx, h, wc, &corev1.PersistentVolumeClaimList{}, extractPVC("dr1"))

	time.Sleep(100 * time.Millisecond)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data-0",
		ManagedFields: []metav1.ManagedFieldsEntry{{Manager: "noise"}}}}
	if err := wc.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}

	waitObjects(t, h, 1)
	o, ok := h.Object("dr1", "PersistentVolumeClaim", "app", "data-0")
	if !ok || len(o.Raw) == 0 {
		t.Fatalf("raw JSON not recorded: %+v", o)
	}
	var m map[string]any
	if err := json.Unmarshal(o.Raw, &m); err != nil {
		t.Fatalf("raw is not JSON: %v", err)
	}
	meta, _ := m["metadata"].(map[string]any)
	if meta["name"] != "data-0" {
		t.Fatalf("raw metadata: %+v", meta)
	}
	if _, noisy := meta["managedFields"]; noisy {
		t.Fatal("managed fields not stripped from raw JSON")
	}

	// Raw must stay out of the wire snapshot.
	b, err := json.Marshal(h.Snapshot())
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "\"raw\"") {
		t.Fatal("raw leaked into the snapshot JSON")
	}
}

// extractOne runs a data-plane extract directly (the watch loop machinery
// is covered elsewhere) and returns the resulting state.
func extractOne(t *testing.T, fn func(client.Object) (ObjectState, bool),
	obj client.Object,
) ObjectState {
	t.Helper()
	o, ok := fn(obj)
	if !ok {
		t.Fatalf("extract rejected %T", obj)
	}
	return o
}

func TestExtractDataPlane(t *testing.T) {
	syncedAt := metav1.NewTime(time.Date(2026, 9, 4, 12, 0, 0, 0, time.UTC))
	ready := true

	vr := &volrep.VolumeReplication{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data-0"}}
	vr.Spec.ReplicationState = "primary"
	vr.Spec.DataSource.Name = "data-0"
	vr.Status.State = "Primary"
	vr.Status.LastSyncTime = &syncedAt
	o := extractOne(t, extractVR("dr1"), vr)
	if o.Kind != "VolumeReplication" || o.Fields["state"] != "primary" ||
		o.Fields["observed"] != "primary" || o.Fields["pvc"] != "data-0" ||
		o.Fields["lastSyncTime"] != "2026-09-04T12:00:00Z" {
		t.Fatalf("vr: %+v", o)
	}

	vgr := &volrep.VolumeGroupReplication{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "grp"}}
	vgr.Spec.ReplicationState = "secondary"
	vgr.Status.PersistentVolumeClaimsRefList = []corev1.LocalObjectReference{{Name: "a"}, {Name: "b"}}
	o = extractOne(t, extractVGR("dr1"), vgr)
	if o.Kind != "VolumeGroupReplication" || o.Fields["state"] != "secondary" || o.Fields["pvcs"] != "2" {
		t.Fatalf("vgr: %+v", o)
	}

	rs := &volsyncv1alpha1.ReplicationSource{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data-0"}}
	rs.Spec.SourcePVC = "data-0"
	rs.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: "final-sync"}
	rs.Status = &volsyncv1alpha1.ReplicationSourceStatus{
		LastSyncTime: &syncedAt, LastManualSync: "final-sync"}
	o = extractOne(t, extractRS("dr1"), rs)
	if o.Fields["pvc"] != "data-0" || o.Fields["manual"] != "final-sync" ||
		o.Fields["lastManualSync"] != "final-sync" ||
		o.Fields["lastSyncTime"] != "2026-09-04T12:00:00Z" {
		t.Fatalf("rs: %+v", o)
	}

	rd := &volsyncv1alpha1.ReplicationDestination{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "data-0"}}
	rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LastSyncTime: &syncedAt,
		LatestImage:  &corev1.TypedLocalObjectReference{Name: "snap-42"}}
	o = extractOne(t, extractRD("dr2"), rd)
	if o.Fields["latestImage"] != "snap-42" || o.Fields["lastSyncTime"] != "2026-09-04T12:00:00Z" {
		t.Fatalf("rd: %+v", o)
	}

	pvcName := "data-0"
	snap := &snapv1.VolumeSnapshot{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "snap-42"}}
	snap.Spec.Source.PersistentVolumeClaimName = &pvcName
	snap.Status = &snapv1.VolumeSnapshotStatus{ReadyToUse: &ready}
	o = extractOne(t, extractSnap("dr1"), snap)
	if o.Fields["pvc"] != "data-0" || o.Fields["ready"] != "true" {
		t.Fatalf("snap: %+v", o)
	}

	// Statusless objects (just created) must extract without panicking.
	o = extractOne(t, extractRS("dr1"), &volsyncv1alpha1.ReplicationSource{})
	if o.Fields["lastSyncTime"] != "" {
		t.Fatalf("statusless rs: %+v", o)
	}
	o = extractOne(t, extractSnap("dr1"), &snapv1.VolumeSnapshot{})
	if o.Fields["ready"] != "false" {
		t.Fatalf("statusless snap: %+v", o)
	}
}

func TestWatchVolumeReplicationFeedsHub(t *testing.T) {
	h := New()
	s := testScheme(t)
	wc := fake.NewClientBuilder().WithScheme(s).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchInto(ctx, h, wc, &volrep.VolumeReplicationList{}, extractVR("dr1"))

	time.Sleep(100 * time.Millisecond)
	vr := &volrep.VolumeReplication{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data-0"}}
	vr.Spec.ReplicationState = "primary"
	vr.Spec.DataSource.Name = "data-0"
	if err := wc.Create(ctx, vr); err != nil {
		t.Fatal(err)
	}

	objs := waitObjects(t, h, 1)
	if objs[0].Kind != "VolumeReplication" || objs[0].Fields["pvc"] != "data-0" {
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
