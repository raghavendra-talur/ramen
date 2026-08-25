// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
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
