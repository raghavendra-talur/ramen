// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"

	"github.com/ramendr/ramen/simtest/actors"
	"github.com/ramendr/ramen/simtest/world"
)

func TestPVBinder(t *testing.T) {
	world.EnsureAssets(t)

	dir := t.TempDir()
	c, err := world.StartCluster("dr1", dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	log, err := actors.NewEvLog(filepath.Join(dir, "events.log"))
	if err != nil {
		t.Fatal(err)
	}
	ref := actors.ClusterRef{Name: c.Name, Cfg: c.Cfg}
	if _, err := actors.Start(ctx, world.NewScheme(), ref, []actors.ClusterRef{ref}, log); err != nil {
		t.Fatal(err)
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "default"},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: ptr.To(world.StorageClassName),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}
	if err := c.Client.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		_ = c.Client.Get(ctx, types.NamespacedName{Name: "data", Namespace: "default"}, pvc)
		if pvc.Status.Phase == corev1.ClaimBound && pvc.Spec.VolumeName != "" {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("PVC never bound: phase=%s volumeName=%q", pvc.Status.Phase, pvc.Spec.VolumeName)
}
