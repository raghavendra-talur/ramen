// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"slices"
	"time"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// runJanitor emulates the kube-controller-manager protection controllers:
// envtest's apiserver adds kubernetes.io/pvc-protection and pv-protection
// finalizers, but nothing removes them, so deletes would hang forever.
func runJanitor(ctx context.Context, c client.Client, cluster string, rt *Runtime) {
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			sweep(ctx, c, cluster, rt)
		}
	}
}

func sweep(ctx context.Context, c client.Client, cluster string, rt *Runtime) {
	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcs); err == nil {
		for i := range pvcs.Items {
			stripFinalizer(ctx, c, &pvcs.Items[i], "kubernetes.io/pvc-protection", cluster, rt)
		}
	}

	pvs := &corev1.PersistentVolumeList{}
	if err := c.List(ctx, pvs); err == nil {
		for i := range pvs.Items {
			stripFinalizer(ctx, c, &pvs.Items[i], "kubernetes.io/pv-protection", cluster, rt)
		}
	}
}

func stripFinalizer(ctx context.Context, c client.Client, obj client.Object, fin, cluster string, rt *Runtime) {
	if obj.GetDeletionTimestamp() == nil || !slices.Contains(obj.GetFinalizers(), fin) {
		return
	}

	obj.SetFinalizers(slices.DeleteFunc(obj.GetFinalizers(), func(s string) bool { return s == fin }))

	if err := c.Update(ctx, obj); err == nil {
		rt.Log.Logf("janitor@%s stripped %s from %s/%s", cluster, fin, obj.GetNamespace(), obj.GetName())
	}
}
