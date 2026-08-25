// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"slices"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	mockDestPVCPrefix     = "mock-volsync-dst-"
	mockLatestImagePrefix = "mock-latestimage-"
)

// runJanitor emulates the kube-controller-manager protection controllers:
// envtest's apiserver adds kubernetes.io/pvc-protection and pv-protection
// finalizers, but nothing removes them, so deletes would hang forever. It
// also stands in for the garbage collector (absent in envtest) for the
// volsync actor's artifacts, sweeping destination PVCs and latestImage
// snapshots whose ReplicationDestination no longer exists.
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
	liveRDs := liveRDNames(ctx, c)

	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, pvcs); err == nil {
		for i := range pvcs.Items {
			stripFinalizer(ctx, c, &pvcs.Items[i], "kubernetes.io/pvc-protection", cluster, rt)
			sweepOrphan(ctx, c, &pvcs.Items[i], mockDestPVCPrefix, liveRDs, cluster, rt)
		}
	}

	snaps := &snapv1.VolumeSnapshotList{}
	if err := c.List(ctx, snaps); err == nil {
		for i := range snaps.Items {
			sweepOrphan(ctx, c, &snaps.Items[i], mockLatestImagePrefix, liveRDs, cluster, rt)
		}
	}

	pvs := &corev1.PersistentVolumeList{}
	if err := c.List(ctx, pvs); err == nil {
		for i := range pvs.Items {
			stripFinalizer(ctx, c, &pvs.Items[i], "kubernetes.io/pv-protection", cluster, rt)
		}
	}
}

// liveRDNames returns the namespace/name keys of every existing
// ReplicationDestination; artifacts derived from an RD outlive it only
// until the next sweep.
func liveRDNames(ctx context.Context, c client.Client) map[string]bool {
	live := map[string]bool{}

	rds := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(ctx, rds); err == nil {
		for i := range rds.Items {
			live[rds.Items[i].GetNamespace()+"/"+rds.Items[i].GetName()] = true
		}
	}

	return live
}

// sweepOrphan deletes a volsync-actor artifact (identified by its name
// prefix) whose owning ReplicationDestination no longer exists.
func sweepOrphan(ctx context.Context, c client.Client, obj client.Object,
	prefix string, liveRDs map[string]bool, cluster string, rt *Runtime,
) {
	rdName, ok := strings.CutPrefix(obj.GetName(), prefix)
	if !ok || liveRDs[obj.GetNamespace()+"/"+rdName] {
		return
	}

	if err := c.Delete(ctx, obj); err == nil {
		rt.Log.Logf("janitor@%s swept orphaned %s/%s", cluster, obj.GetNamespace(), obj.GetName())
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
