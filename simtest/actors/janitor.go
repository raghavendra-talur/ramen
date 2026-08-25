// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"slices"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	groupsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	rmn "github.com/ramendr/ramen/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	mockDestPVCPrefix     = "mock-volsync-dst-"
	mockLatestImagePrefix = "mock-latestimage-"
	mockVGRCPrefix        = "mock-vgrc-"
	mockVGSMemberPrefix   = "mock-vgs-"
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
			sweepOrphanVGSMember(ctx, c, &snaps.Items[i], cluster, rt)
		}
	}

	sweepOrphanVGRCs(ctx, c, cluster, rt)
	sweepOrphanRGChildren(ctx, c, cluster, rt)

	// Ramen deletes its final-sync mount jobs with Foreground propagation;
	// the resulting foregroundDeletion finalizer is the garbage collector's
	// to clear, and envtest runs none.
	jobs := &batchv1.JobList{}
	if err := c.List(ctx, jobs); err == nil {
		for i := range jobs.Items {
			stripFinalizer(ctx, c, &jobs.Items[i], metav1.FinalizerDeleteDependents, cluster, rt)
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

// sweepOrphanVGRCs deletes vgr-actor-created VolumeGroupReplicationContents
// whose referenced VGR no longer exists. VGRCs are cluster-scoped, so they
// cannot carry an ownerRef to their namespaced VGR even where a garbage
// collector runs.
func sweepOrphanVGRCs(ctx context.Context, c client.Client, cluster string, rt *Runtime) {
	vgrcs := &volrep.VolumeGroupReplicationContentList{}
	if err := c.List(ctx, vgrcs); err != nil {
		return
	}

	for i := range vgrcs.Items {
		vgrc := &vgrcs.Items[i]

		ref := vgrc.Spec.VolumeGroupReplicationRef
		if !strings.HasPrefix(vgrc.GetName(), mockVGRCPrefix) || ref == nil {
			continue
		}

		vgr := &volrep.VolumeGroupReplication{}

		err := c.Get(ctx, client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}, vgr)
		if !apierrors.IsNotFound(err) {
			continue
		}

		if err := c.Delete(ctx, vgrc); err == nil {
			rt.Log.Logf("janitor@%s swept orphaned VGRC %s", cluster, vgrc.GetName())
		}
	}
}

// sweepOrphanRGChildren deletes ReplicationSources/Destinations whose
// controlling ReplicationGroupSource/Destination no longer exists: ramen
// deletes the group parents and relies on the garbage collector to cascade
// to the per-PVC children it created — envtest runs none.
func sweepOrphanRGChildren(ctx context.Context, c client.Client, cluster string, rt *Runtime) {
	rss := &volsyncv1alpha1.ReplicationSourceList{}
	if err := c.List(ctx, rss); err == nil {
		for i := range rss.Items {
			sweepIfRGParentGone(ctx, c, &rss.Items[i], cluster, rt)
		}
	}

	rds := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(ctx, rds); err == nil {
		for i := range rds.Items {
			sweepIfRGParentGone(ctx, c, &rds.Items[i], cluster, rt)
		}
	}

	vgss := &groupsnapv1.VolumeGroupSnapshotList{}
	if err := c.List(ctx, vgss); err == nil {
		for i := range vgss.Items {
			sweepIfRGParentGone(ctx, c, &vgss.Items[i], cluster, rt)
		}
	}
}

func sweepIfRGParentGone(ctx context.Context, c client.Client, obj client.Object, cluster string, rt *Runtime) {
	ref := metav1.GetControllerOfNoCopy(obj)
	if ref == nil {
		return
	}

	var parent client.Object

	switch ref.Kind {
	case "ReplicationGroupSource":
		parent = &rmn.ReplicationGroupSource{}
	case "ReplicationGroupDestination":
		parent = &rmn.ReplicationGroupDestination{}
	default:
		return
	}

	err := c.Get(ctx, client.ObjectKey{Namespace: obj.GetNamespace(), Name: ref.Name}, parent)
	if !apierrors.IsNotFound(err) {
		return
	}

	if err := c.Delete(ctx, obj); err == nil {
		rt.Log.Logf("janitor@%s swept %s/%s (its %s is gone)", cluster, obj.GetNamespace(), obj.GetName(), ref.Kind)
	}
}

// sweepOrphanVGSMember deletes a vgs-actor member snapshot whose owning
// VolumeGroupSnapshot no longer exists (envtest runs no garbage collector,
// so the ownerRef alone cleans up nothing).
func sweepOrphanVGSMember(ctx context.Context, c client.Client, snap *snapv1.VolumeSnapshot,
	cluster string, rt *Runtime,
) {
	if !strings.HasPrefix(snap.GetName(), mockVGSMemberPrefix) {
		return
	}

	for _, ref := range snap.GetOwnerReferences() {
		if ref.Kind != "VolumeGroupSnapshot" {
			continue
		}

		vgs := &groupsnapv1.VolumeGroupSnapshot{}

		err := c.Get(ctx, client.ObjectKey{Namespace: snap.GetNamespace(), Name: ref.Name}, vgs)
		if !apierrors.IsNotFound(err) {
			return
		}

		if err := c.Delete(ctx, snap); err == nil {
			rt.Log.Logf("janitor@%s swept orphaned VGS member %s/%s", cluster, snap.GetNamespace(), snap.GetName())
		}

		return
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
