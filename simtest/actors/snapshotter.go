// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"

	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// snapActor stands in for external-snapshotter: it marks every
// VolumeSnapshot ready with a bound content name and a restore size taken
// from the source PVC's request, so VolSync restore flows complete with no
// CSI snapshotter installed. Gated by the policy store under the Snap key.
type snapActor struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupSnapshotter(mgr manager.Manager, cluster string, rt *Runtime) error {
	a := &snapActor{client: mgr.GetClient(), cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&snapv1.VolumeSnapshot{}).
		Named("snap-" + cluster).
		Complete(a)
}

func (a *snapActor) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vs := &snapv1.VolumeSnapshot{}
	if err := a.client.Get(ctx, req.NamespacedName, vs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !vs.GetDeletionTimestamp().IsZero() || volumeSnapshotReady(vs.Status) {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(Snap(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	now := metav1.Now()
	vs.Status = &snapv1.VolumeSnapshotStatus{
		BoundVolumeSnapshotContentName: ptr.To(fmt.Sprintf("mock-snapcontent-%s-%s", vs.GetNamespace(), vs.GetName())),
		CreationTime:                   &now,
		ReadyToUse:                     ptr.To(true),
		RestoreSize:                    a.sourceRestoreSize(ctx, vs),
	}

	if err := a.client.Status().Update(ctx, vs); err != nil {
		return ctrl.Result{}, fmt.Errorf("update snapshot status: %w", err)
	}

	a.rt.Log.Logf("snap@%s fulfilled %s policy=%T", a.cluster, req.NamespacedName, d.Policy)

	return ctrl.Result{}, nil
}

// sourceRestoreSize resolves the snapshot's restore size from the source
// PVC's requested storage. Returns nil if there is no source PVC or it is
// not found (Ramen then falls back to the target PVC's requested capacity).
func (a *snapActor) sourceRestoreSize(ctx context.Context, vs *snapv1.VolumeSnapshot) *resource.Quantity {
	pvcName := vs.Spec.Source.PersistentVolumeClaimName
	if pvcName == nil || *pvcName == "" {
		return nil
	}

	pvc := &corev1.PersistentVolumeClaim{}
	if err := a.client.Get(ctx, types.NamespacedName{Namespace: vs.GetNamespace(), Name: *pvcName}, pvc); err != nil {
		return nil
	}

	if storage, ok := pvc.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		return &storage
	}

	return nil
}

// volumeSnapshotReady reports whether a VolumeSnapshot is already fulfilled,
// keeping reconcile idempotent.
func volumeSnapshotReady(status *snapv1.VolumeSnapshotStatus) bool {
	return status != nil && status.ReadyToUse != nil && *status.ReadyToUse &&
		status.BoundVolumeSnapshotContentName != nil
}
