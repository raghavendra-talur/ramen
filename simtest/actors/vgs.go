// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"time"

	groupsnapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumegroupsnapshot/v1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// memberReadyRequeue is how long to wait before re-checking whether all
// member snapshots have been marked ready (by the snapshotter actor).
const memberReadyRequeue = time.Second

// vgsActor stands in for the group-snapshot side of external-snapshotter
// (public groupsnapshot.storage.k8s.io/v1): for each VolumeGroupSnapshot it
// creates one member VolumeSnapshot per PVC the group selector matches,
// owner-referenced to the group — ramen's CephFS CG restore locates members
// by that ownerRef — and marks the group ready once every member is ready.
// Member readiness itself is the snapshotter actor's job, so this actor and
// the snapshotter compose the way the real group-snapshot and snapshot
// controllers do. Gated under the same Snap policy key as the snapshotter.
type vgsActor struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupVGS(mgr manager.Manager, cluster string, rt *Runtime) error {
	a := &vgsActor{client: mgr.GetClient(), cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&groupsnapv1.VolumeGroupSnapshot{}).
		Named("vgs-" + cluster).
		Complete(a)
}

func (a *vgsActor) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vgs := &groupsnapv1.VolumeGroupSnapshot{}
	if err := a.client.Get(ctx, req.NamespacedName, vgs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if vgs.GetDeletionTimestamp() != nil || volumeGroupSnapshotReady(vgs) {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(Snap(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	members, err := a.ensureMemberSnapshots(ctx, vgs)
	if err != nil {
		return ctrl.Result{}, err
	}

	for _, m := range members {
		if !volumeSnapshotReady(m.Status) {
			return ctrl.Result{RequeueAfter: memberReadyRequeue}, nil
		}
	}

	now := metav1.Now()
	vgs.Status = &groupsnapv1.VolumeGroupSnapshotStatus{
		BoundVolumeGroupSnapshotContentName: ptr.To("mock-vgscontent-" + vgs.GetName()),
		CreationTime:                        &now,
		ReadyToUse:                          ptr.To(true),
	}

	if err := a.client.Status().Update(ctx, vgs); err != nil {
		return ctrl.Result{}, err
	}

	a.rt.Log.Logf("vgs@%s fulfilled %s members=%d policy=%T",
		a.cluster, req.NamespacedName, len(members), d.Policy)

	return ctrl.Result{}, nil
}

// ensureMemberSnapshots creates one VolumeSnapshot per selected PVC, named
// after the group and PVC, controller-owned by the group. It returns the
// current member objects so readiness can be checked.
func (a *vgsActor) ensureMemberSnapshots(
	ctx context.Context, vgs *groupsnapv1.VolumeGroupSnapshot,
) ([]*snapv1.VolumeSnapshot, error) {
	selector := client.MatchingLabels{}
	if vgs.Spec.Source.Selector != nil {
		selector = vgs.Spec.Source.Selector.MatchLabels
	}

	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := a.client.List(ctx, pvcs, client.InNamespace(vgs.GetNamespace()), selector); err != nil {
		return nil, err
	}

	members := make([]*snapv1.VolumeSnapshot, 0, len(pvcs.Items))

	for i := range pvcs.Items {
		pvcName := pvcs.Items[i].GetName()
		name := fmt.Sprintf("mock-vgs-%s-%s", vgs.GetName(), pvcName)

		member := &snapv1.VolumeSnapshot{}

		err := a.client.Get(ctx, client.ObjectKey{Namespace: vgs.GetNamespace(), Name: name}, member)
		if err == nil {
			members = append(members, member)

			continue
		}

		if !apierrors.IsNotFound(err) {
			return nil, err
		}

		member = &snapv1.VolumeSnapshot{
			ObjectMeta: metav1.ObjectMeta{
				Name: name, Namespace: vgs.GetNamespace(),
				OwnerReferences: []metav1.OwnerReference{{
					APIVersion: groupsnapv1.SchemeGroupVersion.String(),
					Kind:       "VolumeGroupSnapshot",
					Name:       vgs.GetName(),
					UID:        vgs.GetUID(),
					Controller: ptr.To(true),
				}},
			},
			Spec: snapv1.VolumeSnapshotSpec{
				Source: snapv1.VolumeSnapshotSource{PersistentVolumeClaimName: ptr.To(pvcName)},
			},
		}
		if err := a.client.Create(ctx, member); err != nil && !apierrors.IsAlreadyExists(err) {
			return nil, fmt.Errorf("create member snapshot %s: %w", name, err)
		}

		members = append(members, member)
	}

	return members, nil
}

func volumeGroupSnapshotReady(vgs *groupsnapv1.VolumeGroupSnapshot) bool {
	return vgs.Status != nil && vgs.Status.ReadyToUse != nil && *vgs.Status.ReadyToUse
}
