// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"slices"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// vgrActor stands in for the csi-addons VolumeGroupReplication controller.
// Beyond the shared VolumeReplication status contract (state + the four
// conditions), the group flavor has two extra obligations ramen depends on:
// a backing VolumeGroupReplicationContent linked into the VGR spec (ramen
// uploads both to S3 for restore on the peer), and
// status.persistentVolumeClaimsRefList tracking the PVCs the group selector
// currently matches — including removals, which ramen's unprotect flow
// blocks on. Gated under the same policy key as the per-PVC volrep actor,
// so volrep faults starve both replication flavors.
type vgrActor struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupVGR(mgr manager.Manager, cluster string, rt *Runtime) error {
	a := &vgrActor{client: mgr.GetClient(), cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&volrep.VolumeGroupReplication{}).
		Named("vgr-" + cluster).
		Complete(a)
}

//nolint:cyclop
func (a *vgrActor) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vgr := &volrep.VolumeGroupReplication{}
	if err := a.client.Get(ctx, req.NamespacedName, vgr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if vgr.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(VolRep(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	if vgr.Spec.VolumeGroupReplicationContentName == "" {
		if err := a.ensureVGRC(ctx, vgr); err != nil {
			return ctrl.Result{}, err
		}
	}

	refs, err := a.selectedPVCRefs(ctx, vgr)
	if err != nil {
		return ctrl.Result{}, err
	}

	desired := fulfillVolumeGroupReplication(vgr, d.Policy, refs)
	if vgrStatusFulfilled(vgr, desired) {
		return ctrl.Result{}, nil
	}

	vgr.Status = desired
	if err := a.client.Status().Update(ctx, vgr); err != nil {
		return ctrl.Result{}, err
	}

	a.rt.Log.Logf("vgr@%s fulfilled %s state=%s pvcs=%d policy=%T",
		a.cluster, req.NamespacedName, desired.State, len(refs), d.Policy)

	return ctrl.Result{}, nil
}

// ensureVGRC creates the backing content object and links it into the VGR
// spec, the way the real csi-addons controller binds a group to its content.
func (a *vgrActor) ensureVGRC(ctx context.Context, vgr *volrep.VolumeGroupReplication) error {
	name := "mock-vgrc-" + vgr.GetName()

	vgrc := &volrep.VolumeGroupReplicationContent{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: volrep.VolumeGroupReplicationContentSpec{
			VolumeGroupReplicationRef: &corev1.ObjectReference{
				Kind: "VolumeGroupReplication", Name: vgr.GetName(), Namespace: vgr.GetNamespace(),
				UID: vgr.GetUID(), APIVersion: volrep.GroupVersion.String(),
			},
			VolumeGroupReplicationHandle:    fmt.Sprintf("mock-group-%s-%s", vgr.GetNamespace(), vgr.GetName()),
			Provisioner:                     "mock.csi.ramen.io",
			VolumeGroupReplicationClassName: vgr.Spec.VolumeGroupReplicationClassName,
			Source:                          volrep.VolumeGroupReplicationContentSource{VolumeHandles: []string{}},
		},
	}
	if err := a.client.Create(ctx, vgrc); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create VGRC: %w", err)
	}

	vgr.Spec.VolumeGroupReplicationContentName = name

	if err := a.client.Update(ctx, vgr); err != nil {
		return fmt.Errorf("link VGRC into VGR: %w", err)
	}

	return nil
}

// selectedPVCRefs lists the PVCs in the VGR's namespace matching its group
// selector, sorted for stable status comparisons.
func (a *vgrActor) selectedPVCRefs(
	ctx context.Context, vgr *volrep.VolumeGroupReplication,
) ([]corev1.LocalObjectReference, error) {
	selector := client.MatchingLabels{}
	if vgr.Spec.Source.Selector != nil {
		selector = vgr.Spec.Source.Selector.MatchLabels
	}

	pvcs := &corev1.PersistentVolumeClaimList{}
	if err := a.client.List(ctx, pvcs, client.InNamespace(vgr.GetNamespace()), selector); err != nil {
		return nil, err
	}

	refs := make([]corev1.LocalObjectReference, 0, len(pvcs.Items))
	for i := range pvcs.Items {
		refs = append(refs, corev1.LocalObjectReference{Name: pvcs.Items[i].GetName()})
	}

	slices.SortFunc(refs, func(x, y corev1.LocalObjectReference) int {
		return cmpStrings(x.Name, y.Name)
	})

	return refs, nil
}

func cmpStrings(x, y string) int {
	switch {
	case x < y:
		return -1
	case x > y:
		return 1
	default:
		return 0
	}
}

// fulfillVolumeGroupReplication builds the desired status: the shared
// VolumeReplication contract plus the group's member-PVC list.
func fulfillVolumeGroupReplication(
	vgr *volrep.VolumeGroupReplication, policy Policy, refs []corev1.LocalObjectReference,
) volrep.VolumeGroupReplicationStatus {
	primary := vgr.Spec.ReplicationState == volrep.Primary
	gen := vgr.GetGeneration()
	now := metav1.Now()

	status := volrep.VolumeGroupReplicationStatus{
		VolumeReplicationStatus: volrep.VolumeReplicationStatus{
			ObservedGeneration: gen,
			Conditions:         vrConditions(gen, primary, policy),
		},
		PersistentVolumeClaimsRefList: refs,
	}

	if primary {
		status.State = volrep.PrimaryState
		status.Message = "volume group is marked primary"
	} else {
		status.State = volrep.SecondaryState
		status.Message = "volume group is marked secondary"
	}

	status.LastSyncTime = &now

	return status
}

// vgrStatusFulfilled reports whether the live status already matches the
// desired one in every field ramen reads, keeping reconcile idempotent.
func vgrStatusFulfilled(vgr *volrep.VolumeGroupReplication, desired volrep.VolumeGroupReplicationStatus) bool {
	if !vrStatusFulfilled(&volrep.VolumeReplication{
		Status: vgr.Status.VolumeReplicationStatus,
	}, desired.VolumeReplicationStatus) {
		return false
	}

	return slices.Equal(vgr.Status.PersistentVolumeClaimsRefList, desired.PersistentVolumeClaimsRefList)
}
