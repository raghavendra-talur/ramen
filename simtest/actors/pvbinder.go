// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// pvBinder stands in for kube-controller-manager + a CSI provisioner: envtest
// runs no controllers, so nothing else would ever bind a PVC.
type pvBinder struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupPVBinder(mgr manager.Manager, cluster string, rt *Runtime) error {
	b := &pvBinder{client: mgr.GetClient(), cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}).
		Named("pvbinder-" + cluster).
		Complete(b)
}

func (b *pvBinder) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	pvc := &corev1.PersistentVolumeClaim{}

	err := b.client.Get(ctx, req.NamespacedName, pvc)
	if errors.IsNotFound(err) {
		// PVC is gone: reclaim its bound PV the way kube-controller-manager +
		// a CSI provisioner would (reclaimPolicy Delete). Without this, a PV
		// left Bound to a deleted claim wedges ramen's PV/PVC restore on the
		// next relocate/failover back to this cluster ("found bound PV ... but
		// unable to validate claim exists").
		d := b.rt.Store.Decide(Binder(b.cluster), req.String())
		if !d.Proceed {
			return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
		}

		return ctrl.Result{}, b.reclaimPVs(ctx, req.NamespacedName)
	}

	if err != nil {
		return ctrl.Result{}, err
	}

	if pvc.GetDeletionTimestamp() != nil || pvc.Status.Phase == corev1.ClaimBound {
		return ctrl.Result{}, nil
	}

	d := b.rt.Store.Decide(Binder(b.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	pvName := pvc.Spec.VolumeName
	if pvName == "" {
		pvName = "pv-" + string(pvc.UID)
	}

	if err := b.ensurePV(ctx, pvName, pvc); err != nil {
		return ctrl.Result{}, err
	}

	if pvc.Spec.VolumeName == "" {
		pvc.Spec.VolumeName = pvName
		if err := b.client.Update(ctx, pvc); err != nil {
			return ctrl.Result{}, err
		}
	}

	pvc.Status.Phase = corev1.ClaimBound
	pvc.Status.AccessModes = pvc.Spec.AccessModes
	pvc.Status.Capacity = pvc.Spec.Resources.Requests

	if err := b.client.Status().Update(ctx, pvc); err != nil {
		return ctrl.Result{}, err
	}

	b.rt.Log.Logf("binder@%s bound pvc %s -> pv %s", b.cluster, req.NamespacedName, pvName)

	return ctrl.Result{}, nil
}

func (b *pvBinder) ensurePV(ctx context.Context, pvName string, pvc *corev1.PersistentVolumeClaim) error {
	pv := &corev1.PersistentVolume{}

	err := b.client.Get(ctx, types.NamespacedName{Name: pvName}, pv)
	if errors.IsNotFound(err) {
		pv = &corev1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{Name: pvName},
			Spec: corev1.PersistentVolumeSpec{
				Capacity:    pvc.Spec.Resources.Requests,
				AccessModes: pvc.Spec.AccessModes,
				PersistentVolumeSource: corev1.PersistentVolumeSource{
					CSI: &corev1.CSIPersistentVolumeSource{Driver: "mock.csi.ramen.io", VolumeHandle: pvName},
				},
				PersistentVolumeReclaimPolicy: corev1.PersistentVolumeReclaimDelete,
				StorageClassName:              deref(pvc.Spec.StorageClassName),
				ClaimRef: &corev1.ObjectReference{
					Kind: "PersistentVolumeClaim", Namespace: pvc.Namespace, Name: pvc.Name, UID: pvc.UID,
				},
			},
		}
		if err := b.client.Create(ctx, pv); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}

	// Adopt restored PVs whose ClaimRef points at a prior incarnation of this claim.
	if pv.Spec.ClaimRef != nil && pv.Spec.ClaimRef.Name == pvc.Name &&
		pv.Spec.ClaimRef.Namespace == pvc.Namespace && pv.Spec.ClaimRef.UID != pvc.UID {
		pv.Spec.ClaimRef.UID = pvc.UID
		if err := b.client.Update(ctx, pv); err != nil {
			return err
		}
	}

	if pv.Status.Phase != corev1.VolumeBound {
		pv.Status.Phase = corev1.VolumeBound
		if err := b.client.Status().Update(ctx, pv); err != nil {
			return err
		}
	}

	return nil
}

// reclaimPVs deletes every Bound, Delete-reclaim PV whose ClaimRef points at
// the (now deleted) claim. PVs that were never bound by us (empty phase, e.g.
// just restored from S3 by ramen and awaiting their PVC) are left alone.
func (b *pvBinder) reclaimPVs(ctx context.Context, claim types.NamespacedName) error {
	pvs := &corev1.PersistentVolumeList{}
	if err := b.client.List(ctx, pvs); err != nil {
		return err
	}

	for i := range pvs.Items {
		pv := &pvs.Items[i]
		ref := pv.Spec.ClaimRef

		if ref == nil || ref.Namespace != claim.Namespace || ref.Name != claim.Name {
			continue
		}
		if pv.Status.Phase != corev1.VolumeBound || pv.GetDeletionTimestamp() != nil {
			continue
		}
		if pv.Spec.PersistentVolumeReclaimPolicy != corev1.PersistentVolumeReclaimDelete {
			continue
		}

		if err := b.client.Delete(ctx, pv); err != nil && !errors.IsNotFound(err) {
			return err
		}

		b.rt.Log.Logf("binder@%s reclaimed pv %s (claim %s deleted)", b.cluster, pv.Name, claim)
	}

	return nil
}

func deref(s *string) string {
	if s == nil {
		return ""
	}

	return *s
}
