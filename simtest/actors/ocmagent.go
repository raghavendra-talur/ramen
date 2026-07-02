// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"encoding/json"
	"time"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ocmworkv1 "open-cluster-management.io/api/work/v1"
	viewv1beta1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/view/v1beta1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

const (
	workFinalizer = "simtest.ramendr.openshift.io/work-cleanup"
	fieldOwner    = client.FieldOwner("simtest-work-agent")
	viewRefresh   = time.Second
)

// setupOCMAgents stands in for OCM between the ramen hub operator and its
// managed clusters: a work agent that applies ManifestWork manifests to the
// managed cluster and reflects Applied/Available status back to the hub, and
// a view agent that fulfills ManagedClusterView reads from the managed
// cluster. Both run on the hub manager, scoped to ManifestWork/MCV objects in
// the hub namespace named after the managed cluster.
func setupOCMAgents(mgr manager.Manager, cluster string, managedClient client.Client, rt *Runtime) error {
	inCluster := predicate.NewPredicateFuncs(func(o client.Object) bool { return o.GetNamespace() == cluster })

	wa := &workAgent{hub: mgr.GetClient(), managed: managedClient, cluster: cluster, rt: rt}
	if err := ctrl.NewControllerManagedBy(mgr).
		For(&ocmworkv1.ManifestWork{}, builder.WithPredicates(inCluster)).
		Named("work-agent-" + cluster).
		Complete(wa); err != nil {
		return err
	}

	va := &viewAgent{hub: mgr.GetClient(), managed: managedClient, cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&viewv1beta1.ManagedClusterView{}, builder.WithPredicates(inCluster)).
		Named("view-agent-" + cluster).
		Complete(va)
}

// ---- work agent ----

type workAgent struct {
	hub, managed client.Client
	cluster      string
	rt           *Runtime
}

func (a *workAgent) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	mw := &ocmworkv1.ManifestWork{}
	if err := a.hub.Get(ctx, req.NamespacedName, mw); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	d := a.rt.Store.Decide(Work(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	if mw.GetDeletionTimestamp() != nil {
		if !controllerutil.ContainsFinalizer(mw, workFinalizer) {
			return ctrl.Result{}, nil
		}

		if err := a.deleteManifests(ctx, mw); err != nil {
			return ctrl.Result{}, err
		}
		controllerutil.RemoveFinalizer(mw, workFinalizer)

		return ctrl.Result{}, a.hub.Update(ctx, mw)
	}

	if controllerutil.AddFinalizer(mw, workFinalizer) {
		if err := a.hub.Update(ctx, mw); err != nil {
			return ctrl.Result{}, err
		}
	}

	for i := range mw.Spec.Workload.Manifests {
		obj, err := decodeManifest(mw.Spec.Workload.Manifests[i].Raw)
		if err != nil {
			return ctrl.Result{}, err
		}
		if err := a.managed.Patch(ctx, obj, client.Apply, fieldOwner, client.ForceOwnership); err != nil {
			return ctrl.Result{}, err
		}
	}

	appliedChanged := setMWCondition(mw, ocmworkv1.WorkApplied, "AppliedManifestWorkComplete")
	availableChanged := setMWCondition(mw, ocmworkv1.WorkAvailable, "ResourcesAvailable")
	if appliedChanged || availableChanged {
		if err := a.hub.Status().Update(ctx, mw); err != nil {
			return ctrl.Result{}, err
		}
		a.rt.Log.Logf("work@%s applied %s (%d manifests)", a.cluster, req.NamespacedName, len(mw.Spec.Workload.Manifests))
	}

	return ctrl.Result{}, nil
}

func (a *workAgent) deleteManifests(ctx context.Context, mw *ocmworkv1.ManifestWork) error {
	for i := range mw.Spec.Workload.Manifests {
		obj, err := decodeManifest(mw.Spec.Workload.Manifests[i].Raw)
		if err != nil {
			a.rt.Log.Logf("work@%s delete: manifest %d of %s/%s undecodable: %v", a.cluster, i, mw.Namespace, mw.Name, err)
			return err
		}
		if err := a.managed.Delete(ctx, obj); err != nil && !errors.IsNotFound(err) {
			return err
		}
	}

	a.rt.Log.Logf("work@%s cleaned up %s/%s", a.cluster, mw.Namespace, mw.Name)

	return nil
}

func decodeManifest(raw []byte) (*unstructured.Unstructured, error) {
	obj := &unstructured.Unstructured{}
	if err := obj.UnmarshalJSON(raw); err != nil {
		return nil, err
	}

	return obj, nil
}

// setMWCondition sets typ to True on mw's status and reports whether that
// changed the condition (new type, or a Status/Reason/ObservedGeneration
// transition). Callers must not short-circuit across the two MW conditions
// (Applied, Available) — both need to run so both get applied.
func setMWCondition(mw *ocmworkv1.ManifestWork, condType, reason string) bool {
	return meta.SetStatusCondition(&mw.Status.Conditions, metav1.Condition{
		Type: condType, Status: metav1.ConditionTrue, Reason: reason,
		Message: "simtest work agent", ObservedGeneration: mw.Generation,
	})
}

// ---- view agent ----

type viewAgent struct {
	hub, managed client.Client
	cluster      string
	rt           *Runtime
}

func (a *viewAgent) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	mcv := &viewv1beta1.ManagedClusterView{}
	if err := a.hub.Get(ctx, req.NamespacedName, mcv); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if mcv.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(View(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	scope := mcv.Spec.Scope
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(schema.GroupVersionKind{Group: scope.Group, Version: scope.Version, Kind: scope.Kind})

	cond := metav1.Condition{
		Type: viewv1beta1.ConditionViewProcessing, ObservedGeneration: mcv.Generation,
		LastTransitionTime: metav1.Now(),
	}

	err := a.managed.Get(ctx, types.NamespacedName{Name: scope.Name, Namespace: scope.Namespace}, obj)

	switch {
	case err == nil:
		raw, jerr := json.Marshal(obj.Object)
		if jerr != nil {
			return ctrl.Result{}, jerr
		}
		mcv.Status.Result.Raw = raw
		cond.Status = metav1.ConditionTrue
		cond.Reason = viewv1beta1.ReasonGetResource
		cond.Message = "Watching resources successfully"
	default:
		mcv.Status.Result.Raw = nil
		cond.Status = metav1.ConditionFalse
		cond.Reason = viewv1beta1.ReasonGetResourceFailed
		cond.Message = err.Error() // contains "not found" for NotFound; ramen's parseErrorMessage matches on this
	}

	// Log fulfillment only on a condition-status transition: the view agent
	// requeues every second by design, and re-logging identical fulfillments
	// every second would make the event log unreadable.
	prevStatus, hadPrev := metav1.ConditionUnknown, false
	if len(mcv.Status.Conditions) == 1 {
		prevStatus, hadPrev = mcv.Status.Conditions[0].Status, true
	}

	mcv.Status.Conditions = []metav1.Condition{cond} // contract: exactly one condition

	if err := a.hub.Status().Update(ctx, mcv); err != nil {
		return ctrl.Result{}, err
	}

	if !hadPrev || prevStatus != cond.Status {
		a.rt.Log.Logf("view@%s fulfilled %s status=%s reason=%s", a.cluster, req.NamespacedName, cond.Status, cond.Reason)
	}

	return ctrl.Result{RequeueAfter: viewRefresh}, nil
}
