// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"time"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// volRepActor stands in for the csi-addons VolumeReplication controller: it
// fulfills VolumeReplication.status the way a real storage-side controller
// would, so VRG promotion/demotion completes with no storage backend
// installed. It is gated by the policy store so tests can inject failure
// modes (Silent, Delayed, FailWith) per cluster.
type volRepActor struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupVolRep(mgr manager.Manager, cluster string, rt *Runtime) error {
	a := &volRepActor{client: mgr.GetClient(), cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&volrep.VolumeReplication{}).
		Named("volrep-" + cluster).
		Complete(a)
}

func (a *volRepActor) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	vr := &volrep.VolumeReplication{}
	if err := a.client.Get(ctx, req.NamespacedName, vr); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if vr.GetDeletionTimestamp() != nil {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(VolRep(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	desired := fulfillVolumeReplication(vr, d.Policy)
	if vrStatusFulfilled(vr, desired) {
		return ctrl.Result{}, nil
	}

	vr.Status = desired
	if err := a.client.Status().Update(ctx, vr); err != nil {
		return ctrl.Result{}, err
	}

	a.rt.Log.Logf("volrep@%s fulfilled %s state=%s policy=%T", a.cluster, req.NamespacedName, desired.State, d.Policy)

	return ctrl.Result{}, nil
}

// fulfillVolumeReplication is the port of mock.FulfillVolumeReplication with
// failure-mode injection layered on top.
func fulfillVolumeReplication(vr *volrep.VolumeReplication, policy Policy) volrep.VolumeReplicationStatus {
	primary := vr.Spec.ReplicationState == volrep.Primary
	gen := vr.GetGeneration()
	now := metav1.Now()

	status := volrep.VolumeReplicationStatus{
		ObservedGeneration: gen,
		Conditions:         vrConditions(gen, primary, policy),
	}

	if primary {
		status.State = volrep.PrimaryState
		status.Message = "volume is marked primary"
		status.DestinationVolumeID = fmt.Sprintf("mock-%s-%s", vr.GetNamespace(), vr.GetName())
	} else {
		status.State = volrep.SecondaryState
		status.Message = "volume is marked secondary"
	}

	status.LastSyncTime = &now
	syncDuration := metav1.Duration{Duration: time.Second}
	status.LastSyncDuration = &syncDuration

	return status
}

func vrConditions(gen int64, primary bool, policy Policy) []metav1.Condition {
	validated, completed := metav1.ConditionTrue, metav1.ConditionTrue
	degraded, resyncing := metav1.ConditionFalse, metav1.ConditionFalse

	if fw, ok := policy.(FailWith); ok {
		switch fw.Mode {
		case "validated-false":
			validated, completed = metav1.ConditionFalse, metav1.ConditionFalse
		case "degraded":
			degraded, resyncing = metav1.ConditionTrue, metav1.ConditionTrue
		}
	}

	completedReason := volrep.Promoted
	if !primary {
		completedReason = volrep.Demoted
	}

	now := metav1.Now()
	mk := func(typ string, st metav1.ConditionStatus, reason, msg string) metav1.Condition {
		return metav1.Condition{
			Type: typ, Status: st, Reason: reason, Message: msg,
			ObservedGeneration: gen, LastTransitionTime: now,
		}
	}

	return []metav1.Condition{
		mk(volrep.ConditionValidated, validated, volrep.PrerequisiteMet, "volume is validated"),
		mk(volrep.ConditionCompleted, completed, completedReason, "replication state set"),
		mk(volrep.ConditionDegraded, degraded, volrep.Healthy, "volume health"),
		mk(volrep.ConditionResyncing, resyncing, volrep.NotResyncing, "resync state"),
	}
}

func vrStatusFulfilled(vr *volrep.VolumeReplication, desired volrep.VolumeReplicationStatus) bool {
	if vr.Status.State != desired.State ||
		vr.Status.ObservedGeneration != desired.ObservedGeneration ||
		vr.Status.DestinationVolumeID != desired.DestinationVolumeID {
		return false
	}

	for _, want := range desired.Conditions {
		got := findCondition(vr.Status.Conditions, want.Type)
		if got == nil || got.Status != want.Status || got.ObservedGeneration != want.ObservedGeneration {
			return false
		}
	}

	return true
}

func findCondition(conds []metav1.Condition, typ string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == typ {
			return &conds[i]
		}
	}

	return nil
}
