// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
)

// jobRunner stands in for the job controller and kubelet, neither of which
// runs in envtest: every Job is marked successfully complete. Ramen's
// VolSync path gates ReplicationSource creation on a PVC mount job
// completing, and recipe hooks run jobs too. Gated by the policy store
// under the Jobs key so faults can hold a job in progress or fail it.
type jobRunner struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupJobRunner(mgr manager.Manager, cluster string, rt *Runtime) error {
	a := &jobRunner{client: mgr.GetClient(), cluster: cluster, rt: rt}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.Job{}).
		Named("jobs-" + cluster).
		Complete(a)
}

func (a *jobRunner) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	job := &batchv1.Job{}
	if err := a.client.Get(ctx, req.NamespacedName, job); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if job.GetDeletionTimestamp() != nil || jobDone(job) {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(Jobs(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	now := metav1.Now()
	job.Status.StartTime = &now
	job.Status.CompletionTime = &now
	job.Status.Succeeded = 1
	// A live job controller (kind backend) may have counted a pod already;
	// a finished job with active>0 fails apiserver validation, so the
	// terminal status must zero the live counters.
	job.Status.Active = 0
	job.Status.Ready = ptr.To(int32(0))
	job.Status.Terminating = ptr.To(int32(0))
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{
			Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue,
			LastProbeTime: now, LastTransitionTime: now,
		},
		batchv1.JobCondition{
			Type: batchv1.JobComplete, Status: corev1.ConditionTrue,
			LastProbeTime: now, LastTransitionTime: now,
		})

	if err := a.client.Status().Update(ctx, job); err != nil {
		return ctrl.Result{}, err
	}

	a.rt.Log.Logf("jobs@%s completed %s policy=%T", a.cluster, req.NamespacedName, d.Policy)

	return ctrl.Result{}, nil
}

func jobDone(job *batchv1.Job) bool {
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return true
		}
	}

	return false
}
