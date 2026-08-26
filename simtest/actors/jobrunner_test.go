// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// The job runner stands in for the job controller + kubelet: envtest runs
// neither, so Jobs would stay pending forever. Ramen's VolSync path gates RS
// creation on a PVC mount job completing, so every Job is marked complete.
func TestJobRunnerCompletesJobs(t *testing.T) {
	// active=1 mimics a live job controller (kind backend) having already
	// created a pod: a job status claiming Complete with active>0 is
	// rejected by apiserver validation, so the runner must zero it.
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "volsync-pvc-mount-data", Namespace: "app-ns"},
		Status:     batchv1.JobStatus{Active: 1, Ready: ptr.To(int32(1))},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&batchv1.Job{}).
		WithObjects(job).Build()
	a := &jobRunner{client: cl, cluster: "dr1", rt: newTestRuntime(t)}
	ctx := context.Background()
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "volsync-pvc-mount-data"}}

	if _, err := a.Reconcile(ctx, req); err != nil {
		t.Fatal(err)
	}

	got := &batchv1.Job{}
	if err := cl.Get(ctx, req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}

	complete := false
	for _, c := range got.Status.Conditions {
		if c.Type == batchv1.JobComplete && c.Status == corev1.ConditionTrue {
			complete = true
		}
	}

	if !complete || got.Status.Succeeded != 1 {
		t.Fatalf("job not completed: %+v", got.Status)
	}
	if got.Status.Active != 0 || (got.Status.Ready != nil && *got.Status.Ready != 0) {
		t.Fatalf("terminal status must zero active/ready: %+v", got.Status)
	}
}

func TestJobRunnerSilentPolicyStalls(t *testing.T) {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "volsync-pvc-mount-data", Namespace: "app-ns"},
	}
	cl := fake.NewClientBuilder().WithScheme(volsyncScheme(t)).
		WithStatusSubresource(&batchv1.Job{}).
		WithObjects(job).Build()
	rt := newTestRuntime(t)
	rt.Store.Set(Jobs("dr1"), Silent{})
	a := &jobRunner{client: cl, cluster: "dr1", rt: rt}
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: "app-ns", Name: "volsync-pvc-mount-data"}}

	res, err := a.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if res.RequeueAfter == 0 {
		t.Fatal("silent policy must requeue")
	}

	got := &batchv1.Job{}
	if err := cl.Get(context.Background(), req.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Conditions) != 0 {
		t.Fatal("silent policy must not complete the job")
	}
}
