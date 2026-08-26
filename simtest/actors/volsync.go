// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	snapv1 "github.com/kubernetes-csi/external-snapshotter/client/v8/apis/volumesnapshot/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	volumeSnapshotKind        = "VolumeSnapshot"
	volumeSnapshotGroup       = "snapshot.storage.k8s.io"
	mockRDPort          int32 = 8000

	// pvcBindRequeue is how long to wait before re-checking whether the
	// destination PVC has bound. The pvbinder actor binds it out of band.
	pvcBindRequeue = 2 * time.Second

	// defaultDestPVCCapacity is used when the ReplicationDestination spec
	// does not carry a capacity (it normally does, from the protected PVC).
	defaultDestPVCCapacity = "1Gi"
)

// volSyncActor stands in for the VolSync operator on one managed cluster,
// fulfilling both sides of a replication pair the way the real mover would:
// ReplicationDestination gets a materialized destination PVC, a latestImage
// VolumeSnapshot of it, and the rsyncTLS address Ramen's restore path waits
// on; ReplicationSource gets sync timestamps and the manual-trigger echo the
// final-sync wait reads. Gated by the policy store under the VolSync key so
// tests can starve either side.
type volSyncActor struct {
	client  client.Client
	cluster string
	rt      *Runtime
}

func setupVolSync(mgr manager.Manager, cluster string, rt *Runtime) error {
	a := &volSyncActor{client: mgr.GetClient(), cluster: cluster, rt: rt}

	if err := ctrl.NewControllerManagedBy(mgr).
		For(&volsyncv1alpha1.ReplicationDestination{}).
		Named("volsync-rd-" + cluster).
		Complete(reconcile.Func(a.reconcileRD)); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&volsyncv1alpha1.ReplicationSource{}).
		Named("volsync-rs-" + cluster).
		Complete(reconcile.Func(a.reconcileRS))
}

func (a *volSyncActor) reconcileRD(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	rd := &volsyncv1alpha1.ReplicationDestination{}
	if err := a.client.Get(ctx, req.NamespacedName, rd); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !rd.GetDeletionTimestamp().IsZero() || replicationDestinationReady(rd) {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(VolSync(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	// Materialize a real destination PVC so the latestImage snapshot has a
	// genuinely restorable source; the pvbinder binds it out of band.
	pvc, err := a.ensureDestinationPVC(ctx, rd)
	if err != nil {
		return ctrl.Result{}, err
	}

	if pvc.Status.Phase != corev1.ClaimBound {
		return ctrl.Result{RequeueAfter: pvcBindRequeue}, nil
	}

	snapName := "mock-latestimage-" + rd.GetName()
	if err := a.ensureLatestImageSnapshot(ctx, rd, snapName, pvc.GetName()); err != nil {
		return ctrl.Result{}, err
	}

	now := metav1.Now()
	address := fmt.Sprintf("mock-rd-%s.%s.svc:%d", rd.GetName(), rd.GetNamespace(), mockRDPort)
	rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LastSyncTime:      &now,
		LastSyncStartTime: &now,
		// RGD-owned destinations use manual-trigger semantics: the RGD
		// state machine treats an RD as completed only when the trigger it
		// set is echoed back here.
		LastManualSync: rdManualTrigger(rd),
		RsyncTLS:       &volsyncv1alpha1.ReplicationDestinationRsyncTLSStatus{Address: ptr.To(address)},
		LatestImage: &corev1.TypedLocalObjectReference{
			APIGroup: ptr.To(volumeSnapshotGroup),
			Kind:     volumeSnapshotKind,
			Name:     snapName,
		},
	}

	if err := a.client.Status().Update(ctx, rd); err != nil {
		return ctrl.Result{}, fmt.Errorf("update RD status: %w", err)
	}

	a.rt.Log.Logf("volsync@%s fulfilled RD %s latestImage=%s policy=%T",
		a.cluster, req.NamespacedName, snapName, d.Policy)

	return ctrl.Result{}, nil
}

func (a *volSyncActor) reconcileRS(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	rs := &volsyncv1alpha1.ReplicationSource{}
	if err := a.client.Get(ctx, req.NamespacedName, rs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	manual := ""
	if rs.Spec.Trigger != nil {
		manual = rs.Spec.Trigger.Manual
	}

	if !rs.GetDeletionTimestamp().IsZero() || replicationSourceSynced(rs, manual) {
		return ctrl.Result{}, nil
	}

	d := a.rt.Store.Decide(VolSync(a.cluster), req.String())
	if !d.Proceed {
		return ctrl.Result{RequeueAfter: d.RequeueAfter}, nil
	}

	now := metav1.Now()

	if rs.Status == nil {
		rs.Status = &volsyncv1alpha1.ReplicationSourceStatus{}
	}

	rs.Status.LastSyncTime = &now
	rs.Status.LastSyncStartTime = &now
	rs.Status.LastManualSync = manual
	// Real VolSync records each mover run here; ramen reads it without a
	// nil check on the failover rollback path (vshandler.go
	// rollbackToLastSnapshot), so a faithful fulfiller must set it.
	rs.Status.LatestMoverStatus = &volsyncv1alpha1.MoverStatus{
		Result: volsyncv1alpha1.MoverResultSuccessful,
		Logs:   "mock mover completed",
	}

	if err := a.client.Status().Update(ctx, rs); err != nil {
		return ctrl.Result{}, fmt.Errorf("update RS status: %w", err)
	}

	a.rt.Log.Logf("volsync@%s fulfilled RS %s lastManualSync=%q policy=%T",
		a.cluster, req.NamespacedName, manual, d.Policy)

	return ctrl.Result{}, nil
}

// rdOwnerRef controller-owns an actor artifact by its RD, so a real
// garbage collector (kind backend) cascades it; envtest's janitor sweeps
// by name prefix either way.
func rdOwnerRef(rd *volsyncv1alpha1.ReplicationDestination) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: volsyncv1alpha1.GroupVersion.String(),
		Kind:       "ReplicationDestination",
		Name:       rd.GetName(),
		UID:        rd.GetUID(),
		Controller: ptr.To(true),
	}
}

// mockDestinationPVCName is the mock destination PVC for an RD. It must NOT
// be the protected PVC name (== rd.GetName()): Ramen restores the workload
// PVC under the protected PVC name, so the destination PVC needs its own.
func mockDestinationPVCName(rd *volsyncv1alpha1.ReplicationDestination) string {
	return "mock-volsync-dst-" + rd.GetName()
}

func (a *volSyncActor) ensureDestinationPVC(
	ctx context.Context, rd *volsyncv1alpha1.ReplicationDestination,
) (*corev1.PersistentVolumeClaim, error) {
	name := mockDestinationPVCName(rd)

	pvc := &corev1.PersistentVolumeClaim{}

	err := a.client.Get(ctx, types.NamespacedName{Namespace: rd.GetNamespace(), Name: name}, pvc)
	if err == nil {
		return pvc, nil
	}

	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get destination PVC: %w", err)
	}

	pvc = &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: rd.GetNamespace(),
			OwnerReferences: []metav1.OwnerReference{rdOwnerRef(rd)},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      destinationAccessModes(rd),
			StorageClassName: destinationStorageClassName(rd),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: destinationCapacity(rd)},
			},
		},
	}
	if err := a.client.Create(ctx, pvc); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create destination PVC: %w", err)
	}

	return pvc, nil
}

func (a *volSyncActor) ensureLatestImageSnapshot(
	ctx context.Context, rd *volsyncv1alpha1.ReplicationDestination, snapName, sourcePVCName string,
) error {
	snap := &snapv1.VolumeSnapshot{}

	err := a.client.Get(ctx, types.NamespacedName{Namespace: rd.GetNamespace(), Name: snapName}, snap)
	if err == nil {
		return nil
	}

	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get latestImage snapshot: %w", err)
	}

	snap = &snapv1.VolumeSnapshot{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapName, Namespace: rd.GetNamespace(),
			OwnerReferences: []metav1.OwnerReference{rdOwnerRef(rd)},
		},
		Spec: snapv1.VolumeSnapshotSpec{
			Source:                  snapv1.VolumeSnapshotSource{PersistentVolumeClaimName: ptr.To(sourcePVCName)},
			VolumeSnapshotClassName: destinationVolumeSnapshotClassName(rd),
		},
	}
	if err := a.client.Create(ctx, snap); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create latestImage snapshot: %w", err)
	}

	return nil
}

// destinationVolumeOptions returns the RD's RsyncTLS volume options, or nil
// when the RD uses an External mover (which the actor does not materialize).
func destinationVolumeOptions(
	rd *volsyncv1alpha1.ReplicationDestination,
) *volsyncv1alpha1.ReplicationDestinationVolumeOptions {
	if rd.Spec.RsyncTLS == nil {
		return nil
	}

	return &rd.Spec.RsyncTLS.ReplicationDestinationVolumeOptions
}

func destinationAccessModes(rd *volsyncv1alpha1.ReplicationDestination) []corev1.PersistentVolumeAccessMode {
	if opts := destinationVolumeOptions(rd); opts != nil && len(opts.AccessModes) > 0 {
		return opts.AccessModes
	}

	return []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce}
}

func destinationStorageClassName(rd *volsyncv1alpha1.ReplicationDestination) *string {
	if opts := destinationVolumeOptions(rd); opts != nil {
		return opts.StorageClassName
	}

	return nil
}

func destinationVolumeSnapshotClassName(rd *volsyncv1alpha1.ReplicationDestination) *string {
	if opts := destinationVolumeOptions(rd); opts != nil {
		return opts.VolumeSnapshotClassName
	}

	return nil
}

func destinationCapacity(rd *volsyncv1alpha1.ReplicationDestination) resource.Quantity {
	if opts := destinationVolumeOptions(rd); opts != nil && opts.Capacity != nil {
		return *opts.Capacity
	}

	return resource.MustParse(defaultDestPVCCapacity)
}

func rdManualTrigger(rd *volsyncv1alpha1.ReplicationDestination) string {
	if rd.Spec.Trigger == nil {
		return ""
	}

	return rd.Spec.Trigger.Manual
}

func replicationDestinationReady(rd *volsyncv1alpha1.ReplicationDestination) bool {
	return rd.Status != nil &&
		rd.Status.RsyncTLS != nil && rd.Status.RsyncTLS.Address != nil &&
		rd.Status.LatestImage != nil && rd.Status.LatestImage.Name != "" &&
		rd.Status.LastManualSync == rdManualTrigger(rd)
}

func replicationSourceSynced(rs *volsyncv1alpha1.ReplicationSource, manual string) bool {
	return rs.Status != nil && rs.Status.LastSyncTime != nil && rs.Status.LastManualSync == manual
}
