// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"context"
	"fmt"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

// VRGBuilder builds VolumeReplicationGroup test fixtures.
type VRGBuilder struct {
	name                 string
	namespace            string
	replicationState     ramen.ReplicationState
	s3Profiles           []string
	pvcSelector          metav1.LabelSelector
	async                *ramen.VRGAsyncSpec
	sync                 *ramen.VRGSyncSpec
	kubeObjectProtection *ramen.KubeObjectProtectionSpec
}

// NewVRGBuilder creates a new VRGBuilder with defaults.
func NewVRGBuilder(name, namespace string) *VRGBuilder {
	return &VRGBuilder{
		name:             name,
		namespace:        namespace,
		replicationState: ramen.Primary,
		pvcSelector:      metav1.LabelSelector{},
	}
}

// WithReplicationState sets the replication state.
func (b *VRGBuilder) WithReplicationState(state ramen.ReplicationState) *VRGBuilder {
	b.replicationState = state

	return b
}

// WithS3Profiles sets the S3 profiles.
func (b *VRGBuilder) WithS3Profiles(profiles []string) *VRGBuilder {
	b.s3Profiles = profiles

	return b
}

// WithPVCSelector sets the PVC selector.
func (b *VRGBuilder) WithPVCSelector(selector metav1.LabelSelector) *VRGBuilder {
	b.pvcSelector = selector

	return b
}

// WithAsyncSpec sets the async specification.
func (b *VRGBuilder) WithAsyncSpec(schedulingInterval string) *VRGBuilder {
	b.async = &ramen.VRGAsyncSpec{
		SchedulingInterval: schedulingInterval,
	}

	return b
}

// WithSyncSpec sets the sync specification.
func (b *VRGBuilder) WithSyncSpec() *VRGBuilder {
	b.sync = &ramen.VRGSyncSpec{}

	return b
}

// WithKubeObjectProtection sets kube object protection.
func (b *VRGBuilder) WithKubeObjectProtection(spec *ramen.KubeObjectProtectionSpec) *VRGBuilder {
	b.kubeObjectProtection = spec

	return b
}

// Build creates the VolumeReplicationGroup.
func (b *VRGBuilder) Build() *ramen.VolumeReplicationGroup {
	vrg := &ramen.VolumeReplicationGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      b.name,
			Namespace: b.namespace,
		},
		Spec: ramen.VolumeReplicationGroupSpec{
			ReplicationState: b.replicationState,
			S3Profiles:       b.s3Profiles,
			PVCSelector:      b.pvcSelector,
		},
	}

	if b.async != nil {
		vrg.Spec.Async = b.async
	}

	if b.sync != nil {
		vrg.Spec.Sync = b.sync
	}

	if b.kubeObjectProtection != nil {
		vrg.Spec.KubeObjectProtection = b.kubeObjectProtection
	}

	return vrg
}

// GetVRG retrieves a VolumeReplicationGroup by name and namespace.
func GetVRG(ctx context.Context, reader client.Reader, name, namespace string) (*ramen.VolumeReplicationGroup, error) {
	vrg := &ramen.VolumeReplicationGroup{}
	err := reader.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, vrg)

	if err != nil {
		return nil, fmt.Errorf("failed to get VRG %s/%s: %w", namespace, name, err)
	}

	return vrg, nil
}

// UpdateVRGSpec updates the spec of a VRG with retry on conflict.
func UpdateVRGSpec(
	ctx context.Context,
	k8sClient client.Client,
	reader client.Reader,
	name, namespace string,
	updateFn func(*ramen.VolumeReplicationGroup),
) (*ramen.VolumeReplicationGroup, error) {
	var latestVRG *ramen.VolumeReplicationGroup

	retryErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		vrg, err := GetVRG(ctx, reader, name, namespace)
		if err != nil {
			return err
		}

		updateFn(vrg)
		latestVRG = vrg

		return k8sClient.Update(ctx, vrg)
	})

	if retryErr != nil {
		return nil, fmt.Errorf("failed to update VRG %s/%s: %w", namespace, name, retryErr)
	}

	return latestVRG, nil
}

// UpdateVRGStatus updates the status of a VRG with retry on conflict.
func UpdateVRGStatus(
	ctx context.Context,
	k8sClient client.Client,
	reader client.Reader,
	name, namespace string,
	updateFn func(*ramen.VolumeReplicationGroup),
) (*ramen.VolumeReplicationGroup, error) {
	var latestVRG *ramen.VolumeReplicationGroup

	retryErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		vrg, err := GetVRG(ctx, reader, name, namespace)
		if err != nil {
			return err
		}

		updateFn(vrg)
		latestVRG = vrg

		return k8sClient.Status().Update(ctx, vrg)
	})

	if retryErr != nil {
		return nil, fmt.Errorf("failed to update VRG status %s/%s: %w", namespace, name, retryErr)
	}

	return latestVRG, nil
}

// DeleteVRG deletes a VRG by name and namespace.
func DeleteVRG(ctx context.Context, k8sClient client.Client, name, namespace string) error {
	vrg := &ramen.VolumeReplicationGroup{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	return client.IgnoreNotFound(k8sClient.Delete(ctx, vrg))
}

// WaitForVRGCondition waits for a VRG to have a specific condition status.
func WaitForVRGCondition(
	ctx context.Context,
	reader client.Reader,
	name, namespace string,
	conditionType string,
	expectedStatus metav1.ConditionStatus,
	waitCond WaitForCondition,
) (*ramen.VolumeReplicationGroup, error) {
	var vrg *ramen.VolumeReplicationGroup

	err := wait.PollUntilContextTimeout(ctx, waitCond.Interval, waitCond.Timeout, true,
		func(ctx context.Context) (bool, error) {
			var err error
			vrg, err = GetVRG(ctx, reader, name, namespace)

			if err != nil {
				if k8serrors.IsNotFound(err) {
					return false, nil
				}

				return false, err
			}

			for _, cond := range vrg.Status.Conditions {
				if cond.Type == conditionType && cond.Status == expectedStatus {
					return true, nil
				}
			}

			return false, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for VRG %s/%s condition %s=%s: %w",
			namespace, name, conditionType, expectedStatus, err)
	}

	return vrg, nil
}

// WaitForVRGState waits for a VRG to reach a specific state.
func WaitForVRGState(
	ctx context.Context,
	reader client.Reader,
	name, namespace string,
	expectedState ramen.State,
	waitCond WaitForCondition,
) (*ramen.VolumeReplicationGroup, error) {
	var vrg *ramen.VolumeReplicationGroup

	err := wait.PollUntilContextTimeout(ctx, waitCond.Interval, waitCond.Timeout, true,
		func(ctx context.Context) (bool, error) {
			var err error
			vrg, err = GetVRG(ctx, reader, name, namespace)

			if err != nil {
				if k8serrors.IsNotFound(err) {
					return false, nil
				}

				return false, err
			}

			return vrg.Status.State == expectedState, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for VRG %s/%s state %s: %w",
			namespace, name, expectedState, err)
	}

	return vrg, nil
}

// WaitForVRGDeleted waits for a VRG to be deleted.
func WaitForVRGDeleted(
	ctx context.Context,
	reader client.Reader,
	name, namespace string,
	waitCond WaitForCondition,
) error {
	err := wait.PollUntilContextTimeout(ctx, waitCond.Interval, waitCond.Timeout, true,
		func(ctx context.Context) (bool, error) {
			_, err := GetVRG(ctx, reader, name, namespace)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return true, nil
				}

				return false, err
			}

			return false, nil
		})

	if err != nil {
		return fmt.Errorf("timeout waiting for VRG %s/%s to be deleted: %w", namespace, name, err)
	}

	return nil
}

// VRGStatusBuilder helps build VRG status for testing.
type VRGStatusBuilder struct {
	state                ramen.State
	observedGeneration   int64
	lastUpdateTime       metav1.Time
	conditions           []metav1.Condition
	protectedPVCs        []ramen.ProtectedPVC
	lastGroupSyncTime    *metav1.Time
	lastGroupSyncBytes   *int64
	lastGroupSyncDuration *metav1.Duration
}

// NewVRGStatusBuilder creates a new VRGStatusBuilder.
func NewVRGStatusBuilder() *VRGStatusBuilder {
	return &VRGStatusBuilder{
		state:          ramen.PrimaryState,
		lastUpdateTime: metav1.Time{Time: time.Now()},
	}
}

// WithState sets the state.
func (b *VRGStatusBuilder) WithState(state ramen.State) *VRGStatusBuilder {
	b.state = state

	return b
}

// WithObservedGeneration sets the observed generation.
func (b *VRGStatusBuilder) WithObservedGeneration(gen int64) *VRGStatusBuilder {
	b.observedGeneration = gen

	return b
}

// WithCondition adds a condition.
func (b *VRGStatusBuilder) WithCondition(condType string, status metav1.ConditionStatus, reason, message string) *VRGStatusBuilder {
	b.conditions = append(b.conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Time{Time: time.Now()},
		Reason:             reason,
		Message:            message,
	})

	return b
}

// VRG Condition types for protected PVCs.
const (
	// VRGConditionTypeDataReady indicates data is ready.
	VRGConditionTypeDataReady = "DataReady"
	// VRGConditionTypeClusterDataReady indicates cluster data is ready.
	VRGConditionTypeClusterDataReady = "ClusterDataReady"
)

// WithProtectedPVC adds a protected PVC.
func (b *VRGStatusBuilder) WithProtectedPVC(name, namespace, storageClass string, protected bool) *VRGStatusBuilder {
	pvc := ramen.ProtectedPVC{
		Name:             name,
		Namespace:        namespace,
		StorageClassName: &storageClass,
	}

	if protected {
		pvc.Conditions = []metav1.Condition{
			{
				Type:               VRGConditionTypeDataReady,
				Status:             metav1.ConditionTrue,
				LastTransitionTime: metav1.Now(),
				Reason:             "Protected",
				Message:            "PVC is protected",
			},
		}
	}

	b.protectedPVCs = append(b.protectedPVCs, pvc)

	return b
}

// Build creates the VRG status.
func (b *VRGStatusBuilder) Build() ramen.VolumeReplicationGroupStatus {
	return ramen.VolumeReplicationGroupStatus{
		State:              b.state,
		ObservedGeneration: b.observedGeneration,
		LastUpdateTime:     b.lastUpdateTime,
		Conditions:         b.conditions,
		ProtectedPVCs:      b.protectedPVCs,
	}
}

// ApplyStatusToVRG applies the built status to a VRG.
func (b *VRGStatusBuilder) ApplyStatusToVRG(vrg *ramen.VolumeReplicationGroup) {
	vrg.Status = b.Build()
}
