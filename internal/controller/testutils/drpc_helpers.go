// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

// DRPCBuilder builds DRPlacementControl test fixtures.
type DRPCBuilder struct {
	name                  string
	namespace             string
	drPolicyRef           string
	placementRef          string
	pvcSelector           metav1.LabelSelector
	action                ramen.DRAction
	preferredCluster      string
	failoverCluster       string
	kubeObjectProtection  *ramen.KubeObjectProtectionSpec
}

// NewDRPCBuilder creates a new DRPCBuilder with defaults.
func NewDRPCBuilder(name, namespace string) *DRPCBuilder {
	return &DRPCBuilder{
		name:       name,
		namespace:  namespace,
		pvcSelector: metav1.LabelSelector{},
	}
}

// WithDRPolicyRef sets the DRPolicy reference.
func (b *DRPCBuilder) WithDRPolicyRef(name string) *DRPCBuilder {
	b.drPolicyRef = name

	return b
}

// WithPlacementRef sets the placement reference.
func (b *DRPCBuilder) WithPlacementRef(name string) *DRPCBuilder {
	b.placementRef = name

	return b
}

// WithPVCSelector sets the PVC selector.
func (b *DRPCBuilder) WithPVCSelector(selector metav1.LabelSelector) *DRPCBuilder {
	b.pvcSelector = selector

	return b
}

// WithAction sets the DR action.
func (b *DRPCBuilder) WithAction(action ramen.DRAction) *DRPCBuilder {
	b.action = action

	return b
}

// WithPreferredCluster sets the preferred cluster.
func (b *DRPCBuilder) WithPreferredCluster(cluster string) *DRPCBuilder {
	b.preferredCluster = cluster

	return b
}

// WithFailoverCluster sets the failover cluster.
func (b *DRPCBuilder) WithFailoverCluster(cluster string) *DRPCBuilder {
	b.failoverCluster = cluster

	return b
}

// WithKubeObjectProtection sets kube object protection.
func (b *DRPCBuilder) WithKubeObjectProtection(spec *ramen.KubeObjectProtectionSpec) *DRPCBuilder {
	b.kubeObjectProtection = spec

	return b
}

// Build creates the DRPlacementControl.
func (b *DRPCBuilder) Build() *ramen.DRPlacementControl {
	drpc := &ramen.DRPlacementControl{
		ObjectMeta: metav1.ObjectMeta{
			Name:      b.name,
			Namespace: b.namespace,
		},
		Spec: ramen.DRPlacementControlSpec{
			DRPolicyRef: corev1.ObjectReference{
				Name: b.drPolicyRef,
			},
			PlacementRef: corev1.ObjectReference{
				Name: b.placementRef,
			},
			PVCSelector:      b.pvcSelector,
			Action:           b.action,
			PreferredCluster: b.preferredCluster,
			FailoverCluster:  b.failoverCluster,
		},
	}

	if b.kubeObjectProtection != nil {
		drpc.Spec.KubeObjectProtection = b.kubeObjectProtection
	}

	return drpc
}

// GetDRPC retrieves a DRPlacementControl by name and namespace.
func GetDRPC(ctx context.Context, reader client.Reader, name, namespace string) (*ramen.DRPlacementControl, error) {
	drpc := &ramen.DRPlacementControl{}
	err := reader.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, drpc)

	if err != nil {
		return nil, fmt.Errorf("failed to get DRPC %s/%s: %w", namespace, name, err)
	}

	return drpc, nil
}

// UpdateDRPCSpec updates the spec of a DRPC with retry on conflict.
func UpdateDRPCSpec(
	ctx context.Context,
	k8sClient client.Client,
	reader client.Reader,
	name, namespace string,
	updateFn func(*ramen.DRPlacementControl),
) (*ramen.DRPlacementControl, error) {
	var latestDRPC *ramen.DRPlacementControl

	retryErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		drpc, err := GetDRPC(ctx, reader, name, namespace)
		if err != nil {
			return err
		}

		updateFn(drpc)
		latestDRPC = drpc

		return k8sClient.Update(ctx, drpc)
	})

	if retryErr != nil {
		return nil, fmt.Errorf("failed to update DRPC %s/%s: %w", namespace, name, retryErr)
	}

	return latestDRPC, nil
}

// UpdateDRPCStatus updates the status of a DRPC with retry on conflict.
func UpdateDRPCStatus(
	ctx context.Context,
	k8sClient client.Client,
	reader client.Reader,
	name, namespace string,
	updateFn func(*ramen.DRPlacementControl),
) (*ramen.DRPlacementControl, error) {
	var latestDRPC *ramen.DRPlacementControl

	retryErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		drpc, err := GetDRPC(ctx, reader, name, namespace)
		if err != nil {
			return err
		}

		updateFn(drpc)
		latestDRPC = drpc

		return k8sClient.Status().Update(ctx, drpc)
	})

	if retryErr != nil {
		return nil, fmt.Errorf("failed to update DRPC status %s/%s: %w", namespace, name, retryErr)
	}

	return latestDRPC, nil
}

// DeleteDRPC deletes a DRPC by name and namespace.
func DeleteDRPC(ctx context.Context, k8sClient client.Client, name, namespace string) error {
	drpc := &ramen.DRPlacementControl{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
	}

	return client.IgnoreNotFound(k8sClient.Delete(ctx, drpc))
}

// WaitForDRPCPhase waits for a DRPC to reach a specific phase.
func WaitForDRPCPhase(
	ctx context.Context,
	reader client.Reader,
	name, namespace string,
	expectedPhase ramen.DRState,
	waitCond WaitForCondition,
) (*ramen.DRPlacementControl, error) {
	var drpc *ramen.DRPlacementControl

	err := wait.PollUntilContextTimeout(ctx, waitCond.Interval, waitCond.Timeout, true,
		func(ctx context.Context) (bool, error) {
			var err error
			drpc, err = GetDRPC(ctx, reader, name, namespace)

			if err != nil {
				if k8serrors.IsNotFound(err) {
					return false, nil
				}

				return false, err
			}

			return drpc.Status.Phase == expectedPhase, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for DRPC %s/%s phase %s: %w",
			namespace, name, expectedPhase, err)
	}

	return drpc, nil
}

// WaitForDRPCCondition waits for a DRPC to have a specific condition status.
func WaitForDRPCCondition(
	ctx context.Context,
	reader client.Reader,
	name, namespace string,
	conditionType string,
	expectedStatus metav1.ConditionStatus,
	waitCond WaitForCondition,
) (*ramen.DRPlacementControl, error) {
	var drpc *ramen.DRPlacementControl

	err := wait.PollUntilContextTimeout(ctx, waitCond.Interval, waitCond.Timeout, true,
		func(ctx context.Context) (bool, error) {
			var err error
			drpc, err = GetDRPC(ctx, reader, name, namespace)

			if err != nil {
				if k8serrors.IsNotFound(err) {
					return false, nil
				}

				return false, err
			}

			for _, cond := range drpc.Status.Conditions {
				if cond.Type == conditionType && cond.Status == expectedStatus {
					return true, nil
				}
			}

			return false, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for DRPC %s/%s condition %s=%s: %w",
			namespace, name, conditionType, expectedStatus, err)
	}

	return drpc, nil
}

// WaitForDRPCDeleted waits for a DRPC to be deleted.
func WaitForDRPCDeleted(
	ctx context.Context,
	reader client.Reader,
	name, namespace string,
	waitCond WaitForCondition,
) error {
	err := wait.PollUntilContextTimeout(ctx, waitCond.Interval, waitCond.Timeout, true,
		func(ctx context.Context) (bool, error) {
			_, err := GetDRPC(ctx, reader, name, namespace)
			if err != nil {
				if k8serrors.IsNotFound(err) {
					return true, nil
				}

				return false, err
			}

			return false, nil
		})

	if err != nil {
		return fmt.Errorf("timeout waiting for DRPC %s/%s to be deleted: %w", namespace, name, err)
	}

	return nil
}

// DRPCStatusBuilder helps build DRPC status for testing.
type DRPCStatusBuilder struct {
	phase              ramen.DRState
	observedGeneration int64
	preferredDecision  ramen.PlacementDecision
	conditions         []metav1.Condition
}

// NewDRPCStatusBuilder creates a new DRPCStatusBuilder.
func NewDRPCStatusBuilder() *DRPCStatusBuilder {
	return &DRPCStatusBuilder{
		phase: ramen.Deployed,
	}
}

// WithPhase sets the phase.
func (b *DRPCStatusBuilder) WithPhase(phase ramen.DRState) *DRPCStatusBuilder {
	b.phase = phase

	return b
}

// WithObservedGeneration sets the observed generation.
func (b *DRPCStatusBuilder) WithObservedGeneration(gen int64) *DRPCStatusBuilder {
	b.observedGeneration = gen

	return b
}

// WithPreferredDecision sets the preferred decision.
func (b *DRPCStatusBuilder) WithPreferredDecision(cluster string) *DRPCStatusBuilder {
	b.preferredDecision = ramen.PlacementDecision{
		ClusterName: cluster,
	}

	return b
}

// WithCondition adds a condition.
func (b *DRPCStatusBuilder) WithCondition(condType string, status metav1.ConditionStatus, reason, message string) *DRPCStatusBuilder {
	b.conditions = append(b.conditions, metav1.Condition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.Now(),
		Reason:             reason,
		Message:            message,
	})

	return b
}

// Build creates the DRPC status.
func (b *DRPCStatusBuilder) Build() ramen.DRPlacementControlStatus {
	return ramen.DRPlacementControlStatus{
		Phase:              b.phase,
		ObservedGeneration: b.observedGeneration,
		PreferredDecision:  b.preferredDecision,
		Conditions:         b.conditions,
	}
}

// ApplyStatusToDRPC applies the built status to a DRPC.
func (b *DRPCStatusBuilder) ApplyStatusToDRPC(drpc *ramen.DRPlacementControl) {
	drpc.Status = b.Build()
}
