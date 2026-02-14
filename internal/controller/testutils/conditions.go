// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"context"
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

// ConditionMatcher defines how to match a condition.
type ConditionMatcher struct {
	Type    string
	Status  metav1.ConditionStatus
	Reason  string
	Message string
}

// MatchCondition checks if the given condition matches the expected values.
// Empty strings in the matcher are treated as wildcards.
func MatchCondition(cond metav1.Condition, matcher ConditionMatcher) error {
	if matcher.Type != "" && cond.Type != matcher.Type {
		return fmt.Errorf("condition type mismatch: expected %s, got %s", matcher.Type, cond.Type)
	}

	if matcher.Status != "" && cond.Status != matcher.Status {
		return fmt.Errorf("condition status mismatch: expected %s, got %s", matcher.Status, cond.Status)
	}

	if matcher.Reason != "" && cond.Reason != matcher.Reason {
		return fmt.Errorf("condition reason mismatch: expected %s, got %s", matcher.Reason, cond.Reason)
	}

	if matcher.Message != "" && cond.Message != matcher.Message {
		return fmt.Errorf("condition message mismatch: expected %s, got %s", matcher.Message, cond.Message)
	}

	return nil
}

// FindCondition finds a condition by type in a list of conditions.
func FindCondition(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == conditionType {
			return &conditions[i]
		}
	}

	return nil
}

// CheckDRClusterCondition checks if a DRCluster has the expected condition.
func CheckDRClusterCondition(drcluster *ramen.DRCluster, matcher ConditionMatcher) error {
	cond := FindCondition(drcluster.Status.Conditions, matcher.Type)
	if cond == nil {
		return fmt.Errorf("condition %s not found on DRCluster %s", matcher.Type, drcluster.Name)
	}

	return MatchCondition(*cond, matcher)
}

// CheckDRClusterConfigCondition checks if a DRClusterConfig has the expected condition.
func CheckDRClusterConfigCondition(drcc *ramen.DRClusterConfig, matcher ConditionMatcher) error {
	cond := FindCondition(drcc.Status.Conditions, matcher.Type)
	if cond == nil {
		return fmt.Errorf("condition %s not found on DRClusterConfig %s", matcher.Type, drcc.Name)
	}

	return MatchCondition(*cond, matcher)
}

// CheckDRPolicyCondition checks if a DRPolicy has the expected condition.
func CheckDRPolicyCondition(drpolicy *ramen.DRPolicy, matcher ConditionMatcher) error {
	cond := FindCondition(drpolicy.Status.Conditions, matcher.Type)
	if cond == nil {
		return fmt.Errorf("condition %s not found on DRPolicy %s", matcher.Type, drpolicy.Name)
	}

	return MatchCondition(*cond, matcher)
}

// WaitOptions configures wait behavior for polling functions.
type WaitOptions struct {
	Timeout  time.Duration
	Interval time.Duration
}

// DefaultWaitOptions returns default wait options.
func DefaultWaitOptions() WaitOptions {
	return WaitOptions{
		Timeout:  10 * time.Second,
		Interval: 100 * time.Millisecond,
	}
}

// WaitForDRClusterCondition waits for a DRCluster to have the specified condition.
func WaitForDRClusterConditionMatch(
	ctx context.Context,
	reader client.Reader,
	name string,
	matcher ConditionMatcher,
	opts WaitOptions,
) (*ramen.DRCluster, error) {
	var drcluster *ramen.DRCluster

	err := wait.PollUntilContextTimeout(ctx, opts.Interval, opts.Timeout, true,
		func(ctx context.Context) (bool, error) {
			drcluster = &ramen.DRCluster{}
			if err := reader.Get(ctx, types.NamespacedName{Name: name}, drcluster); err != nil {
				return false, err
			}

			if err := CheckDRClusterCondition(drcluster, matcher); err != nil {
				return false, nil // Keep waiting
			}

			return true, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for DRCluster %s condition %s: %w", name, matcher.Type, err)
	}

	return drcluster, nil
}

// WaitForDRClusterConfigCondition waits for a DRClusterConfig to have the specified condition.
func WaitForDRClusterConfigConditionMatch(
	ctx context.Context,
	reader client.Reader,
	name, namespace string,
	matcher ConditionMatcher,
	opts WaitOptions,
) (*ramen.DRClusterConfig, error) {
	var drcc *ramen.DRClusterConfig

	err := wait.PollUntilContextTimeout(ctx, opts.Interval, opts.Timeout, true,
		func(ctx context.Context) (bool, error) {
			drcc = &ramen.DRClusterConfig{}
			if err := reader.Get(ctx, types.NamespacedName{Name: name, Namespace: namespace}, drcc); err != nil {
				return false, err
			}

			if err := CheckDRClusterConfigCondition(drcc, matcher); err != nil {
				return false, nil // Keep waiting
			}

			return true, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for DRClusterConfig %s/%s condition %s: %w",
			namespace, name, matcher.Type, err)
	}

	return drcc, nil
}

// WaitForDRPolicyCondition waits for a DRPolicy to have the specified condition.
func WaitForDRPolicyConditionMatch(
	ctx context.Context,
	reader client.Reader,
	name string,
	matcher ConditionMatcher,
	opts WaitOptions,
) (*ramen.DRPolicy, error) {
	var drpolicy *ramen.DRPolicy

	err := wait.PollUntilContextTimeout(ctx, opts.Interval, opts.Timeout, true,
		func(ctx context.Context) (bool, error) {
			drpolicy = &ramen.DRPolicy{}
			if err := reader.Get(ctx, types.NamespacedName{Name: name}, drpolicy); err != nil {
				return false, err
			}

			if err := CheckDRPolicyCondition(drpolicy, matcher); err != nil {
				return false, nil // Keep waiting
			}

			return true, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for DRPolicy %s condition %s: %w", name, matcher.Type, err)
	}

	return drpolicy, nil
}

// WaitForObjectDeletion waits for an object to be deleted.
func WaitForObjectDeletion(
	ctx context.Context,
	reader client.Reader,
	obj client.Object,
	opts WaitOptions,
) error {
	key := client.ObjectKeyFromObject(obj)

	err := wait.PollUntilContextTimeout(ctx, opts.Interval, opts.Timeout, true,
		func(ctx context.Context) (bool, error) {
			err := reader.Get(ctx, key, obj)
			if err != nil {
				if client.IgnoreNotFound(err) == nil {
					return true, nil // Object deleted
				}

				return false, err
			}

			return false, nil // Object still exists
		})

	if err != nil {
		return fmt.Errorf("timeout waiting for object %s/%s deletion: %w", key.Namespace, key.Name, err)
	}

	return nil
}
