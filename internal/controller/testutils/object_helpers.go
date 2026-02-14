// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"context"
	"fmt"
	"time"

	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ObjectGetError retrieves an object and returns any error.
func ObjectGetError(ctx context.Context, reader client.Reader, key types.NamespacedName, obj client.Object) error {
	return reader.Get(ctx, key, obj)
}

// IsObjectAbsent checks if an object does not exist (returns true if NotFound).
func IsObjectAbsent(ctx context.Context, reader client.Reader, key types.NamespacedName, obj client.Object) (bool, error) {
	err := reader.Get(ctx, key, obj)
	if err != nil {
		if k8serrors.IsNotFound(err) {
			return true, nil
		}

		return false, err
	}

	return false, nil
}

// IsNotFoundError checks if an error is a NotFound error for the expected resource.
func IsNotFoundError(err error, groupResource schema.GroupResource, name string) bool {
	expectedErr := k8serrors.NewNotFound(groupResource, name)

	return k8serrors.IsNotFound(err) && err.Error() == expectedErr.Error()
}

// HasDeletionTimestamp checks if an object has a deletion timestamp set.
func HasDeletionTimestamp(obj client.Object) bool {
	return obj.GetDeletionTimestamp() != nil
}

// IsDeletionTimestampRecent checks if the deletion timestamp is within the specified duration of now.
func IsDeletionTimestampRecent(obj client.Object, within time.Duration) bool {
	ts := obj.GetDeletionTimestamp()
	if ts == nil {
		return false
	}

	return time.Since(ts.Time) <= within
}

// HasFinalizer checks if an object has a specific finalizer.
func HasFinalizer(obj client.Object, finalizerName string) bool {
	for _, f := range obj.GetFinalizers() {
		if f == finalizerName {
			return true
		}
	}

	return false
}

// FindCondition finds a condition by type using the meta package.
func FindConditionByType(conditions []metav1.Condition, conditionType string) *metav1.Condition {
	return meta.FindStatusCondition(conditions, conditionType)
}

// WaitForObjectAbsent waits for an object to be deleted.
func WaitForObjectAbsent(
	ctx context.Context,
	reader client.Reader,
	key types.NamespacedName,
	obj client.Object,
	opts WaitOptions,
) error {
	return wait.PollUntilContextTimeout(ctx, opts.Interval, opts.Timeout, true,
		func(ctx context.Context) (bool, error) {
			absent, err := IsObjectAbsent(ctx, reader, key, obj)
			if err != nil {
				return false, err
			}

			return absent, nil
		})
}

// WaitForDeletionTimestamp waits for an object to have a deletion timestamp.
func WaitForDeletionTimestamp(
	ctx context.Context,
	reader client.Reader,
	key types.NamespacedName,
	obj client.Object,
	opts WaitOptions,
) error {
	return wait.PollUntilContextTimeout(ctx, opts.Interval, opts.Timeout, true,
		func(ctx context.Context) (bool, error) {
			if err := reader.Get(ctx, key, obj); err != nil {
				return false, err
			}

			return HasDeletionTimestamp(obj), nil
		})
}

// WaitForFinalizerAbsent waits for a specific finalizer to be removed from an object.
// Returns true if the object itself was deleted, false if just the finalizer was removed.
func WaitForFinalizerAbsent(
	ctx context.Context,
	reader client.Reader,
	key types.NamespacedName,
	obj client.Object,
	finalizerName string,
	groupResource schema.GroupResource,
	opts WaitOptions,
) (objectDeleted bool, err error) {
	err = wait.PollUntilContextTimeout(ctx, opts.Interval, opts.Timeout, true,
		func(ctx context.Context) (bool, error) {
			getErr := reader.Get(ctx, key, obj)
			if getErr != nil {
				if IsNotFoundError(getErr, groupResource, key.Name) {
					objectDeleted = true

					return true, nil
				}

				return false, getErr
			}

			return !HasFinalizer(obj, finalizerName), nil
		})

	return objectDeleted, err
}

// ValidateConditionExists checks that a condition of the given type exists and returns it.
func ValidateConditionExists(conditions []metav1.Condition, conditionType string) (*metav1.Condition, error) {
	cond := FindConditionByType(conditions, conditionType)
	if cond == nil {
		return nil, fmt.Errorf("condition %s not found", conditionType)
	}

	return cond, nil
}
