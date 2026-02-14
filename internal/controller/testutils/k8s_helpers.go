// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	ocmv1 "open-cluster-management.io/api/cluster/v1"
	workv1 "open-cluster-management.io/api/work/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
	"github.com/ramendr/ramen/internal/controller/util"
)

// CreateNamespace creates a namespace with the given name.
// Returns an error if creation fails.
func CreateNamespace(ctx context.Context, k8sClient client.Client, name string) error {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}

	return k8sClient.Create(ctx, ns)
}

// CreateNamespaceIfNotExists creates a namespace if it doesn't already exist.
func CreateNamespaceIfNotExists(ctx context.Context, k8sClient client.Client, name string) error {
	ns := &corev1.Namespace{}
	err := k8sClient.Get(ctx, types.NamespacedName{Name: name}, ns)

	if err != nil {
		if k8serrors.IsNotFound(err) {
			return CreateNamespace(ctx, k8sClient, name)
		}

		return err
	}

	return nil
}

// DeleteNamespace deletes a namespace by name.
func DeleteNamespace(ctx context.Context, k8sClient client.Client, name string) error {
	ns := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}

	return client.IgnoreNotFound(k8sClient.Delete(ctx, ns))
}

// ManagedClusterOptions contains options for creating a ManagedCluster.
type ManagedClusterOptions struct {
	HubAcceptsClient bool
	ClusterID        string
}

// DefaultManagedClusterOptions returns default options for creating a ManagedCluster.
func DefaultManagedClusterOptions() ManagedClusterOptions {
	return ManagedClusterOptions{
		HubAcceptsClient: true,
		ClusterID:        "fake",
	}
}

// CreateManagedCluster creates a ManagedCluster with the given name.
func CreateManagedCluster(
	ctx context.Context,
	k8sClient client.Client,
	name string,
	opts ManagedClusterOptions,
) (*ocmv1.ManagedCluster, error) {
	mc := &ocmv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       ocmv1.ManagedClusterSpec{HubAcceptsClient: opts.HubAcceptsClient},
	}

	if err := k8sClient.Create(ctx, mc); err != nil {
		return nil, fmt.Errorf("failed to create ManagedCluster %s: %w", name, err)
	}

	return mc, nil
}

// UpdateManagedClusterStatus updates the status of a ManagedCluster to indicate it's joined.
func UpdateManagedClusterStatus(
	ctx context.Context,
	k8sClient client.Client,
	mc *ocmv1.ManagedCluster,
	clusterID string,
) error {
	mc.Status = ocmv1.ManagedClusterStatus{
		Conditions: []metav1.Condition{
			{
				Type:               ocmv1.ManagedClusterConditionJoined,
				LastTransitionTime: metav1.Time{Time: time.Now()},
				Status:             metav1.ConditionTrue,
				Reason:             ocmv1.ManagedClusterConditionJoined,
				Message:            "Faked status",
			},
		},
		ClusterClaims: []ocmv1.ManagedClusterClaim{
			{Name: "id.k8s.io", Value: clusterID},
		},
	}

	return k8sClient.Status().Update(ctx, mc)
}

// CreateManagedClusterWithStatus creates a ManagedCluster and updates its status.
func CreateManagedClusterWithStatus(
	ctx context.Context,
	k8sClient client.Client,
	name string,
	opts ManagedClusterOptions,
) (*ocmv1.ManagedCluster, error) {
	mc, err := CreateManagedCluster(ctx, k8sClient, name, opts)
	if err != nil {
		return nil, err
	}

	if err := UpdateManagedClusterStatus(ctx, k8sClient, mc, opts.ClusterID); err != nil {
		return nil, fmt.Errorf("failed to update ManagedCluster status: %w", err)
	}

	return mc, nil
}

// DeleteManagedCluster deletes a ManagedCluster by name.
func DeleteManagedCluster(ctx context.Context, k8sClient client.Client, name string) error {
	mc := &ocmv1.ManagedCluster{
		ObjectMeta: metav1.ObjectMeta{Name: name},
	}

	return client.IgnoreNotFound(k8sClient.Delete(ctx, mc))
}

// GetDRCluster retrieves a DRCluster by name.
func GetDRCluster(ctx context.Context, reader client.Reader, name string) (*ramen.DRCluster, error) {
	drcluster := &ramen.DRCluster{}
	err := reader.Get(ctx, types.NamespacedName{Name: name}, drcluster)

	if err != nil {
		return nil, fmt.Errorf("failed to get DRCluster %s: %w", name, err)
	}

	return drcluster, nil
}

// UpdateDRClusterSpec updates the spec of a DRCluster with retry on conflict.
func UpdateDRClusterSpec(
	ctx context.Context,
	k8sClient client.Client,
	reader client.Reader,
	name string,
	updateFn func(*ramen.DRCluster),
) (*ramen.DRCluster, error) {
	var latestDRC *ramen.DRCluster

	retryErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		drc, err := GetDRCluster(ctx, reader, name)
		if err != nil {
			return err
		}

		updateFn(drc)
		latestDRC = drc

		return k8sClient.Update(ctx, drc)
	})

	if retryErr != nil {
		return nil, fmt.Errorf("failed to update DRCluster %s: %w", name, retryErr)
	}

	return latestDRC, nil
}

// ManifestWorkStatusOptions contains options for updating ManifestWork status.
type ManifestWorkStatusOptions struct {
	Available bool
	Applied   bool
}

// DefaultManifestWorkStatusOptions returns default options (both Available and Applied true).
func DefaultManifestWorkStatusOptions() ManifestWorkStatusOptions {
	return ManifestWorkStatusOptions{
		Available: true,
		Applied:   true,
	}
}

// UpdateManifestWorkStatus updates the status of a ManifestWork to indicate it's applied.
func UpdateManifestWorkStatus(
	ctx context.Context,
	k8sClient client.Client,
	reader client.Reader,
	key types.NamespacedName,
	opts ManifestWorkStatusOptions,
) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		mw := &workv1.ManifestWork{}
		if err := reader.Get(ctx, key, mw); err != nil {
			return err
		}

		now := metav1.Time{Time: time.Now()}
		conditions := []metav1.Condition{}

		if opts.Available {
			conditions = append(conditions, metav1.Condition{
				Type:               workv1.WorkAvailable,
				LastTransitionTime: now,
				Status:             metav1.ConditionTrue,
				Reason:             "ResourceAvailable",
				Message:            "All resources are available",
			})
		}

		if opts.Applied {
			conditions = append(conditions, metav1.Condition{
				Type:               workv1.WorkApplied,
				LastTransitionTime: now,
				Status:             metav1.ConditionTrue,
				Reason:             "AppliedManifestworkComplete",
				Message:            "Apply Manifest Work Complete",
			})
		}

		mw.Status = workv1.ManifestWorkStatus{Conditions: conditions}

		return k8sClient.Status().Update(ctx, mw)
	})
}

// UpdateDRClusterManifestWorkStatus updates the DRCluster ManifestWork status for a cluster.
func UpdateDRClusterManifestWorkStatus(
	ctx context.Context,
	k8sClient client.Client,
	reader client.Reader,
	clusterNamespace string,
) error {
	key := types.NamespacedName{
		Name:      util.DrClusterManifestWorkName,
		Namespace: clusterNamespace,
	}

	return UpdateManifestWorkStatus(ctx, k8sClient, reader, key, DefaultManifestWorkStatusOptions())
}

// UpdateDRClusterConfigMWStatus updates the DRClusterConfig ManifestWork status.
func UpdateDRClusterConfigMWStatus(
	ctx context.Context,
	k8sClient client.Client,
	reader client.Reader,
	clusterNamespace string,
) error {
	key := types.NamespacedName{
		Name:      fmt.Sprintf(util.ManifestWorkNameTypeFormat, util.MWTypeDRCConfig),
		Namespace: clusterNamespace,
	}

	return UpdateManifestWorkStatus(ctx, k8sClient, reader, key, DefaultManifestWorkStatusOptions())
}

// WaitForCondition waits for a condition to be met with polling.
type WaitForCondition struct {
	Timeout  time.Duration
	Interval time.Duration
}

// DefaultWaitCondition returns default wait conditions.
func DefaultWaitCondition() WaitForCondition {
	return WaitForCondition{
		Timeout:  time.Second * 10,
		Interval: time.Millisecond * 100,
	}
}

// WaitForManifestWork waits for a ManifestWork to exist.
func WaitForManifestWork(
	ctx context.Context,
	reader client.Reader,
	key types.NamespacedName,
	waitCond WaitForCondition,
) (*workv1.ManifestWork, error) {
	var mw *workv1.ManifestWork

	err := wait.PollUntilContextTimeout(ctx, waitCond.Interval, waitCond.Timeout, true,
		func(ctx context.Context) (bool, error) {
			mw = &workv1.ManifestWork{}
			if err := reader.Get(ctx, key, mw); err != nil {
				if k8serrors.IsNotFound(err) {
					return false, nil
				}

				return false, err
			}

			return true, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for ManifestWork %s/%s: %w", key.Namespace, key.Name, err)
	}

	return mw, nil
}

// WaitForDRClusterCondition waits for a DRCluster to have a specific condition status.
func WaitForDRClusterCondition(
	ctx context.Context,
	reader client.Reader,
	name string,
	conditionType string,
	expectedStatus metav1.ConditionStatus,
	waitCond WaitForCondition,
) (*ramen.DRCluster, error) {
	var drcluster *ramen.DRCluster

	err := wait.PollUntilContextTimeout(ctx, waitCond.Interval, waitCond.Timeout, true,
		func(ctx context.Context) (bool, error) {
			var err error
			drcluster, err = GetDRCluster(ctx, reader, name)

			if err != nil {
				return false, err
			}

			for _, cond := range drcluster.Status.Conditions {
				if cond.Type == conditionType && cond.Status == expectedStatus {
					return true, nil
				}
			}

			return false, nil
		})

	if err != nil {
		return nil, fmt.Errorf("timeout waiting for DRCluster %s condition %s=%s: %w",
			name, conditionType, expectedStatus, err)
	}

	return drcluster, nil
}

// EqualStringSlices checks if two string slices contain the same elements (order independent).
func EqualStringSlices(desired, actual []string) error {
	d := make(map[string]bool)
	for _, value := range desired {
		d[value] = false
	}

	for _, value := range actual {
		if found, ok := d[value]; !ok || found {
			return fmt.Errorf("mismatch: desired %v, actual %v", desired, actual)
		}

		d[value] = true
	}

	for _, value := range d {
		if !value {
			return fmt.Errorf("mismatch: desired %v, actual %v", desired, actual)
		}
	}

	return nil
}
