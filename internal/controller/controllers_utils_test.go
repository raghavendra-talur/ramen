// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package controllers_test

import (
	"context"
	"fmt"

	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	gomegaTypes "github.com/onsi/gomega/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ocmv1 "open-cluster-management.io/api/cluster/v1"
	workv1 "open-cluster-management.io/api/work/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
	controllers "github.com/ramendr/ramen/internal/controller"
	"github.com/ramendr/ramen/internal/controller/testutils"
	"github.com/ramendr/ramen/internal/controller/util"
)

// ensureManagedCluster creates a ManagedCluster with status using testutils.
// This is a test helper that wraps testutils and uses Expect().
func ensureManagedCluster(k8sClient client.Client, cluster string) {
	_, err := testutils.CreateManagedClusterWithStatus(
		context.TODO(),
		k8sClient,
		cluster,
		testutils.DefaultManagedClusterOptions(),
	)
	Expect(err).NotTo(HaveOccurred(), "failed to create ManagedCluster %s", cluster)
}

// createManagedCluster creates a ManagedCluster without status update.
func createManagedCluster(k8sClient client.Client, cluster string) *ocmv1.ManagedCluster {
	mc, err := testutils.CreateManagedCluster(
		context.TODO(),
		k8sClient,
		cluster,
		testutils.DefaultManagedClusterOptions(),
	)
	Expect(err).NotTo(HaveOccurred(), "failed to create ManagedCluster %s", cluster)

	return mc
}

// updateManagedClusterStatus updates the status of a ManagedCluster to indicate it's joined.
func updateManagedClusterStatus(k8sClient client.Client, mc *ocmv1.ManagedCluster) {
	err := testutils.UpdateManagedClusterStatus(
		context.TODO(),
		k8sClient,
		mc,
		testutils.DefaultManagedClusterOptions().ClusterID,
	)
	Expect(err).NotTo(HaveOccurred(), "failed to update ManagedCluster status %s", mc.Name)
}

// getLatestDRCluster retrieves the latest DRCluster.
func getLatestDRCluster(cluster string) *ramen.DRCluster {
	drcluster, err := testutils.GetDRCluster(context.TODO(), apiReader, cluster)
	Expect(err).NotTo(HaveOccurred(), "failed to get DRCluster %s", cluster)

	return drcluster
}

// updateDRClusterParameters updates DRCluster spec fields.
func updateDRClusterParameters(drc *ramen.DRCluster) *ramen.DRCluster {
	latestDRC, err := testutils.UpdateDRClusterSpec(
		context.TODO(),
		k8sClient,
		apiReader,
		drc.Name,
		func(latestdrc *ramen.DRCluster) {
			latestdrc.Spec.ClusterFence = drc.Spec.ClusterFence
			latestdrc.Spec.S3ProfileName = drc.Spec.S3ProfileName
			latestdrc.Spec.CIDRs = drc.Spec.CIDRs
		},
	)
	Expect(err).NotTo(HaveOccurred(), "failed to update DRCluster %s", drc.Name)

	return latestDRC
}

// updateDRClusterManifestWorkStatus updates the DRCluster ManifestWork status.
func updateDRClusterManifestWorkStatus(k8sClient client.Client, apiReader client.Reader, clusterNamespace string) {
	// First wait for the ManifestWork to exist
	key := types.NamespacedName{
		Name:      util.DrClusterManifestWorkName,
		Namespace: clusterNamespace,
	}

	Eventually(func() error {
		mw := &workv1.ManifestWork{}

		return apiReader.Get(context.TODO(), key, mw)
	}, timeout, interval).Should(Succeed(),
		"failed to get ManifestWork %s for DRCluster %s", key.Name, key.Namespace)

	// Then update the status
	err := testutils.UpdateDRClusterManifestWorkStatus(context.TODO(), k8sClient, apiReader, clusterNamespace)
	Expect(err).NotTo(HaveOccurred(), "failed to update DRCluster ManifestWork status for %s", clusterNamespace)
}

// updateDRClusterConfigMWStatus updates the DRClusterConfig ManifestWork status.
func updateDRClusterConfigMWStatus(k8sClient client.Client, apiReader client.Reader, clusterNamespace string) {
	key := types.NamespacedName{
		Name:      fmt.Sprintf(util.ManifestWorkNameTypeFormat, util.MWTypeDRCConfig),
		Namespace: clusterNamespace,
	}

	Eventually(func() error {
		mw := &workv1.ManifestWork{}

		return apiReader.Get(context.TODO(), key, mw)
	}, timeout, interval).Should(Succeed(),
		"failed to get ManifestWork %s for DRClusterConfig %s", key.Name, key.Namespace)

	err := testutils.UpdateDRClusterConfigMWStatus(context.TODO(), k8sClient, apiReader, clusterNamespace)
	Expect(err).NotTo(HaveOccurred(), "failed to update DRClusterConfig ManifestWork status for %s", clusterNamespace)
}

// objectConditionExpectEventually waits for an object to have the expected condition.
// This function uses Ginkgo's Eventually for polling, which is appropriate for test assertions.
func objectConditionExpectEventually(
	apiReader client.Reader,
	obj client.Object,
	status metav1.ConditionStatus,
	reasonMatcher,
	messageMatcher gomegaTypes.GomegaMatcher,
	conditionType string,
	disabled ...bool,
) {
	switch objActual := obj.(type) {
	case *ramen.DRCluster:
		drclusterConditionExpect(
			apiReader,
			objActual,
			disabled[0],
			status,
			reasonMatcher,
			messageMatcher,
			conditionType,
			false,
		)
	case *ramen.DRClusterConfig:
		drclusterConfigConditionExpect(
			apiReader,
			objActual,
			status,
			reasonMatcher,
			messageMatcher,
			conditionType,
			false,
		)
	default:
		return
	}
}

// drclusterConfigConditionExpect asserts a DRClusterConfig has the expected condition.
// Uses Ginkgo's Eventually/Consistently for polling - this is test assertion code.
func drclusterConfigConditionExpect(
	apiReader client.Reader,
	drclusterConfig *ramen.DRClusterConfig,
	status metav1.ConditionStatus,
	reasonMatcher,
	messageMatcher gomegaTypes.GomegaMatcher,
	conditionType string,
	always bool,
) {
	testFunc := func(g Gomega) []metav1.Condition {
		err := apiReader.Get(context.TODO(), types.NamespacedName{
			Namespace: drclusterConfig.Namespace,
			Name:      drclusterConfig.Name,
		}, drclusterConfig)
		g.Expect(err).NotTo(HaveOccurred())

		return drclusterConfig.Status.Conditions
	}

	matchElements := MatchElements(
		func(element interface{}) string {
			cond, ok := element.(metav1.Condition)
			if !ok {
				return ""
			}

			return cond.Type
		},
		IgnoreExtras,
		Elements{
			conditionType: MatchAllFields(Fields{
				`Type`:               Ignore(),
				`Status`:             Equal(status),
				`ObservedGeneration`: Equal(drclusterConfig.Generation),
				`LastTransitionTime`: Ignore(),
				`Reason`:             reasonMatcher,
				`Message`:            messageMatcher,
			}),
		},
	)

	if always {
		Consistently(testFunc, timeout, interval).Should(matchElements)
	} else {
		Eventually(testFunc, timeout, interval).Should(matchElements)
	}
}

// drclusterConditionExpectConsistently asserts a DRCluster consistently has the expected condition.
func drclusterConditionExpectConsistently(
	apiReader client.Reader,
	drcluster *ramen.DRCluster,
	disabled bool,
	reasonMatcher,
	messageMatcher gomegaTypes.GomegaMatcher,
) {
	drclusterConditionExpect(
		apiReader,
		drcluster,
		disabled,
		metav1.ConditionTrue,
		reasonMatcher,
		messageMatcher,
		ramen.DRClusterValidated,
		true,
	)
}

// drclusterConditionExpect asserts a DRCluster has the expected condition.
// Uses Ginkgo's Eventually/Consistently for polling - this is test assertion code.
func drclusterConditionExpect(
	apiReader client.Reader,
	drcluster *ramen.DRCluster,
	disabled bool,
	status metav1.ConditionStatus,
	reasonMatcher,
	messageMatcher gomegaTypes.GomegaMatcher,
	conditionType string,
	always bool,
) {
	testFunc := func(g Gomega) []metav1.Condition {
		err := apiReader.Get(context.TODO(), types.NamespacedName{
			Namespace: drcluster.Namespace,
			Name:      drcluster.Name,
		}, drcluster)
		g.Expect(err).NotTo(HaveOccurred())

		return drcluster.Status.Conditions
	}

	matchElements := MatchElements(
		func(element interface{}) string {
			cond, ok := element.(metav1.Condition)
			if !ok {
				return ""
			}

			return cond.Type
		},
		IgnoreExtras,
		Elements{
			conditionType: MatchAllFields(Fields{
				`Type`:               Ignore(),
				`Status`:             Equal(status),
				`ObservedGeneration`: Equal(drcluster.Generation),
				`LastTransitionTime`: Ignore(),
				`Reason`:             reasonMatcher,
				`Message`:            messageMatcher,
			}),
		},
	)

	if always {
		Consistently(testFunc, timeout, interval).Should(matchElements)
	} else {
		Eventually(testFunc, timeout, interval).Should(matchElements)
	}

	// Skip manifest validation if condition is false
	if status == metav1.ConditionFalse {
		return
	}

	validateClusterManifest(apiReader, drcluster, disabled)
}

// validateClusterManifest validates the DRCluster ManifestWork.
// Uses Ginkgo's Eventually for polling - this is test assertion code.
func validateClusterManifest(apiReader client.Reader, drcluster *ramen.DRCluster, disabled bool) {
	expectedCount := 8
	if disabled {
		expectedCount = 3
	}

	clusterName := drcluster.Name

	key := types.NamespacedName{
		Name:      util.DrClusterManifestWorkName,
		Namespace: clusterName,
	}

	manifestWork := &workv1.ManifestWork{}

	Eventually(
		func(g Gomega) []workv1.Manifest {
			g.Expect(apiReader.Get(context.TODO(), key, manifestWork)).To(Succeed())

			return manifestWork.Spec.Workload.Manifests
		}, timeout, interval,
	).Should(HaveLen(expectedCount))

	Expect(manifestWork.GetAnnotations()[controllers.DRClusterNameAnnotation]).Should(Equal(clusterName))
}

// verifyDRClusterConfigMW verifies the DRClusterConfig ManifestWork.
// Uses Ginkgo's Eventually/Consistently for polling - this is test assertion code.
//
//nolint:unparam
func verifyDRClusterConfigMW(
	k8sClient client.Client,
	managedCluster, clusterID string,
	schedules []string,
	always bool,
) {
	testFunc := func() error {
		mw := &workv1.ManifestWork{}

		err := k8sClient.Get(
			context.TODO(),
			types.NamespacedName{
				Name:      fmt.Sprintf(util.ManifestWorkNameTypeFormat, util.MWTypeDRCConfig),
				Namespace: managedCluster,
			},
			mw,
		)
		if err != nil {
			return err
		}

		drcConfig, err := util.ExtractDRCConfigFromManifestWork(mw)
		if err != nil {
			return fmt.Errorf("error extracting ManifestWork from %v", mw)
		}

		if drcConfig.Spec.ClusterID != clusterID {
			return fmt.Errorf("clusterID mismatch, expected %s got %s", clusterID, drcConfig.Spec.ClusterID)
		}

		// Use testutils for string slice comparison
		return testutils.EqualStringSlices(schedules, drcConfig.Spec.ReplicationSchedules)
	}

	if always {
		Consistently(testFunc, timeout, interval).Should(Succeed())
	} else {
		Eventually(testFunc, timeout, interval).Should(Succeed())
	}
}

// ensureDRClusterConfigMWNotFound asserts the DRClusterConfig ManifestWork does not exist.
// Uses Ginkgo's Eventually/Consistently for polling - this is test assertion code.
func ensureDRClusterConfigMWNotFound(k8sClient client.Client, managedCluster string, always bool) {
	testFunc := func() bool {
		mw := &workv1.ManifestWork{}

		err := k8sClient.Get(
			context.TODO(),
			types.NamespacedName{
				Name:      fmt.Sprintf(util.ManifestWorkNameTypeFormat, util.MWTypeDRCConfig),
				Namespace: managedCluster,
			},
			mw,
		)

		return k8serrors.IsNotFound(err)
	}

	if always {
		Consistently(testFunc, timeout, interval).Should(BeTrue())
	} else {
		Eventually(testFunc, timeout, interval).Should(BeTrue())
	}
}
