// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package controllers_test

import (
	"context"
	"time"

	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/format"
	gomegatypes "github.com/onsi/gomega/types"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/internal/controller/testutils"
)

// objectGet retrieves an object from the API reader.
func objectGet(namespacedName types.NamespacedName, object client.Object) error {
	return testutils.ObjectGetError(context.TODO(), apiReader, namespacedName, object)
}

// objectDeletionTimestampRecentVerify verifies that an object has a recent deletion timestamp.
// Uses Ginkgo's Eventually for polling - this is test assertion code.
func objectDeletionTimestampRecentVerify(namespacedName types.NamespacedName, object client.Object) {
	Eventually(func() bool {
		err := objectGet(namespacedName, object)
		Expect(err).To(Succeed())

		return testutils.HasDeletionTimestamp(object)
	}).Should(BeTrue())

	Expect(testutils.IsDeletionTimestampRecent(object, 2*time.Second)).To(BeTrue(),
		"deletion timestamp not recent: %s", format.Object(object, 0))
}

// objectNotFoundErrorMatch returns a matcher for NotFound errors.
func objectNotFoundErrorMatch(groupResource schema.GroupResource, objectName string) gomegatypes.GomegaMatcher {
	return MatchError(k8serrors.NewNotFound(groupResource, objectName))
}

// objectAbsentVerify verifies that an object does not exist.
// Uses Ginkgo's Eventually for polling - this is test assertion code.
func objectAbsentVerify(namespacedName types.NamespacedName, object client.Object, groupResource schema.GroupResource) {
	Eventually(func() bool {
		absent, err := testutils.IsObjectAbsent(context.TODO(), apiReader, namespacedName, object)
		if err != nil {
			return false
		}

		return absent
	}).Should(BeTrue(), "object should be absent: %s", format.Object(object, 0))
}

// objectOrItsFinalizerAbsentVerify verifies that an object is absent or its finalizer is removed.
// Uses Ginkgo's Eventually for polling - this is test assertion code.
func objectOrItsFinalizerAbsentVerify(
	namespacedName types.NamespacedName,
	object client.Object,
	groupResource schema.GroupResource,
	finalizerName string,
) (objectAbsent bool) {
	Eventually(func() bool {
		absent, err := testutils.IsObjectAbsent(context.TODO(), apiReader, namespacedName, object)
		if err == nil && absent {
			objectAbsent = true

			return true
		}

		if err := objectGet(namespacedName, object); err != nil {
			Expect(err).To(objectNotFoundErrorMatch(groupResource, namespacedName.Name),
				format.Object(object, 0))
			objectAbsent = true

			return true
		}

		return !testutils.HasFinalizer(object, finalizerName)
	}).Should(BeTrue())

	return objectAbsent
}

// conditionExpect asserts that a condition exists and returns it.
func conditionExpect(conditions []metav1.Condition, typ string) *metav1.Condition {
	condition, err := testutils.ValidateConditionExists(conditions, typ)
	Expect(err).ToNot(HaveOccurred(), "condition %s should exist", typ)

	return condition
}
