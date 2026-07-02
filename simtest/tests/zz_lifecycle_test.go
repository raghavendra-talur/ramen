// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"testing"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ramendr/ramen/simtest/world"
)

// TestSharedWorldSurvivesSiblingTests is a regression test for the shared
// world lifecycle: it is a separate top-level test from TestBaselines (go
// test runs top-level tests in a package in source-file order, and this
// file is named to sort after baseline_test.go), so it only passes if the
// world getWorld returned to TestBaselines is still alive here — i.e.
// teardown is owned by TestMain and not by TestBaselines' t.Cleanup.
func TestSharedWorldSurvivesSiblingTests(t *testing.T) {
	w, _ := getWorld(t)
	ctx := context.Background()

	policy := &rmn.DRPolicy{}
	if err := w.Hub.Client.Get(ctx, types.NamespacedName{Name: world.DRPolicyName}, policy); err != nil {
		t.Fatalf("shared world unavailable after sibling test(s) ran: %v", err)
	}
}
