// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world_test

import (
	"context"
	"os"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ramendr/ramen/simtest/world"
)

func TestWorldBringUp(t *testing.T) {
	if _, err := os.Stat(world.ManagerBin()); err != nil {
		t.Skipf("bin/manager not built, run 'make -C .. build': %v", err)
	}

	w := world.New(t)
	ctx := context.Background()

	// The hub operator must validate the DRClusters and then the DRPolicy —
	// this exercises: config load, S3 (validate profile), bootstrap MW apply
	// (work agent), DRClusterConfig MW + reconcile (dr-cluster operator), MCV
	// (view agent), ManagedCluster claims.
	deadline := time.Now().Add(3 * time.Minute)
	for {
		policy := &rmn.DRPolicy{}
		if err := w.Hub.Client.Get(ctx, types.NamespacedName{Name: world.DRPolicyName}, policy); err == nil {
			if meta.IsStatusConditionTrue(policy.Status.Conditions, rmn.DRPolicyValidated) {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("DRPolicy never became Validated; check .artifacts logs")
		}
		time.Sleep(500 * time.Millisecond)
	}
}
