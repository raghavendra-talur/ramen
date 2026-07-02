// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/ramendr/ramen/simtest/invariants"
	"github.com/ramendr/ramen/simtest/world"
)

var (
	worldOnce sync.Once
	sharedW   *world.World
	sharedC   *invariants.Checker
)

// getWorld lazily builds one world per test binary. The first caller's t owns
// cleanup, so every test funcs through the same top-level Test* functions in
// this package, which the go test runner executes sequentially per package —
// subtests inside them may parallelize.
func getWorld(t *testing.T) (*world.World, *invariants.Checker) {
	t.Helper()

	worldOnce.Do(func() {
		if _, err := os.Stat(world.ManagerBin()); err != nil {
			t.Skipf("bin/manager not built: %v", err)
		}
		sharedW = world.New(t)

		var err error
		sharedC, err = invariants.StartChecker(context.Background(), sharedW,
			filepath.Join(sharedW.Dir, "progression-edges.log"))
		if err != nil {
			t.Fatalf("start invariant checker: %v", err)
		}
	})

	if sharedW == nil {
		t.Skip("world unavailable")
	}

	return sharedW, sharedC
}
