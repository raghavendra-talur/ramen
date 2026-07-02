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

// getWorld lazily builds one world for the whole test binary via
// world.SharedWorld, which — unlike world.New — does not tear the world down
// when the calling test's t completes. That matters here because multiple
// independent top-level Test* functions in this package (TestBaselines and
// its siblings) share this one world across the whole run; ownership of
// teardown belongs to TestMain below, not to whichever top-level test
// happens to call getWorld first.
func getWorld(t *testing.T) (*world.World, *invariants.Checker) {
	t.Helper()

	worldOnce.Do(func() {
		if _, err := os.Stat(world.ManagerBin()); err != nil {
			t.Skipf("bin/manager not built: %v", err)
		}
		sharedW = world.SharedWorld(t)

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

// TestMain owns the lifecycle of the shared world and its invariants
// checker: they are built lazily by the first top-level test that calls
// getWorld, but must be torn down exactly once, after all top-level tests in
// this binary have finished — not by any individual test's t.Cleanup, which
// would tear the world down as soon as the first top-level test returned and
// leave later sibling tests (e.g. TestSharedWorldSurvivesSiblingTests)
// holding a dead world.
func TestMain(m *testing.M) {
	code := m.Run()

	if sharedC != nil {
		sharedC.Stop()
	}
	world.StopShared()

	os.Exit(code)
}
