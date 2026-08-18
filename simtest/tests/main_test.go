// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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

		sharedW.UIHub().RunStart("simtest")
		sharedC.OnViolation = sharedW.UIHub().ObserveInvariant
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

	if os.Getenv("SIMTEST_UI_HOLD") != "" && sharedW != nil && sharedW.UI != nil {
		fmt.Printf("simtest ui: holding at %s — Ctrl-C to exit\n", sharedW.UI.URL())
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		<-ctx.Done()
		stop()
	}

	if sharedC != nil {
		sharedC.Stop()
	}
	world.StopShared()

	os.Exit(code)
}

// uiScenario reports one scenario's lifecycle to the UI hub (no-op when the
// UI is off). Call it first thing inside a subtest.
func uiScenario(t *testing.T, w *world.World, id string) {
	t.Helper()
	h := w.UIHub()
	h.ScenarioStart(id)
	t.Cleanup(func() {
		result, reason := "passed", ""
		if t.Failed() {
			result, reason = "failed", "see test log"
		}
		h.ScenarioEnd(id, result, reason)
	})
}
