// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"

	"github.com/ramendr/ramen/simtest/observe"
	"github.com/ramendr/ramen/simtest/user"
	"github.com/ramendr/ramen/simtest/world"
)

const (
	enableTimeout = 3 * time.Minute
	moveTimeout   = 5 * time.Minute
)

type Hook struct {
	At rmn.ProgressionStatus
	Do func()
}

func newRecorder(t *testing.T, w *world.World, app user.App) *observe.Recorder {
	t.Helper()

	r, err := observe.NewRecorder(context.Background(), w.Hub.Cfg, app.ManagementNamespace(), app.Name)
	if err != nil {
		t.Fatalf("recorder: %v", err)
	}
	t.Cleanup(r.Stop)

	return r
}

func runEnable(t *testing.T, w *world.World, app user.App) *observe.Recorder {
	t.Helper()

	if err := user.CreateApp(context.Background(), w, app, world.DR1Name); err != nil {
		t.Fatalf("create app: %v", err)
	}

	return runEnroll(t, w, app)
}

// runEnroll enables DR protection for an already-created app and gates on
// the DRPC reaching steady Deployed (the enroll stage of the lifecycle).
func runEnroll(t *testing.T, w *world.World, app user.App) *observe.Recorder {
	t.Helper()
	ctx := context.Background()

	rec := newRecorder(t, w, app)

	if err := user.EnableProtection(ctx, w, app); err != nil {
		t.Fatalf("enable: %v", err)
	}
	if err := rec.WaitPhase(string(rmn.Deployed), enableTimeout); err != nil {
		t.Fatal(err)
	}
	if err := observe.WaitDRPCReady(ctx, w.Hub.Client, app.ManagementNamespace(), app.Name, enableTimeout); err != nil {
		t.Fatal(err)
	}

	return rec
}

// runMove drives one failover/relocate and gates on its completion, returning
// the progression sequence its recorder observed (the matrix seed run uses it
// to discover checkpoints; other callers ignore it). It uses a
// fresh recorder rather than the lifecycle one from runEnable: OnProgression
// fires immediately for values the recorder has already seen, so reusing one
// recorder across moves would fire the second move's WaitOnUserToCleanUp
// cleanup hook instantly (the value was recorded during the first move) and
// delete the app's PVC on the current primary before the move even starts.
func runMove(t *testing.T, w *world.World, app user.App,
	action func(context.Context) error, cleanupCluster string, donePhase rmn.DRState, hooks []Hook,
) []string {
	t.Helper()
	ctx := context.Background()

	rec := newRecorder(t, w, app)
	defer rec.Stop()

	for _, h := range hooks {
		rec.OnProgression(string(h.At), h.Do)
	}

	// Discovered apps park at WaitOnUserToCleanUp until the user deletes the
	// workload on the old cluster. The hook runs on a recorder goroutine, so
	// it must not call t.* directly; it records its error for the gates below.
	var (
		cleanupMu  sync.Mutex
		cleanupErr error
	)

	rec.OnProgression(string(rmn.ProgressionWaitOnUserToCleanUp), func() {
		err := user.DeleteApp(ctx, w, app, cleanupCluster)

		cleanupMu.Lock()
		cleanupErr = err
		cleanupMu.Unlock()
	})

	lastCleanupErr := func() error {
		cleanupMu.Lock()
		defer cleanupMu.Unlock()

		return cleanupErr
	}

	if err := action(ctx); err != nil {
		t.Fatalf("action: %v", err)
	}
	if err := rec.WaitPhase(string(donePhase), moveTimeout); err != nil {
		t.Fatalf("%v (cleanup on %s: %v)", err, cleanupCluster, lastCleanupErr())
	}
	if err := observe.WaitDRPCReady(ctx, w.Hub.Client, app.ManagementNamespace(), app.Name, moveTimeout); err != nil {
		t.Fatalf("%v (cleanup on %s: %v)", err, cleanupCluster, lastCleanupErr())
	}
	if err := lastCleanupErr(); err != nil {
		t.Fatalf("cleanup app on %s: %v", cleanupCluster, err)
	}

	return rec.Progressions()
}

func runFailover(t *testing.T, w *world.World, app user.App, hooks ...Hook) {
	runMove(t, w, app,
		func(ctx context.Context) error { return user.Failover(ctx, w, app, world.DR2Name) },
		world.DR1Name, rmn.FailedOver, hooks)
}

func runRelocate(t *testing.T, w *world.World, app user.App, hooks ...Hook) {
	runMove(t, w, app,
		func(ctx context.Context) error { return user.Relocate(ctx, w, app, world.DR1Name) },
		world.DR2Name, rmn.Relocated, hooks)
}

// pvcspecs is the storage-story axis, mirroring e2e's PVCSpec: rbd routes
// through VolRep (peer classes carry a replicationid), cephfs through
// VolSync (storageid-only peer classes with a snapshot class).
var pvcspecs = []struct {
	name         string
	storageClass string
	cg           bool
}{
	{name: "rbd", storageClass: world.StorageClassName},
	{name: "cephfs", storageClass: world.CephFSStorageClassName},
	{name: "rbd-cg", storageClass: world.CGStorageClassName, cg: true},
	{name: "cephfs-cg", storageClass: world.CGCephFSStorageClassName, cg: true},
}

// TestBaselines is T1: the full happy-path lifecycle of one discovered app,
// once per pvcspec.
func TestBaselines(t *testing.T) {
	w, checker := getWorld(t)

	for _, spec := range pvcspecs {
		t.Run(spec.name, func(t *testing.T) {
			runBaseline(t, w, user.App{Name: "bl-" + spec.name, StorageClassName: spec.storageClass, CG: spec.cg})
		})
	}

	checker.AssertClean(t)
}

func runBaseline(t *testing.T, w *world.World, app user.App) {
	t.Helper()
	ctx := context.Background()

	rec := runEnable(t, w, app)

	t.Run("failover", func(t *testing.T) {
		uiScenario(t, w, app.Name+"/failover")
		runFailover(t, w, app)
	})
	t.Run("relocate", func(t *testing.T) {
		uiScenario(t, w, app.Name+"/relocate")
		runRelocate(t, w, app)
	})

	t.Run("disable", func(t *testing.T) {
		uiScenario(t, w, app.Name+"/disable")

		if err := user.Disable(ctx, w, app, observe.Scale(2*time.Minute)); err != nil {
			t.Fatal(err)
		}

		// Cleanup completeness: no VRG remains on either managed cluster.
		deadline := time.Now().Add(observe.Scale(2 * time.Minute))
		for {
			left := 0
			for _, m := range w.Managed() {
				vrgs := &rmn.VolumeReplicationGroupList{}
				if err := m.Client.List(ctx, vrgs); err == nil {
					left += len(vrgs.Items)
				}
			}
			if left == 0 {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%d VRGs left after disable", left)
			}
			time.Sleep(500 * time.Millisecond)
		}
	})

	t.Logf("%s progression sequence: %v", app.Name, rec.Progressions())
}
