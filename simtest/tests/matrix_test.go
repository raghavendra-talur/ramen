// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"

	"github.com/ramendr/ramen/simtest/actors"
	"github.com/ramendr/ramen/simtest/observe"
	"github.com/ramendr/ramen/simtest/user"
	"github.com/ramendr/ramen/simtest/world"
)

// faultWindow is how long each injected fault stays active before it is
// cleared and recovery is asserted.
const faultWindow = 8 * time.Second

// checkpoint is a failover progression the matrix may hook, with a short slug
// used in app names (DRPC/PVC/namespace name-length limits).
type checkpoint struct {
	prog rmn.ProgressionStatus
	code string
}

// stableCheckpoints are the failover progressions deterministic enough to
// hook: each is gated on external work (resource restore, peer cleanup, user
// cleanup) rather than flashing by between two status updates. Flash states
// (VolSync-related, WaitForReadiness) are non-deterministic and are dropped
// from the seed sequence with a log line. ProgressionCompleted is
// deliberately absent: each move uses a fresh recorder, which replays the
// resting state of the previous move, so a Completed hook would fire before
// the new move even starts.
var stableCheckpoints = []checkpoint{
	{rmn.ProgressionWaitingForResourceRestore, "wrr"},
	{rmn.ProgressionCleaningUp, "clean"},
	{rmn.ProgressionWaitOnUserToCleanUp, "wuc"},
}

// fault is one injectable external failure; clear() must fully restore health
// and must be idempotent (the matrix clears via a fault-window timer and,
// belt-and-braces, on subtest exit).
type fault struct {
	name  string
	apply func(w *world.World)
	clear func(w *world.World)
}

func policyFault(name string, key func(string) actors.Key, cluster string, p actors.Policy) fault {
	return fault{
		name:  name,
		apply: func(w *world.World) { w.Actors.Store.Set(key(cluster), p) },
		clear: func(w *world.World) { w.Actors.Store.Set(key(cluster), actors.Normal{}) },
	}
}

// faults is the external-failure axis of the matrix. s3-down is a GLOBAL
// fault (one store for the whole world), so the matrix must stay sequential
// as long as it is present. The policy faults are per actor per cluster.
func faults() []fault {
	fs := []fault{
		{
			name:  "s3-down",
			apply: func(w *world.World) { w.S3.SetDown(true) },
			clear: func(w *world.World) { w.S3.SetDown(false) },
		},
	}

	for _, cluster := range []string{world.DR1Name, world.DR2Name} {
		fs = append(fs,
			policyFault("volrep-silent-"+cluster, actors.VolRep, cluster, actors.Silent{}),
			policyFault("volrep-degraded-"+cluster, actors.VolRep, cluster, actors.FailWith{Mode: "degraded"}),
			policyFault("view-silent-"+cluster, actors.View, cluster, actors.Silent{}),
			policyFault("work-silent-"+cluster, actors.Work, cluster, actors.Silent{}),
			// T3 ordering variants: delays reorder independent external events
			// (e.g. VR status lands before/after the MCV refresh at the same gate).
			policyFault("volrep-delayed-"+cluster, actors.VolRep, cluster, actors.Delayed{After: 3 * time.Second}),
			policyFault("view-delayed-"+cluster, actors.View, cluster, actors.Delayed{After: 3 * time.Second}),
		)
	}

	return fs
}

// runFailoverSeq is the runFailover variant that returns the progression
// sequence the move recorder observed; the seed run uses it to discover
// checkpoints, and combos log it as the per-fault behavioral record.
func runFailoverSeq(t *testing.T, w *world.World, app user.App, hooks ...Hook) []string {
	t.Helper()

	return runMove(t, w, app,
		func(ctx context.Context) error { return user.Failover(ctx, w, app, world.DR2Name) },
		world.DR1Name, rmn.FailedOver, hooks)
}

// TestMatrix is T2 (+T3 via the delayed faults): run one failover to record
// the checkpoint sequence of THIS world, then for each stable checkpoint x
// fault, run a fresh app through failover with the fault injected at that
// checkpoint and cleared after faultWindow. Recovery to completion is the
// per-combo assertion; the shared invariants checker guards safety (single
// primary) cumulatively across all combos.
func TestMatrix(t *testing.T) {
	w, checker := getWorld(t)

	// Seed run: discover the failover checkpoint sequence.
	seedApp := user.App{Name: "mx-seed"}
	runEnable(t, w, seedApp)
	seq := runFailoverSeq(t, w, seedApp)
	t.Logf("seed failover sequence: %v", seq)

	checkpoints := seedCheckpoints(t, seq)
	if len(checkpoints) == 0 {
		t.Fatalf("no stable checkpoints observed in seed sequence %v", seq)
	}

	fs := faults()
	start := time.Now()
	n := 0

	for _, cp := range checkpoints {
		for _, f := range fs {
			n++
			runCombo(t, w, n, cp, f)
		}
	}

	t.Logf("matrix ran %d combinations in %s", n, time.Since(start).Round(time.Second))
	checker.AssertClean(t)
}

// runCombo runs one checkpoint x fault cell as a subtest: fresh app, enable,
// failover with the fault applied at the checkpoint and cleared after
// faultWindow. The clear is wrapped in a sync.Once and also deferred, so a
// combo that fails or times out can never leak an active fault (a leaked
// Silent{} or S3 outage would poison every later combo).
func runCombo(t *testing.T, w *world.World, idx int, cp checkpoint, f fault) {
	name := fmt.Sprintf("failover/at=%s/fault=%s", cp.code, f.name)

	t.Run(name, func(t *testing.T) {
		begin := time.Now()
		app := user.App{Name: sanitize(fmt.Sprintf("mx%02d-%s-%s", idx, cp.code, f.name))}

		runEnable(t, w, app)

		var (
			applied   atomic.Bool
			clearOnce sync.Once
		)

		clearFault := func() {
			clearOnce.Do(func() {
				f.clear(w)
				w.Actors.Log.Logf("matrix: cleared %s (%s)", f.name, app.Name)
			})
		}
		defer clearFault()

		seq := runFailoverSeq(t, w, app, Hook{
			At: cp.prog,
			Do: func() {
				applied.Store(true)
				f.apply(w)
				w.Actors.Log.Logf("matrix: applied %s at %s (%s)", f.name, cp.prog, app.Name)
				time.AfterFunc(observe.Scale(faultWindow), clearFault)
			},
		})

		if !applied.Load() {
			t.Errorf("fault %s was never applied: checkpoint %s not reached during failover", f.name, cp.prog)
		}

		t.Logf("recovered in %s; sequence: %v", time.Since(begin).Round(time.Second), seq)
	})
}

// seedCheckpoints intersects the seed run's observed progressions with the
// known-stable checkpoint set, preserving order, and logs everything dropped
// from either side (no silent trimming).
func seedCheckpoints(t *testing.T, seq []string) []checkpoint {
	t.Helper()

	observed := map[string]bool{}
	for _, s := range seq {
		observed[s] = true
	}

	stable := map[string]bool{}
	kept := []checkpoint{}

	for _, cp := range stableCheckpoints {
		stable[string(cp.prog)] = true

		if observed[string(cp.prog)] {
			kept = append(kept, cp)
		} else {
			t.Logf("dropping checkpoint %q: not observed in this world's seed run", cp.prog)
		}
	}

	for _, s := range seq {
		if !stable[s] && s != string(rmn.ProgressionCompleted) {
			t.Logf("dropping observed progression %q: flash/non-deterministic, not hookable", s)
		}
	}

	return kept
}

// sanitize maps a combo id to a DNS-1123-safe app name; the mx<idx> prefix
// guarantees uniqueness even if truncation collides.
func sanitize(s string) string {
	s = strings.ToLower(s)
	s = strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			return r
		}

		return '-'
	}, s)
	if len(s) > 40 {
		s = s[:40]
	}

	return strings.Trim(s, "-")
}
