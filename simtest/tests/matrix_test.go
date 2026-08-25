// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
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
	// Relocate-only: gated on the final sync completing before the switch.
	{rmn.ProgressionPreparingFinalSync, "pfs"},
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

// s3Fault is a GLOBAL fault (one store for the whole world), so the matrix
// must stay sequential as long as it is present.
func s3Fault() fault {
	return fault{
		name:  "s3-down",
		apply: func(w *world.World) { w.S3.SetDown(true) },
		clear: func(w *world.World) { w.S3.SetDown(false) },
	}
}

// volRepFaults starves or perturbs the replication backend actor — the key
// gates both the per-PVC VolumeReplication and the grouped VGR fulfillers.
func volRepFaults() []fault {
	fs := []fault{}

	for _, cluster := range []string{world.DR1Name, world.DR2Name} {
		fs = append(fs,
			policyFault("volrep-silent-"+cluster, actors.VolRep, cluster, actors.Silent{}),
			policyFault("volrep-degraded-"+cluster, actors.VolRep, cluster, actors.FailWith{Mode: "degraded"}),
			// T3 ordering variants: delays reorder independent external events
			// (e.g. VR status lands before/after the MCV refresh at the same gate).
			policyFault("volrep-delayed-"+cluster, actors.VolRep, cluster, actors.Delayed{After: 3 * time.Second}),
		)
	}

	return fs
}

// ocmFaults perturbs the hub-to-managed transports (work/view agents).
func ocmFaults() []fault {
	fs := []fault{}

	for _, cluster := range []string{world.DR1Name, world.DR2Name} {
		fs = append(fs,
			policyFault("view-silent-"+cluster, actors.View, cluster, actors.Silent{}),
			policyFault("work-silent-"+cluster, actors.Work, cluster, actors.Silent{}),
			policyFault("view-delayed-"+cluster, actors.View, cluster, actors.Delayed{After: 3 * time.Second}),
		)
	}

	return fs
}

// volSyncFaults starves the VolSync-side actors: the RS/RD fulfiller, the
// snapshotter (which also gates group snapshots), the job runner, and the
// governance policy agent that delivers the PSK secret.
func volSyncFaults() []fault {
	fs := []fault{}

	for _, cluster := range []string{world.DR1Name, world.DR2Name} {
		fs = append(fs,
			policyFault("volsync-silent-"+cluster, actors.VolSync, cluster, actors.Silent{}),
			policyFault("snap-silent-"+cluster, actors.Snap, cluster, actors.Silent{}),
			policyFault("jobs-silent-"+cluster, actors.Jobs, cluster, actors.Silent{}),
			policyFault("polagent-silent-"+cluster, actors.PolicyAgent, cluster, actors.Silent{}),
		)
	}

	return fs
}

// matrixSpec is one storage story's slice of the matrix: its own seed run
// (checkpoint sequences differ per story), the faults that can plausibly
// touch its data path, and an optional checkpoint filter for stories whose
// lifecycle is too slow for the full grid.
type matrixSpec struct {
	name         string
	storageClass string
	cg           bool
	faults       []fault
	// cpFilter, when non-nil, keeps only these checkpoint codes — used to
	// bound the cephfs-cg grid, whose lifecycle converges on ramen's
	// minute-scale requeues (~5min per combo).
	cpFilter map[string]bool
}

func matrixSpecs() []matrixSpec {
	rbdFaults := append([]fault{s3Fault()}, append(volRepFaults(), ocmFaults()...)...)
	cephfsFaults := append([]fault{s3Fault()}, volSyncFaults()...)

	return []matrixSpec{
		// rbd keeps the historical full grid: every fault kind including the
		// OCM transports (exercised once here rather than per story).
		{name: "rbd", storageClass: world.StorageClassName, faults: rbdFaults},
		{name: "rbd-cg", storageClass: world.CGStorageClassName, cg: true,
			faults: append([]fault{s3Fault()}, volRepFaults()...)},
		{name: "cephfs", storageClass: world.CephFSStorageClassName, faults: cephfsFaults},
		{name: "cephfs-cg", storageClass: world.CGCephFSStorageClassName, cg: true,
			faults: []fault{
				s3Fault(),
				policyFault("volsync-silent-"+world.DR1Name, actors.VolSync, world.DR1Name, actors.Silent{}),
				policyFault("snap-silent-"+world.DR2Name, actors.Snap, world.DR2Name, actors.Silent{}),
				policyFault("jobs-silent-"+world.DR1Name, actors.Jobs, world.DR1Name, actors.Silent{}),
			},
			cpFilter: map[string]bool{"wrr": true, "pfs": true},
		},
	}
}

// TestMatrix is T2 (+T3 via the delayed faults): one seed app runs the full
// lifecycle to record THIS world's failover and relocate checkpoint
// sequences, then every stable (stage, checkpoint) x fault combination runs
// a fresh app through the ENTIRE lifecycle — create, enroll, failover,
// relocate, unenroll, delete — with the fault injected at that stage's
// checkpoint and cleared after faultWindow. Recovery to completion is the
// per-stage assertion, the per-app leak check guards cleanup after delete,
// and the shared invariants checker guards safety cumulatively.
func TestMatrix(t *testing.T) {
	w, checker := getWorld(t)

	start := time.Now()
	n := 0

	for _, spec := range matrixSpecs() {
		t.Run(spec.name, func(t *testing.T) {
			seedApp := user.App{Name: "mx-seed-" + sanitize(spec.name),
				StorageClassName: spec.storageClass, CG: spec.cg}
			seqs := runLifecycle(t, w, seedApp, fullLifecycle, "")
			t.Logf("%s seed failover sequence: %v", spec.name, seqs[stgFailover])
			t.Logf("%s seed relocate sequence: %v", spec.name, seqs[stgRelocate])

			for _, st := range []stage{stgFailover, stgRelocate} {
				cps := seedCheckpoints(t, seqs[st])
				if len(cps) == 0 {
					t.Fatalf("no stable checkpoints for %s in seed sequence %v", st, seqs[st])
				}

				for _, cp := range cps {
					if spec.cpFilter != nil && !spec.cpFilter[cp.code] {
						t.Logf("dropping checkpoint %q for %s: filtered by the spec's grid bound", cp.code, spec.name)

						continue
					}

					for _, f := range spec.faults {
						n++
						runCombo(t, w, n, spec, st, cp, f)
					}
				}
			}
		})
	}

	t.Logf("matrix ran %d combinations in %s", n, time.Since(start).Round(time.Second))
	checker.AssertClean(t)
}

// runCombo runs one (stage, checkpoint) x fault cell as a subtest: a fresh
// app runs the FULL lifecycle with the fault applied at the given stage's
// checkpoint and cleared after faultWindow. The clear is wrapped in a
// sync.Once and also deferred, so a combo that fails or times out can never
// leak an active fault (a leaked Silent{} or S3 outage would poison every
// later combo); the lifecycle's delete stage asserts the app leaves no
// residue behind.
func runCombo(t *testing.T, w *world.World, idx int, spec matrixSpec, st stage, cp checkpoint, f fault) {
	name := fmt.Sprintf("%s/at=%s/fault=%s", st, cp.code, f.name)

	t.Run(name, func(t *testing.T) {
		uiScenario(t, w, name)

		// Fail fast on a half-dead world: a manager that died during an
		// earlier combo would otherwise time this combo out and blame the
		// wrong fault, silently poisoning every combo after it too.
		if err := w.ManagersAlive(); err != nil {
			t.Fatalf("aborting combo, world unhealthy before injection: %v", err)
		}

		begin := time.Now()
		app := user.App{
			Name:             sanitize(fmt.Sprintf("mx%02d-%.1s-%s-%s", idx, st, cp.code, f.name)),
			StorageClassName: spec.storageClass,
			CG:               spec.cg,
		}

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

		seqs := runLifecycle(t, w, app, fullLifecycle, st, Hook{
			At: cp.prog,
			Do: func() {
				applied.Store(true)
				f.apply(w)
				w.Actors.Log.Logf("matrix: applied %s at %s/%s (%s)", f.name, st, cp.prog, app.Name)
				time.AfterFunc(observe.Scale(faultWindow), clearFault)
			},
		})

		if !applied.Load() {
			t.Errorf("fault %s was never applied: checkpoint %s not reached during %s", f.name, cp.prog, st)
		}

		t.Logf("lifecycle done in %s; %s sequence: %v", time.Since(begin).Round(time.Second), st, seqs[st])
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
