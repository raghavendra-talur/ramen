// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

// Internal tests for the rbd-mirror health-wait retry/daemon-restart loop. They
// live in package addon so they can shrink the unexported attempt/timeout knobs
// to force the restart path deterministically without real timing.

import (
	"context"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// healthDeps builds a minimal Deps for the health-wait helpers.
func healthDeps(f *cli.FakeRunner) Deps {
	return Deps{
		K:    &cli.Kubectl{R: f},
		Opts: ensure.Options{VerifyInterval: time.Millisecond},
	}
}

// withHealthKnobs temporarily overrides the package retry knobs.
func withHealthKnobs(t *testing.T, attempts int, timeout time.Duration) {
	t.Helper()
	oa, ot := rbdMirrorHealthAttempts, rbdMirrorHealthTimeout
	rbdMirrorHealthAttempts, rbdMirrorHealthTimeout = attempts, timeout
	t.Cleanup(func() { rbdMirrorHealthAttempts, rbdMirrorHealthTimeout = oa, ot })
}

// TestWaitRBDMirroringHealthyRestartsOnTimeout drives the loop through one
// timeout (which triggers a daemon restart) and then success on the second
// attempt, asserting the restart issued the expected kubectl rollout commands.
func TestWaitRBDMirroringHealthyRestartsOnTimeout(t *testing.T) {
	withHealthKnobs(t, 2, 0) // timeout 0 → first non-OK check trips the deadline

	f := &cli.FakeRunner{}
	// Attempt 1: first health field not OK → 1 get, then immediate timeout.
	f.Script(cli.FakeResult{Out: "NOTOK"}) // [0] get daemon_health → not OK
	// Daemon restart: rollout restart + rollout status.
	f.Script(cli.FakeResult{}) // [1] rollout restart
	f.Script(cli.FakeResult{}) // [2] rollout status
	// Attempt 2: all three fields OK → healthy.
	f.Script(cli.FakeResult{Out: "OK"}) // [3] daemon_health
	f.Script(cli.FakeResult{Out: "OK"}) // [4] health
	f.Script(cli.FakeResult{Out: "OK"}) // [5] image_health

	if err := waitRBDMirroringHealthy(context.Background(), healthDeps(f), "dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(f.Calls) != 6 {
		t.Fatalf("expected 6 calls, got %d: %+v", len(f.Calls), f.Calls)
	}

	// [1]: kubectl --context dr1 -n rook-ceph rollout restart deploy/rook-ceph-rbd-mirror-a
	wantRestart := []string{"--context", "dr1", "-n", "rook-ceph", "rollout", "restart", rbdMirrorDaemonDeploy}
	if got := f.Calls[1].Args; !equalArgs(got, wantRestart) {
		t.Errorf("restart args = %v, want %v", got, wantRestart)
	}
	// [2]: rollout status ... --timeout 120s
	wantStatus := []string{"--context", "dr1", "-n", "rook-ceph", "rollout", "status", rbdMirrorDaemonDeploy, "--timeout", "120s"}
	if got := f.Calls[2].Args; !equalArgs(got, wantStatus) {
		t.Errorf("status args = %v, want %v", got, wantStatus)
	}
}

// TestWaitRBDMirroringHealthyFailsAfterAllAttempts verifies the final attempt's
// timeout is fatal and no restart happens after it.
func TestWaitRBDMirroringHealthyFailsAfterAllAttempts(t *testing.T) {
	withHealthKnobs(t, 2, 0)

	f := &cli.FakeRunner{}
	// Attempt 1: not OK → timeout → restart (2 calls).
	f.Script(cli.FakeResult{Out: "NOTOK"})
	f.Script(cli.FakeResult{})
	f.Script(cli.FakeResult{})
	// Attempt 2 (final): not OK → timeout → fatal, no restart.
	f.Script(cli.FakeResult{Out: "NOTOK"})

	err := waitRBDMirroringHealthy(context.Background(), healthDeps(f), "dr1")
	if err == nil {
		t.Fatal("expected error after all attempts exhausted, got nil")
	}
	if len(f.Calls) != 4 {
		t.Fatalf("expected 4 calls (1 get + 2 restart + 1 get), got %d: %+v", len(f.Calls), f.Calls)
	}
}

// TestPollRBDMirroringHealthyPropagatesKubectlError verifies a kubectl failure
// during polling is returned immediately (not treated as a retryable timeout).
func TestPollRBDMirroringHealthyPropagatesKubectlError(t *testing.T) {
	withHealthKnobs(t, 3, time.Minute)

	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Err: context.DeadlineExceeded}) // get fails hard

	err := waitRBDMirroringHealthy(context.Background(), healthDeps(f), "dr1")
	if err == nil {
		t.Fatal("expected kubectl error to propagate, got nil")
	}
	// A hard error must short-circuit: exactly one call, no restart.
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d: %+v", len(f.Calls), f.Calls)
	}
}

func equalArgs(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
