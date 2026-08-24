// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package invariants

import (
	"testing"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func drpcWith(phase rmn.DRState, peerReady bool) *rmn.DRPlacementControl {
	d := &rmn.DRPlacementControl{}
	d.Status.Phase = phase
	st := metav1.ConditionFalse
	if peerReady {
		st = metav1.ConditionTrue
	}
	d.Status.Conditions = []metav1.Condition{{Type: rmn.ConditionPeerReady, Status: st, Reason: "x"}}

	return d
}

func TestSinglePrimaryRule(t *testing.T) {
	if !violatesSinglePrimary(2, drpcWith(rmn.Deployed, true)) {
		t.Fatal("two primaries while Deployed must violate")
	}
	if violatesSinglePrimary(2, drpcWith(rmn.FailingOver, false)) {
		t.Fatal("two primaries while FailingOver is the allowed window")
	}
	if violatesSinglePrimary(2, drpcWith(rmn.FailedOver, false)) {
		t.Fatal("two primaries in FailedOver before PeerReady is allowed (old cluster not cleaned)")
	}
	if !violatesSinglePrimary(2, drpcWith(rmn.FailedOver, true)) {
		t.Fatal("two primaries after PeerReady must violate")
	}
	if violatesSinglePrimary(1, drpcWith(rmn.Deployed, true)) {
		t.Fatal("one primary never violates")
	}
	if !violatesSinglePrimary(2, drpcWith(rmn.Relocating, false)) {
		t.Fatal("relocate demotes before promoting; two primaries during relocate violate")
	}
}

func TestCheckerOnViolation(t *testing.T) {
	c := &Checker{}

	var got string
	c.SetOnViolation(func(v string) { got = v })
	c.addViolation("dual primary")

	if got != "dual primary" {
		t.Fatalf("OnViolation got %q", got)
	}
}

// Ramen updates the peer MW to primary before persisting the FailingOver
// phase, so with millisecond-speed actors the checker can observe two
// primaries while status still reads a stable phase. The transitional window
// must therefore open on recorded spec intent (Action=Failover), not the
// lagging phase, and close when PeerReady reports cleanup complete.
func TestSinglePrimaryAllowsSpecFailoverBeforePhaseCatchesUp(t *testing.T) {
	drpc := &rmn.DRPlacementControl{}
	drpc.Spec.Action = rmn.ActionFailover
	drpc.Status.Phase = rmn.Deployed

	if violatesSinglePrimary(2, drpc) {
		t.Fatal("2 primaries with spec.Action=Failover and lagging phase must be transitional, not a violation")
	}
}

func TestSinglePrimaryStillViolatedWithoutAction(t *testing.T) {
	drpc := &rmn.DRPlacementControl{}
	drpc.Status.Phase = rmn.Deployed

	if !violatesSinglePrimary(2, drpc) {
		t.Fatal("2 primaries in steady Deployed with no action must violate")
	}
}

func TestSinglePrimaryViolatedAfterFailoverCleanupCompletes(t *testing.T) {
	drpc := &rmn.DRPlacementControl{}
	drpc.Spec.Action = rmn.ActionFailover
	drpc.Status.Phase = rmn.FailedOver
	drpc.Status.Conditions = []metav1.Condition{{
		Type: rmn.ConditionPeerReady, Status: metav1.ConditionTrue, Reason: "Ready",
	}}

	if !violatesSinglePrimary(2, drpc) {
		t.Fatal("2 primaries after PeerReady=True must violate even with spec.Action still set")
	}
}
