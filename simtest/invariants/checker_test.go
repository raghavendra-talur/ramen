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
	c.OnViolation = func(v string) { got = v }
	c.addViolation("dual primary")

	if got != "dual primary" {
		t.Fatalf("OnViolation got %q", got)
	}
}
