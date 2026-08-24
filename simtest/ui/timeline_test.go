// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"testing"
	"time"
)

func at(sec int) time.Time { return time.Unix(int64(sec), 0) }

func TestAppendSegmentClosesPriorOnChange(t *testing.T) {
	tl := Timeline{}
	appendSegment(tl, "drpc-phase", "Deployed", at(0))
	appendSegment(tl, "drpc-phase", "FailingOver", at(10))

	segs := tl["drpc-phase"]
	if len(segs) != 2 {
		t.Fatalf("segments: %+v", segs)
	}
	if !segs[0].End.Equal(at(10)) {
		t.Fatalf("first segment not closed at change: %+v", segs[0])
	}
	if segs[1].Value != "FailingOver" || !segs[1].End.IsZero() {
		t.Fatalf("second segment wrong: %+v", segs[1])
	}
}

func TestAppendSegmentSameValueExtends(t *testing.T) {
	tl := Timeline{}
	appendSegment(tl, "drpc-phase", "Deployed", at(0))
	appendSegment(tl, "drpc-phase", "Deployed", at(5)) // watch resync duplicate
	if len(tl["drpc-phase"]) != 1 {
		t.Fatalf("duplicate value split the segment: %+v", tl["drpc-phase"])
	}
}

func TestScenarioTimelineEndToEnd(t *testing.T) {
	h := New()
	h.ScenarioStart("s1")
	h.ObserveObject(ObjectState{Cluster: "hub", Kind: "DRPlacementControl",
		Namespace: "ramen-ops", Name: "a", At: at(1),
		Fields: map[string]string{"phase": "Deployed", "progression": "Completed"}})
	h.ObserveObject(ObjectState{Cluster: "dr1", Kind: "VolumeReplicationGroup",
		Namespace: "app", Name: "vrg", At: at(2),
		Fields: map[string]string{"state": "primary"}})
	h.ObserveFault("s3", "s3 outage", true)
	h.ObserveInvariant("dual primary")
	h.ScenarioEnd("s1", "failed", "inv")

	tl := h.Snapshot().Timelines["s1"]
	for _, track := range []string{"drpc-phase", "drpc-progression", "vrg-dr1", "s3", "invariants"} {
		if len(tl[track]) == 0 {
			t.Fatalf("track %q empty; timeline: %+v", track, tl)
		}
	}

	// The s3 track renders on the timeline; its values must be the states
	// an operator reads at a glance, not the fault-event vocabulary.
	if v := tl["s3"][0].Value; v != "down" {
		t.Fatalf(`s3 track value = %q, want "down"`, v)
	}
	for track, segs := range tl {
		if segs[len(segs)-1].End.IsZero() {
			t.Fatalf("track %q left open after ScenarioEnd", track)
		}
	}
}

func TestObservationsOutsideScenarioDropFromTimelines(t *testing.T) {
	h := New()
	// No ScenarioStart: between-scenario noise must not create timelines,
	// but the object still lands in Objects for the stage.
	h.ObserveObject(ObjectState{Cluster: "hub", Kind: "DRPlacementControl",
		Namespace: "ramen-ops", Name: "a", At: at(1),
		Fields: map[string]string{"phase": "Deployed", "progression": "Completed"}})
	s := h.Snapshot()
	if len(s.Timelines) != 0 {
		t.Fatalf("timelines: %+v", s.Timelines)
	}
	if len(s.Objects) != 1 {
		t.Fatalf("objects: %+v", s.Objects)
	}
}

func TestFinishedScenarioTimelineRetained(t *testing.T) {
	h := New()
	h.ScenarioStart("s1")
	h.ObserveObject(ObjectState{Cluster: "hub", Kind: "DRPlacementControl",
		Namespace: "ramen-ops", Name: "a", At: at(1),
		Fields: map[string]string{"phase": "FailingOver", "progression": "WaitForReadiness"}})
	h.ScenarioEnd("s1", "passed", "")
	h.ScenarioStart("s2")
	h.ScenarioEnd("s2", "passed", "")

	if tl := h.Snapshot().Timelines["s1"]; len(tl["drpc-phase"]) == 0 {
		t.Fatal("finished scenario s1 lost its timeline (replay-on-click needs it)")
	}
}
