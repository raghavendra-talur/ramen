// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestNilHubIsNoOp(t *testing.T) {
	var h *Hub // nil on purpose: UI disabled
	h.RunStart("run")
	h.ScenarioStart("s1")
	h.ScenarioEnd("s1", "passed", "")
	h.ObserveObject(ObjectState{Kind: "DRPC"})
	h.ObserveManager("hub", true)
	h.ObserveFault("s3", "s3 outage", true)
	h.ObserveActorEvent("line")
	h.ObserveInvariant("v")
	// Snapshot on nil returns a zero snapshot rather than panicking.
	if s := h.Snapshot(); s.Seq != 0 || len(s.Tests) != 0 {
		t.Fatalf("nil hub snapshot not zero: %+v", s)
	}
}

func TestScenarioLifecycle(t *testing.T) {
	h := New()
	h.RunStart("failover matrix")
	h.ScenarioStart("failover/at=wrr/fault=s3-down")
	s := h.Snapshot()
	if s.Run.Name != "failover matrix" {
		t.Fatalf("run name = %q", s.Run.Name)
	}
	if len(s.Tests) != 1 || s.Tests[0].Status != TestRunning {
		t.Fatalf("tests after start: %+v", s.Tests)
	}
	if s.Current != "failover/at=wrr/fault=s3-down" {
		t.Fatalf("current = %q", s.Current)
	}

	h.ScenarioEnd("failover/at=wrr/fault=s3-down", "failed", "inv: dual primary")
	s = h.Snapshot()
	if s.Tests[0].Status != TestFailed || s.Tests[0].Reason != "inv: dual primary" {
		t.Fatalf("tests after end: %+v", s.Tests)
	}
	if s.Current != "" {
		t.Fatalf("current not cleared: %q", s.Current)
	}
	if s.Tests[0].FinishedAt.IsZero() {
		t.Fatal("FinishedAt not set")
	}
}

func TestObservationsLandInSnapshot(t *testing.T) {
	h := New()
	h.ObserveObject(ObjectState{Cluster: "dr1", Kind: "VolumeReplicationGroup",
		Namespace: "app", Name: "vrg", Fields: map[string]string{"state": "primary"}})
	h.ObserveManager("dr2", false)
	h.ObserveFault("volrep@dr1", "volrep silent", true)
	h.ObserveActorEvent("mw-agent applied vrg")
	h.ObserveInvariant("dual primary 340ms")

	s := h.Snapshot()
	if len(s.Objects) != 1 || s.Objects[0].Fields["state"] != "primary" {
		t.Fatalf("objects: %+v", s.Objects)
	}
	if alive := s.Managers["dr2"]; alive {
		t.Fatal("manager dr2 should be dead")
	}
	if len(s.Faults) != 1 || !s.Faults[0].Active {
		t.Fatalf("faults: %+v", s.Faults)
	}
	if len(s.Events) != 2 { // actor_event + invariant_violated
		t.Fatalf("events: %+v", s.Events)
	}
	if s.Run.Violations != 1 {
		t.Fatalf("violations = %d", s.Run.Violations)
	}
}

func TestObjectUpdatesReplaceNotAppend(t *testing.T) {
	h := New()
	o := ObjectState{Cluster: "hub", Kind: "DRPC", Namespace: "ramen-ops", Name: "a",
		Fields: map[string]string{"phase": "Deployed"}}
	h.ObserveObject(o)
	o.Fields = map[string]string{"phase": "FailingOver"}
	h.ObserveObject(o)
	s := h.Snapshot()
	if len(s.Objects) != 1 || s.Objects[0].Fields["phase"] != "FailingOver" {
		t.Fatalf("objects: %+v", s.Objects)
	}
}

func TestSubscribeReceivesEventsAndDropsWhenFull(t *testing.T) {
	h := New()
	ch, cancel := h.Subscribe()
	defer cancel()

	h.ScenarioStart("s1")
	select {
	case ev := <-ch:
		if ev.Type != "test_started" || ev.Data["id"] != "s1" {
			t.Fatalf("event: %+v", ev)
		}
		if ev.Seq == 0 {
			t.Fatal("seq must be monotonic from 1")
		}
	case <-time.After(time.Second):
		t.Fatal("no event")
	}

	// Fill the buffer without draining: producers must never block.
	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			h.ObserveActorEvent("spam")
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("producer blocked on slow subscriber")
	}
}

func TestSnapshotIsACopy(t *testing.T) {
	h := New()
	h.ScenarioStart("s1")
	s := h.Snapshot()
	s.Tests[0].Status = TestFailed // mutate the copy
	if h.Snapshot().Tests[0].Status != TestRunning {
		t.Fatal("snapshot aliases hub state")
	}
}

func TestZeroTimeFieldsOmittedInJSON(t *testing.T) {
	// Verify that zero time.Time fields with omitzero tags are absent from JSON,
	// not serialized as "0001-01-01T00:00:00Z".
	var data []byte
	var err error

	// TestInfo with zero StartedAt/FinishedAt should omit those fields.
	ti := TestInfo{ID: "s1", Status: TestRunning}
	data, err = json.Marshal(ti)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte("startedAt")) || bytes.Contains(data, []byte("finishedAt")) {
		t.Fatalf("zero TestInfo times not omitted: %s", string(data))
	}

	// Segment with zero End should omit the end field.
	seg := Segment{Value: "primary", Start: time.Now()}
	data, err = json.Marshal(seg)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(data, []byte(`"end"`)) {
		t.Fatalf("zero Segment.End not omitted: %s", string(data))
	}
}
