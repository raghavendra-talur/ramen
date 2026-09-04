// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package ui is simtest's read-only live observability layer: a status hub
// fed by the world's producers, an SSE web server, and one embedded page.
// The UI must never influence a test: every hub method is safe on a nil
// receiver and never blocks on a slow consumer.
package ui

import (
	"encoding/json"
	"time"
)

type TestStatus string

const (
	TestQueued  TestStatus = "queued"
	TestRunning TestStatus = "running"
	TestPassed  TestStatus = "passed"
	TestFailed  TestStatus = "failed"
)

// TestInfo is one scenario row in the TESTS rail. ID is the full subtest
// name (e.g. "failover/at=wrr/fault=s3-down"); rows render flat in v1.
// Grouping rows by the "at=..." element is a planned follow-up.
type TestInfo struct {
	ID         string     `json:"id"`
	Status     TestStatus `json:"status"`
	Reason     string     `json:"reason,omitempty"`
	StartedAt  time.Time  `json:"startedAt,omitzero"`
	FinishedAt time.Time  `json:"finishedAt,omitzero"`
}

// RunInfo summarizes the whole run for the top-left panel.
type RunInfo struct {
	Name string `json:"name"`
	// Command is how this run was invoked (the go test line), shown in the
	// header so a viewer can tell which slice of the matrix they watch.
	Command    string    `json:"command,omitzero"`
	StartedAt  time.Time `json:"startedAt"`
	Passed     int       `json:"passed"`
	Failed     int       `json:"failed"`
	Violations int       `json:"violations"`
}

// ObjectState is one watched object's current state, pre-flattened for the
// frontend: Fields holds only what the stage and inspector render.
type ObjectState struct {
	Cluster   string            `json:"cluster"`
	Kind      string            `json:"kind"`
	Namespace string            `json:"namespace"`
	Name      string            `json:"name"`
	Fields    map[string]string `json:"fields"`
	At        time.Time         `json:"at"`
	// Raw is the object's full JSON, kept hub-side for the on-demand
	// /api/object drawer; excluded from the snapshot and the event stream
	// so neither grows with object size.
	Raw json.RawMessage `json:"-"`
}

func (o ObjectState) key() string {
	return o.Cluster + "/" + o.Kind + "/" + o.Namespace + "/" + o.Name
}

// Fault is one fault-injection toggle (policy store key or "s3").
type Fault struct {
	Key    string    `json:"key"`
	Desc   string    `json:"desc"`
	Active bool      `json:"active"`
	Since  time.Time `json:"since"`
}

// Event is one incremental update on the SSE stream; the same encoding is
// persisted to ui-events.jsonl.
type Event struct {
	Seq  uint64            `json:"seq"`
	At   time.Time         `json:"at"`
	Type string            `json:"type"`
	Data map[string]string `json:"data"`
}

// Segment is one band on a timeline track. End is zero while the segment is
// still open.
type Segment struct {
	Value string    `json:"value"`
	Start time.Time `json:"start"`
	End   time.Time `json:"end,omitzero"`
}

// Timeline maps track name (drpc-phase, drpc-progression, vrg-dr1, vrg-dr2,
// s3, invariants) to its segments. One Timeline per scenario.
type Timeline map[string][]Segment

// Snapshot is the hub's full model; a fresh client renders entirely from it.
type Snapshot struct {
	Seq       uint64              `json:"seq"`
	Run       RunInfo             `json:"run"`
	Current   string              `json:"current"` // running scenario id, "" if none
	Tests     []TestInfo          `json:"tests"`
	Objects   []ObjectState       `json:"objects"`
	Managers  map[string]bool     `json:"managers"`
	Faults    []Fault             `json:"faults"`
	Events    []Event             `json:"events"` // bounded ring, oldest first
	Timelines map[string]Timeline `json:"timelines"`
}

// track appends a value change to the CURRENT scenario's timeline. Callers
// hold h.mu.
func (h *Hub) track(name, value string, at time.Time) {
	if h.current == "" {
		return
	}
	appendSegment(h.timelines[h.current], name, value, at)
}

// recordTracks derives timeline tracks from an object observation. Callers
// hold h.mu.
func (h *Hub) recordTracks(o ObjectState) {
	switch o.Kind {
	case "DRPlacementControl":
		h.track("drpc-phase", o.Fields["phase"], o.At)
		h.track("drpc-progression", o.Fields["progression"], o.At)
	case "VolumeReplicationGroup":
		h.track("vrg-"+o.Cluster, o.Fields["state"], o.At)
	}
}

// appendSegment closes the track's open segment (if its value changed) and
// opens a new one. Same-value observations extend the open segment.
func appendSegment(tl Timeline, track, value string, at time.Time) {
	segs := tl[track]
	if n := len(segs); n > 0 && segs[n-1].End.IsZero() {
		if segs[n-1].Value == value {
			return
		}
		segs[n-1].End = at
	}
	tl[track] = append(segs, Segment{Value: value, Start: at})
}

// closeOpenSegments ends every open segment, called when a scenario ends.
func closeOpenSegments(tl Timeline, at time.Time) {
	for track, segs := range tl {
		if n := len(segs); n > 0 && segs[n-1].End.IsZero() {
			segs[n-1].End = at
			tl[track] = segs
		}
	}
}
