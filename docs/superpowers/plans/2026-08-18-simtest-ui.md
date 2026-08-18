# simtest Live UI Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A read-only web UI served by the simtest process during a matrix run: test-progress rail, animated world stage, object inspector, per-scenario state timelines, fed by a non-blocking status hub over SSE.

**Architecture:** New stdlib-only package `simtest/ui` containing a mutex-guarded status hub (single source of truth), an HTTP server (`/`, `/api/snapshot`, `/api/stream` SSE), watch-based cluster observers, and one embedded vanilla-JS page. Existing simtest components get one-line callback hooks. A nil `*Hub` is a no-op, so hooks are called unconditionally.

**Tech Stack:** Go stdlib (`net/http`, `embed`, `encoding/json`), controller-runtime clients already in simtest's go.mod, vanilla HTML/CSS/JS (no build step).

**Spec:** `docs/superpowers/specs/2026-08-18-simtest-ui-design.md`

## Global Constraints

- Work happens in the `simtest/` Go module; run all commands from `simtest/`.
- No new entries in `simtest/go.mod`. Stdlib + already-present deps only.
- Every Go file starts with the SPDX header:
  ```go
  // SPDX-FileCopyrightText: The RamenDR authors
  // SPDX-License-Identifier: Apache-2.0
  ```
- Every hub method must be safe on a nil receiver and must never block on a slow consumer (subscriber channels drop when full).
- No file outside `simtest/` and `docs/` may be modified.
- Commit style: small logical units, `git commit -s`, end message body with `Assisted-by: Claude Code/claude-fable-5`. No `Co-Authored-By`.
- Verify each task with `go test ./ui/... ./actors/... ./invariants/...` (fast; no envtest needed except where a task says otherwise) plus `go vet ./...`.

---

### Task 1: Hub model, mutators, and subscriptions

**Files:**
- Create: `simtest/ui/model.go`
- Create: `simtest/ui/hub.go`
- Test: `simtest/ui/hub_test.go`

**Interfaces:**
- Consumes: nothing (foundation task).
- Produces (used by every later task):
  - Types `TestInfo`, `ObjectState`, `Fault`, `Event`, `Segment`, `Timeline`, `Snapshot`, `RunInfo`, `TestStatus` (constants `TestQueued`, `TestRunning`, `TestPassed`, `TestFailed`).
  - `func New() *Hub`
  - `func (h *Hub) RunStart(name string)`
  - `func (h *Hub) ScenarioStart(id string)` — unknown ids are appended lazily.
  - `func (h *Hub) ScenarioEnd(id, result, reason string)` — result is `"passed"` or `"failed"`.
  - `func (h *Hub) ObserveObject(o ObjectState)`
  - `func (h *Hub) ObserveManager(name string, alive bool)`
  - `func (h *Hub) ObserveFault(key, desc string, active bool)`
  - `func (h *Hub) ObserveActorEvent(line string)`
  - `func (h *Hub) ObserveInvariant(v string)`
  - `func (h *Hub) Snapshot() Snapshot`
  - `func (h *Hub) Subscribe() (<-chan Event, func())` — buffered 256, drops events when full; the returned func unsubscribes.
  - Event `Type` strings, verbatim: `run_started`, `test_started`, `test_finished`, `state_changed`, `actor_event`, `fault_changed`, `manager_changed`, `invariant_violated`.

- [ ] **Step 1: Write the failing tests**

Create `simtest/ui/hub_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd simtest && go test ./ui/ -run 'TestNilHub|TestScenario|TestObserv|TestObject|TestSubscribe|TestSnapshot' -v`
Expected: FAIL to build — `undefined: Hub`, `undefined: New`, etc.

- [ ] **Step 3: Write the model**

Create `simtest/ui/model.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package ui is simtest's read-only live observability layer: a status hub
// fed by the world's producers, an SSE web server, and one embedded page.
// The UI must never influence a test: every hub method is safe on a nil
// receiver and never blocks on a slow consumer.
package ui

import "time"

type TestStatus string

const (
	TestQueued  TestStatus = "queued"
	TestRunning TestStatus = "running"
	TestPassed  TestStatus = "passed"
	TestFailed  TestStatus = "failed"
)

// TestInfo is one scenario row in the TESTS rail. ID is the full subtest
// name (e.g. "failover/at=wrr/fault=s3-down"); the frontend groups rows by
// the "at=..." element.
type TestInfo struct {
	ID         string     `json:"id"`
	Status     TestStatus `json:"status"`
	Reason     string     `json:"reason,omitempty"`
	StartedAt  time.Time  `json:"startedAt,omitempty"`
	FinishedAt time.Time  `json:"finishedAt,omitempty"`
}

// RunInfo summarizes the whole run for the top-left panel.
type RunInfo struct {
	Name       string    `json:"name"`
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
	End   time.Time `json:"end,omitempty"`
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
```

- [ ] **Step 4: Write the hub**

Create `simtest/ui/hub.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"sort"
	"sync"
	"time"
)

const (
	maxEvents  = 256 // ring buffer shown in RECENT EVENTS
	subBuffer  = 256 // per-subscriber channel depth; overflow drops
)

// Hub is the single source of truth for the UI. All methods are safe on a
// nil receiver (UI disabled) and never block: subscribers with full
// channels lose events (they recover via /api/snapshot on reconnect).
type Hub struct {
	mu        sync.Mutex
	seq       uint64
	run       RunInfo
	current   string
	order     []string // test ids in first-seen order
	tests     map[string]*TestInfo
	objects   map[string]ObjectState
	managers  map[string]bool
	faults    map[string]Fault
	events    []Event
	timelines map[string]Timeline
	subs      map[int]chan Event
	nextSub   int
}

func New() *Hub {
	return &Hub{
		tests:     map[string]*TestInfo{},
		objects:   map[string]ObjectState{},
		managers:  map[string]bool{},
		faults:    map[string]Fault{},
		timelines: map[string]Timeline{},
		subs:      map[int]chan Event{},
	}
}

// emit assigns a sequence number, applies bookkeeping shared by all events,
// and fans out without blocking. Callers hold h.mu.
func (h *Hub) emit(typ string, data map[string]string) {
	h.seq++
	ev := Event{Seq: h.seq, At: time.Now(), Type: typ, Data: data}

	if typ == "actor_event" || typ == "invariant_violated" {
		h.events = append(h.events, ev)
		if len(h.events) > maxEvents {
			h.events = h.events[len(h.events)-maxEvents:]
		}
	}

	for _, ch := range h.subs {
		select {
		case ch <- ev:
		default: // slow subscriber: drop, never block
		}
	}
}

func (h *Hub) RunStart(name string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.run = RunInfo{Name: name, StartedAt: time.Now()}
	h.emit("run_started", map[string]string{"name": name})
}

func (h *Hub) ScenarioStart(id string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ti, ok := h.tests[id]
	if !ok {
		ti = &TestInfo{ID: id}
		h.tests[id] = ti
		h.order = append(h.order, id)
	}
	ti.Status = TestRunning
	ti.StartedAt = time.Now()
	h.current = id
	if h.timelines[id] == nil {
		h.timelines[id] = Timeline{}
	}
	h.emit("test_started", map[string]string{"id": id})
}

func (h *Hub) ScenarioEnd(id, result, reason string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	ti, ok := h.tests[id]
	if !ok {
		return
	}
	ti.Reason = reason
	ti.FinishedAt = time.Now()
	if result == "passed" {
		ti.Status = TestPassed
		h.run.Passed++
	} else {
		ti.Status = TestFailed
		h.run.Failed++
	}
	if h.current == id {
		h.current = ""
	}
	closeOpenSegments(h.timelines[id], ti.FinishedAt)
	h.emit("test_finished", map[string]string{"id": id, "result": result, "reason": reason})
}

func (h *Hub) ObserveObject(o ObjectState) {
	if h == nil {
		return
	}
	if o.At.IsZero() {
		o.At = time.Now()
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.objects[o.key()] = o
	h.recordTracks(o)
	d := map[string]string{"cluster": o.Cluster, "kind": o.Kind,
		"namespace": o.Namespace, "name": o.Name}
	for k, v := range o.Fields {
		d[k] = v
	}
	h.emit("state_changed", d)
}

func (h *Hub) ObserveManager(name string, alive bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if cur, ok := h.managers[name]; ok && cur == alive {
		return // no change, no event
	}
	h.managers[name] = alive
	v := "down"
	if alive {
		v = "up"
	}
	h.emit("manager_changed", map[string]string{"name": name, "state": v})
}

func (h *Hub) ObserveFault(key, desc string, active bool) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.faults[key] = Fault{Key: key, Desc: desc, Active: active, Since: time.Now()}
	state := "cleared"
	if active {
		state = "active"
	}
	if key == "s3" {
		h.track("s3", state, time.Now())
	}
	h.emit("fault_changed", map[string]string{"key": key, "desc": desc, "state": state})
}

func (h *Hub) ObserveActorEvent(line string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.emit("actor_event", map[string]string{"line": line})
}

func (h *Hub) ObserveInvariant(v string) {
	if h == nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.run.Violations++
	h.track("invariants", v, time.Now())
	h.emit("invariant_violated", map[string]string{"violation": v})
}

func (h *Hub) Snapshot() Snapshot {
	if h == nil {
		return Snapshot{}
	}
	h.mu.Lock()
	defer h.mu.Unlock()

	s := Snapshot{
		Seq:       h.seq,
		Run:       h.run,
		Current:   h.current,
		Managers:  map[string]bool{},
		Timelines: map[string]Timeline{},
	}
	for _, id := range h.order {
		s.Tests = append(s.Tests, *h.tests[id])
	}
	for _, o := range h.objects {
		s.Objects = append(s.Objects, o)
	}
	sort.Slice(s.Objects, func(i, j int) bool { return s.Objects[i].key() < s.Objects[j].key() })
	for k, v := range h.managers {
		s.Managers[k] = v
	}
	for _, f := range h.faults {
		s.Faults = append(s.Faults, f)
	}
	sort.Slice(s.Faults, func(i, j int) bool { return s.Faults[i].Key < s.Faults[j].Key })
	s.Events = append([]Event(nil), h.events...)
	for id, tl := range h.timelines {
		cp := Timeline{}
		for track, segs := range tl {
			cp[track] = append([]Segment(nil), segs...)
		}
		s.Timelines[id] = cp
	}
	return s
}

func (h *Hub) Subscribe() (<-chan Event, func()) {
	if h == nil {
		ch := make(chan Event)
		close(ch)
		return ch, func() {}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	id := h.nextSub
	h.nextSub++
	ch := make(chan Event, subBuffer)
	h.subs[id] = ch
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		delete(h.subs, id)
	}
}
```

Also create the two timeline helpers this file calls, in `simtest/ui/model.go` (append at the end) — Task 2 tests them exhaustively:

```go
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cd simtest && go test ./ui/ -v`
Expected: PASS (all six tests).

- [ ] **Step 6: Vet and commit**

Run: `cd simtest && go vet ./ui/`

```bash
git add simtest/ui/model.go simtest/ui/hub.go simtest/ui/hub_test.go
git commit -s -m "simtest: ui status hub model and mutators

Nil-safe, non-blocking hub: scenario lifecycle, object/manager/fault
observations, bounded event ring, and per-subscriber drop-on-full fanout.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 2: Timeline derivation tests

**Files:**
- Test: `simtest/ui/timeline_test.go`

**Interfaces:**
- Consumes: `Hub`, `appendSegment`, `closeOpenSegments` from Task 1.
- Produces: confidence; no new API. (Implementation already exists in Task 1 — this task pins its edge cases before anything builds on it. If a test exposes a bug, fix `model.go` in this task.)

- [ ] **Step 1: Write the tests**

Create `simtest/ui/timeline_test.go`:

```go
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
```

- [ ] **Step 2: Run the tests**

Run: `cd simtest && go test ./ui/ -run 'TestAppend|TestScenarioTimeline|TestObservationsOutside|TestFinished' -v`
Expected: PASS. If any fail, the bug is in Task 1's helpers — fix `model.go`, re-run all of `go test ./ui/`.

- [ ] **Step 3: Commit**

```bash
git add simtest/ui/timeline_test.go
git commit -s -m "simtest: ui timeline derivation tests

Pin segment close/extend semantics, per-scenario retention, and
between-scenario observation handling.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 3: HTTP server with snapshot and SSE endpoints

**Files:**
- Create: `simtest/ui/server.go`
- Create: `simtest/ui/static/index.html` (placeholder shell; Task 8 replaces it)
- Test: `simtest/ui/server_test.go`

**Interfaces:**
- Consumes: `Hub`, `Snapshot`, `Event`, `Subscribe` from Task 1.
- Produces (used by Tasks 4 and 5):
  - `func Serve(h *Hub, addr string) (*Server, error)` — addr `""` means `127.0.0.1:0` (ephemeral).
  - `type Server struct{ ... }` with `func (s *Server) URL() string` and `func (s *Server) Close()`.
  - Wire format: `GET /api/snapshot` returns `Snapshot` as JSON; `GET /api/stream` is SSE, each event framed as `event: <Type>\ndata: <Event JSON>\n\n`.

- [ ] **Step 1: Write the failing tests**

Create `simtest/ui/server_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"bufio"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (*Hub, *Server) {
	t.Helper()
	h := New()
	s, err := Serve(h, "")
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(s.Close)
	return h, s
}

func TestIndexServes(t *testing.T) {
	_, s := startTestServer(t)
	resp, err := http.Get(s.URL() + "/")
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content-type %q", ct)
	}
}

func TestSnapshotEndpoint(t *testing.T) {
	h, s := startTestServer(t)
	h.RunStart("run")
	h.ScenarioStart("s1")

	resp, err := http.Get(s.URL() + "/api/snapshot")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	defer resp.Body.Close()
	var snap Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Run.Name != "run" || snap.Current != "s1" || len(snap.Tests) != 1 {
		t.Fatalf("snapshot: %+v", snap)
	}
}

func TestStreamDeliversEvents(t *testing.T) {
	h, s := startTestServer(t)

	resp, err := http.Get(s.URL() + "/api/stream")
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type %q", ct)
	}

	// The handler subscribes asynchronously; poll until the event arrives.
	go func() {
		for i := 0; i < 50; i++ {
			h.ScenarioStart("s1")
			time.Sleep(20 * time.Millisecond)
		}
	}()

	r := bufio.NewReader(resp.Body)
	deadline := time.After(5 * time.Second)
	lines := make(chan string, 16)
	go func() {
		for {
			l, err := r.ReadString('\n')
			if err != nil {
				return
			}
			lines <- strings.TrimRight(l, "\n")
		}
	}()

	var sawEventLine, sawDataLine bool
	for !(sawEventLine && sawDataLine) {
		select {
		case l := <-lines:
			if l == "event: test_started" {
				sawEventLine = true
			}
			if strings.HasPrefix(l, "data: ") {
				var ev Event
				if err := json.Unmarshal([]byte(strings.TrimPrefix(l, "data: ")), &ev); err != nil {
					t.Fatalf("bad data line %q: %v", l, err)
				}
				if ev.Type == "test_started" {
					sawDataLine = true
				}
			}
		case <-deadline:
			t.Fatal("no SSE event within deadline")
		}
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd simtest && go test ./ui/ -run 'TestIndex|TestSnapshotEndpoint|TestStream' -v`
Expected: FAIL to build — `undefined: Serve`.

- [ ] **Step 3: Create the placeholder page and the server**

Create `simtest/ui/static/index.html` (Task 8 replaces the body; the marker div ids are stable):

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>simtest</title>
</head>
<body>
<div id="app">simtest ui: placeholder (Task 8 builds the real page)</div>
</body>
</html>
```

Create `simtest/ui/server.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net"
	"net/http"
)

//go:embed static
var staticFS embed.FS

// Server is the read-only observability endpoint. It holds no state of its
// own: everything is read from the Hub per request.
type Server struct {
	hub  *Hub
	ln   net.Listener
	http *http.Server
}

// Serve starts the UI server. addr "" binds 127.0.0.1 on an ephemeral port.
func Serve(h *Hub, addr string) (*Server, error) {
	if addr == "" || addr == "1" { // SIMTEST_UI=1 means "on, pick a port"
		addr = "127.0.0.1:0"
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("ui listen %s: %w", addr, err)
	}

	sub, err := fs.Sub(staticFS, "static")
	if err != nil {
		return nil, fmt.Errorf("ui static fs: %w", err)
	}

	s := &Server{hub: h, ln: ln}
	mux := http.NewServeMux()
	mux.Handle("/", http.FileServer(http.FS(sub)))
	mux.HandleFunc("/api/snapshot", s.snapshot)
	mux.HandleFunc("/api/stream", s.stream)
	s.http = &http.Server{Handler: mux}

	go func() { _ = s.http.Serve(ln) }()

	return s, nil
}

func (s *Server) URL() string { return "http://" + s.ln.Addr().String() }

func (s *Server) Close() { _ = s.http.Close() }

func (s *Server) snapshot(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s.hub.Snapshot())
}

func (s *Server) stream(w http.ResponseWriter, r *http.Request) {
	fl, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")

	ch, cancel := s.hub.Subscribe()
	defer cancel()

	for {
		select {
		case <-r.Context().Done():
			return
		case ev, ok := <-ch:
			if !ok {
				return
			}
			b, err := json.Marshal(ev)
			if err != nil {
				continue
			}
			fmt.Fprintf(w, "event: %s\ndata: %s\n\n", ev.Type, b)
			fl.Flush()
		}
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd simtest && go test ./ui/ -v`
Expected: PASS (Tasks 1–3 tests all green).

- [ ] **Step 5: Vet and commit**

Run: `cd simtest && go vet ./ui/`

```bash
git add simtest/ui/server.go simtest/ui/server_test.go simtest/ui/static/index.html
git commit -s -m "simtest: ui snapshot and SSE endpoints

Stdlib-only server: embedded page at /, full-model JSON at /api/snapshot,
incremental events at /api/stream framed as SSE.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 4: Event persistence (ui-events.jsonl)

**Files:**
- Create: `simtest/ui/persist.go`
- Test: `simtest/ui/persist_test.go`

**Interfaces:**
- Consumes: `Hub.Subscribe`, `Event` from Task 1.
- Produces (used by Task 5): `func StartPersist(h *Hub, path string) (stop func(), err error)` — subscribes to the hub and appends each event as one JSON line; `stop` unsubscribes and closes the file.

- [ ] **Step 1: Write the failing test**

Create `simtest/ui/persist_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPersistWritesEventLines(t *testing.T) {
	h := New()
	path := filepath.Join(t.TempDir(), "ui-events.jsonl")
	stop, err := StartPersist(h, path)
	if err != nil {
		t.Fatalf("start persist: %v", err)
	}

	h.ScenarioStart("s1")
	h.ScenarioEnd("s1", "passed", "")

	// The writer goroutine is async; poll for both lines.
	deadline := time.Now().Add(5 * time.Second)
	var types []string
	for time.Now().Before(deadline) {
		types = types[:0]
		f, err := os.Open(path)
		if err == nil {
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				var ev Event
				if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
					t.Fatalf("bad line %q: %v", sc.Text(), err)
				}
				types = append(types, ev.Type)
			}
			f.Close()
		}
		if len(types) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()

	if len(types) < 2 || types[0] != "test_started" || types[1] != "test_finished" {
		t.Fatalf("persisted types: %v", types)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./ui/ -run TestPersist -v`
Expected: FAIL to build — `undefined: StartPersist`.

- [ ] **Step 3: Implement**

Create `simtest/ui/persist.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"encoding/json"
	"fmt"
	"os"
)

// StartPersist tees the hub's event stream into path, one JSON line per
// event — the future replay format. Writing rides the same drop-on-full
// subscription as browsers, so persistence can never block producers.
func StartPersist(h *Hub, path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("ui persist open: %w", err)
	}

	ch, cancel := h.Subscribe()
	done := make(chan struct{})

	go func() {
		defer close(done)
		enc := json.NewEncoder(f)
		for ev := range ch {
			_ = enc.Encode(ev)
		}
	}()

	return func() {
		cancel()
		// cancel() removes the subscriber but does not close its channel;
		// closing is the hub's prerogative only on... it never closes. So
		// drain by closing the file after the goroutine is released.
		f.Close()
	}, nil
}
```

Note: `cancel()` leaves the writer goroutine blocked on `ch` forever. Fix the subscription contract instead of working around it here — in `hub.go`, make the unsubscribe function close the channel after removing it:

```go
	return ch, func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		if _, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(ch)
		}
	}
```

(`emit` sends while holding `h.mu` and unsubscribe closes while holding `h.mu`, so a send on a closed channel is impossible.) Then simplify `StartPersist`'s stop to:

```go
	return func() {
		cancel()
		<-done
		f.Close()
	}, nil
}
```

Also update the Task 3 `stream` handler comment — its `ev, ok := <-ch` case already handles the closed channel correctly.

- [ ] **Step 4: Run the full package tests**

Run: `cd simtest && go test ./ui/ -v`
Expected: PASS, including Task 1's subscription tests with the changed unsubscribe semantics.

- [ ] **Step 5: Vet and commit**

Run: `cd simtest && go vet ./ui/`

```bash
git add simtest/ui/persist.go simtest/ui/persist_test.go simtest/ui/hub.go
git commit -s -m "simtest: ui event persistence to ui-events.jsonl

Tee the event stream into the artifacts dir via a normal subscription;
unsubscribe now closes the channel so consumers terminate cleanly.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 5: Cluster watchers

**Files:**
- Create: `simtest/ui/watch.go`
- Test: `simtest/ui/watch_test.go`

**Interfaces:**
- Consumes: `Hub.ObserveObject`, `ObjectState` from Task 1.
- Produces (used by Task 6):
  - `type ClusterRef struct{ Name string; Cfg *rest.Config }`
  - `func StartWatches(ctx context.Context, h *Hub, scheme *runtime.Scheme, hub ClusterRef, managed []ClusterRef) error` — watches DRPC on the hub cluster and VRG + PVC on each managed cluster; feeds `ObserveObject`; reconnects on watch channel close; returns only setup errors.

- [ ] **Step 1: Write the failing test**

The test drives the unexported `watchInto` loop with controller-runtime's fake client (which implements `client.WithWatch`), so no envtest is needed.

Create `simtest/ui/watch_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := rmn.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	return s
}

func waitObjects(t *testing.T, h *Hub, want int) []ObjectState {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if objs := h.Snapshot().Objects; len(objs) >= want {
			return objs
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("hub never saw %d objects: %+v", want, h.Snapshot().Objects)
	return nil
}

func TestWatchDRPCFeedsHub(t *testing.T) {
	h := New()
	s := testScheme(t)
	wc := fake.NewClientBuilder().WithScheme(s).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchInto(ctx, h, wc, &rmn.DRPlacementControlList{}, extractDRPC("hub"))

	// Give the watch a moment to establish, then create.
	time.Sleep(100 * time.Millisecond)
	drpc := &rmn.DRPlacementControl{ObjectMeta: metav1.ObjectMeta{
		Namespace: "ramen-ops", Name: "app-drpc"}}
	if err := wc.Create(ctx, drpc); err != nil {
		t.Fatal(err)
	}
	drpc.Status.Phase = rmn.FailingOver
	drpc.Status.Progression = rmn.ProgressionWaitForReadiness
	if err := wc.Status().Update(ctx, drpc); err != nil {
		t.Fatal(err)
	}

	objs := waitObjects(t, h, 1)
	o := objs[0]
	if o.Kind != "DRPlacementControl" || o.Cluster != "hub" || o.Name != "app-drpc" {
		t.Fatalf("object: %+v", o)
	}
	// The status update must eventually be reflected.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		o = h.Snapshot().Objects[0]
		if o.Fields["phase"] == string(rmn.FailingOver) {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if o.Fields["phase"] != string(rmn.FailingOver) ||
		o.Fields["progression"] != string(rmn.ProgressionWaitForReadiness) {
		t.Fatalf("fields: %+v", o.Fields)
	}
}

func TestWatchPVCFeedsHub(t *testing.T) {
	h := New()
	s := testScheme(t)
	wc := fake.NewClientBuilder().WithScheme(s).Build()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watchInto(ctx, h, wc, &corev1.PersistentVolumeClaimList{}, extractPVC("dr1"))

	time.Sleep(100 * time.Millisecond)
	pvc := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
		Namespace: "app", Name: "data-0"}}
	if err := wc.Create(ctx, pvc); err != nil {
		t.Fatal(err)
	}

	objs := waitObjects(t, h, 1)
	if objs[0].Kind != "PersistentVolumeClaim" || objs[0].Cluster != "dr1" {
		t.Fatalf("object: %+v", objs[0])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd simtest && go test ./ui/ -run TestWatch -v`
Expected: FAIL to build — `undefined: watchInto`, `undefined: extractDRPC`, `undefined: extractPVC`.

- [ ] **Step 3: Implement**

Create `simtest/ui/watch.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"fmt"
	"strings"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterRef is the minimum the UI needs to watch a cluster. Kept local so
// the ui package never imports world (world imports ui).
type ClusterRef struct {
	Name string
	Cfg  *rest.Config
}

// StartWatches wires the hub to live cluster state: DRPCs on the hub
// cluster, VRGs and PVCs on each managed cluster.
func StartWatches(ctx context.Context, h *Hub, scheme *runtime.Scheme,
	hub ClusterRef, managed []ClusterRef,
) error {
	hubClient, err := client.NewWithWatch(hub.Cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("ui watch client %s: %w", hub.Name, err)
	}
	go watchInto(ctx, h, hubClient, &rmn.DRPlacementControlList{}, extractDRPC(hub.Name))

	for _, m := range managed {
		mc, err := client.NewWithWatch(m.Cfg, client.Options{Scheme: scheme})
		if err != nil {
			return fmt.Errorf("ui watch client %s: %w", m.Name, err)
		}
		go watchInto(ctx, h, mc, &rmn.VolumeReplicationGroupList{}, extractVRG(m.Name))
		go watchInto(ctx, h, mc, &corev1.PersistentVolumeClaimList{}, extractPVC(m.Name))
	}
	return nil
}

// watchInto runs one watch loop, feeding every event's object through
// extract into the hub. It re-establishes the watch when the server closes
// it and gives up only when ctx is done.
func watchInto(ctx context.Context, h *Hub, wc client.WithWatch,
	list client.ObjectList, extract func(client.Object) (ObjectState, bool),
) {
	for ctx.Err() == nil {
		wi, err := wc.Watch(ctx, list)
		if err != nil {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
				continue
			}
		}
		for ev := range wi.ResultChan() {
			obj, ok := ev.Object.(client.Object)
			if !ok {
				continue
			}
			if o, ok := extract(obj); ok {
				h.ObserveObject(o)
			}
		}
		// channel closed: loop re-establishes the watch
	}
}

func extractDRPC(cluster string) func(client.Object) (ObjectState, bool) {
	return func(obj client.Object) (ObjectState, bool) {
		d, ok := obj.(*rmn.DRPlacementControl)
		if !ok {
			return ObjectState{}, false
		}
		fields := map[string]string{
			"phase":       string(d.Status.Phase),
			"progression": string(d.Status.Progression),
		}
		for _, c := range d.Status.Conditions {
			fields["cond-"+c.Type] = string(c.Status)
		}
		return ObjectState{Cluster: cluster, Kind: "DRPlacementControl",
			Namespace: d.Namespace, Name: d.Name, Fields: fields}, true
	}
}

func extractVRG(cluster string) func(client.Object) (ObjectState, bool) {
	return func(obj client.Object) (ObjectState, bool) {
		v, ok := obj.(*rmn.VolumeReplicationGroup)
		if !ok {
			return ObjectState{}, false
		}
		fields := map[string]string{
			"state": strings.ToLower(string(v.Spec.ReplicationState)),
		}
		for _, c := range v.Status.Conditions {
			fields["cond-"+c.Type] = string(c.Status)
		}
		return ObjectState{Cluster: cluster, Kind: "VolumeReplicationGroup",
			Namespace: v.Namespace, Name: v.Name, Fields: fields}, true
	}
}

func extractPVC(cluster string) func(client.Object) (ObjectState, bool) {
	return func(obj client.Object) (ObjectState, bool) {
		p, ok := obj.(*corev1.PersistentVolumeClaim)
		if !ok {
			return ObjectState{}, false
		}
		return ObjectState{Cluster: cluster, Kind: "PersistentVolumeClaim",
			Namespace: p.Namespace, Name: p.Name,
			Fields: map[string]string{"phase": string(p.Status.Phase)}}, true
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd simtest && go test ./ui/ -v`
Expected: PASS. If the fake client's `Watch` misbehaves on typed lists, check the controller-runtime version in `simtest/go.mod` — v0.19+ fake clients support Watch; adjust the test to `WithStatusSubresource(&rmn.DRPlacementControl{})` on the builder if the status update errors.

- [ ] **Step 5: Vet and commit**

Run: `cd simtest && go vet ./ui/`

```bash
git add simtest/ui/watch.go simtest/ui/watch_test.go
git commit -s -m "simtest: ui cluster watchers

Watch DRPCs on the hub and VRGs/PVCs on managed clusters into the hub,
with reconnect-on-close. Fake-client tests, no envtest.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 6: Launch plumbing and world wiring

**Files:**
- Create: `simtest/ui/launch.go`
- Modify: `simtest/world/world.go` (add `UI` field + `UIHub()` accessor; launch in `build()`; close in `Teardown`; manager poller)
- Test: `simtest/ui/launch_test.go`

**Interfaces:**
- Consumes: `Serve`, `StartPersist`, `StartWatches`, `ClusterRef` from Tasks 3–5; `world.NewScheme`, `World`/`Cluster` fields, `ManagerProcess.Alive()` from existing simtest.
- Produces (used by Tasks 7 and 8):
  - `func Enabled() bool` — true when `SIMTEST_UI` is non-empty.
  - `type Options struct{ Addr, Dir string; Scheme *runtime.Scheme; Hub ClusterRef; Managed []ClusterRef }`
  - `func Launch(ctx context.Context, o Options) (*UI, error)`
  - `type UI struct{ Hub *Hub; ... }` with `func (u *UI) URL() string` and `func (u *UI) Close()`.
  - `func (w *World) UIHub() *ui.Hub` on world — returns nil when the UI is off (nil is a valid no-op hub).

- [ ] **Step 1: Write the failing test**

Create `simtest/ui/launch_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestEnabled(t *testing.T) {
	t.Setenv("SIMTEST_UI", "")
	if Enabled() {
		t.Fatal("enabled with empty SIMTEST_UI")
	}
	t.Setenv("SIMTEST_UI", "1")
	if !Enabled() {
		t.Fatal("not enabled with SIMTEST_UI=1")
	}
}

func TestLaunchServesAndPersists(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No clusters: Launch with zero ClusterRefs still serves and persists.
	u, err := Launch(ctx, Options{Addr: "1", Dir: dir})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	defer u.Close()

	resp, err := http.Get(u.URL() + "/api/snapshot")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	resp.Body.Close()

	u.Hub.ScenarioStart("s1")
	u.Close() // flushes the persist goroutine

	if _, err := os.Stat(filepath.Join(dir, "ui-events.jsonl")); err != nil {
		t.Fatalf("ui-events.jsonl missing: %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./ui/ -run 'TestEnabled|TestLaunch' -v`
Expected: FAIL to build — `undefined: Enabled`, `undefined: Launch`, `undefined: Options`.

- [ ] **Step 3: Implement launch.go**

Create `simtest/ui/launch.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
)

// Enabled reports whether the operator asked for the UI (SIMTEST_UI=1 or
// SIMTEST_UI=<addr>).
func Enabled() bool { return os.Getenv("SIMTEST_UI") != "" }

// Options carries everything Launch needs; the ui package never imports
// world, world passes refs in.
type Options struct {
	Addr    string // "" or "1" for ephemeral, or ":8090" / "host:port"
	Dir     string // artifacts dir for ui-events.jsonl; "" disables persist
	Scheme  *runtime.Scheme
	Hub     ClusterRef
	Managed []ClusterRef
}

// UI bundles the hub and its server for the world to own.
type UI struct {
	Hub *Hub

	srv         *Server
	stopPersist func()
	closeOnce   sync.Once
}

// Launch builds the hub, starts persistence, the server, and (when cluster
// refs are given) the watches. Errors are returned for the caller to log —
// per spec, UI failures must never fail a test.
func Launch(ctx context.Context, o Options) (*UI, error) {
	h := New()
	u := &UI{Hub: h}

	if o.Dir != "" {
		stop, err := StartPersist(h, filepath.Join(o.Dir, "ui-events.jsonl"))
		if err != nil {
			return nil, err
		}
		u.stopPersist = stop
	}

	srv, err := Serve(h, o.Addr)
	if err != nil {
		if u.stopPersist != nil {
			u.stopPersist()
		}
		return nil, err
	}
	u.srv = srv

	if o.Hub.Cfg != nil {
		if err := StartWatches(ctx, h, o.Scheme, o.Hub, o.Managed); err != nil {
			u.Close()
			return nil, err
		}
	}

	return u, nil
}

func (u *UI) URL() string { return u.srv.URL() }

func (u *UI) Close() {
	u.closeOnce.Do(func() {
		if u.srv != nil {
			u.srv.Close()
		}
		if u.stopPersist != nil {
			u.stopPersist()
		}
	})
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd simtest && go test ./ui/ -v`
Expected: PASS.

- [ ] **Step 5: Wire the world**

Modify `simtest/world/world.go`. Add the import `"github.com/ramendr/ramen/simtest/ui"` (and `"fmt"` is already imported). Add the field to the struct:

```go
type World struct {
	Dir    string
	Hub    *Cluster
	DR1    *Cluster
	DR2    *Cluster
	S3     *S3Server
	Actors *actors.Runtime
	UI     *ui.UI // nil unless SIMTEST_UI is set

	procs  map[string]*ManagerProcess
	cancel context.CancelFunc
}
```

Add the accessor next to `Managed()`:

```go
// UIHub returns the live hub or nil; a nil *ui.Hub is a no-op receiver, so
// callers never need to check.
func (w *World) UIHub() *ui.Hub {
	if w.UI == nil {
		return nil
	}
	return w.UI.Hub
}
```

In `build()`, after the actors start (`w.Actors = rt`) and BEFORE the `startManager` calls, insert:

```go
	if ui.Enabled() {
		u, err := ui.Launch(ctx, ui.Options{
			Addr:   os.Getenv("SIMTEST_UI"),
			Dir:    dir,
			Scheme: NewScheme(),
			Hub:    ui.ClusterRef{Name: HubName, Cfg: w.Hub.Cfg},
			Managed: []ui.ClusterRef{
				{Name: DR1Name, Cfg: w.DR1.Cfg},
				{Name: DR2Name, Cfg: w.DR2.Cfg},
			},
		})
		if err != nil {
			// Per spec: UI failures never fail a test.
			fmt.Printf("simtest ui: disabled (launch failed: %v)\n", err)
		} else {
			w.UI = u
			fmt.Printf("simtest ui: %s\n", u.URL())
			go w.pollManagers(ctx)
		}
	}
```

Add the poller at the bottom of the file:

```go
// pollManagers feeds manager subprocess liveness into the UI hub. The hub
// suppresses no-change updates, so a tight-ish interval is cheap.
func (w *World) pollManagers(ctx context.Context) {
	t := time.NewTicker(500 * time.Millisecond)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			for name, p := range w.procs {
				w.UIHub().ObserveManager(name, p.Alive())
			}
		}
	}
}
```

In `Teardown()`, before the managers are stopped, add:

```go
	if w.UI != nil {
		w.UI.Close()
	}
```

- [ ] **Step 6: Compile and run the world's unit tests**

Run: `cd simtest && go build ./... && go vet ./world/ && go test ./world/ -run 'TestRamenConfig|TestPaths' -v`
Expected: builds clean; the fast world unit tests still pass (the envtest-backed world tests need assets and are exercised in Task 9).

- [ ] **Step 7: Commit**

```bash
git add simtest/ui/launch.go simtest/ui/launch_test.go simtest/world/world.go
git commit -s -m "simtest: launch ui from world bring-up

SIMTEST_UI=1 (or =addr) starts the hub, server, persistence, watches, and
a manager-liveness poller; the URL prints at world start and Teardown
closes it. Launch failure logs and disables, never fails a test.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 7: Producer hooks (evlog, policy store, S3, invariants, tests)

**Files:**
- Modify: `simtest/actors/evlog.go` (add `OnLine` callback)
- Modify: `simtest/actors/policy.go` (add `OnChange` callback to `Store`)
- Modify: `simtest/world/s3.go` (add `OnChange` callback to `S3Server`)
- Modify: `simtest/invariants/checker.go` (add `OnViolation` callback)
- Modify: `simtest/world/world.go` (wire evlog/store/s3 callbacks after `Launch`)
- Modify: `simtest/tests/main_test.go` (RunStart + checker wiring in `getWorld`; `uiScenario` helper)
- Modify: `simtest/tests/matrix_test.go` (call `uiScenario` in `runCombo`'s subtest)
- Modify: `simtest/tests/baseline_test.go` (call `uiScenario` in `TestBaselines`' subtests)
- Test: `simtest/actors/evlog_test.go` (create), extend `simtest/actors/policy_test.go`, extend `simtest/invariants/checker_test.go`

**Interfaces:**
- Consumes: `Hub` observe methods from Task 1; `World.UIHub()` from Task 6.
- Produces:
  - `EvLog.OnLine func(string)` — called with each formatted log line.
  - `Store.OnChange func(Key, Policy)` — called after each `Set`.
  - `S3Server.OnChange func(bool)` — called on each `SetDown`.
  - `Checker.OnViolation func(string)` — called once per recorded violation.
  - `func uiScenario(t *testing.T, w *world.World, id string)` in `tests` — marks scenario start and registers end-on-cleanup with pass/fail from `t.Failed()`.

- [ ] **Step 1: Write the failing hook tests**

Create `simtest/actors/evlog_test.go`:

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestEvLogOnLine(t *testing.T) {
	l, err := NewEvLog(filepath.Join(t.TempDir(), "ev.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var got string
	l.OnLine = func(line string) { got = line }
	l.Logf("actor %s did %s", "volrep", "fulfill")

	if !strings.Contains(got, "actor volrep did fulfill") {
		t.Fatalf("OnLine got %q", got)
	}
}
```

Append to `simtest/actors/policy_test.go`:

```go
func TestStoreOnChange(t *testing.T) {
	s := NewStore()

	var gotKey Key
	var gotPolicy Policy
	s.OnChange = func(k Key, p Policy) { gotKey, gotPolicy = k, p }

	s.Set(VolRep("dr1"), Silent{})

	if gotKey != VolRep("dr1") {
		t.Fatalf("OnChange key = %v", gotKey)
	}
	if _, ok := gotPolicy.(Silent); !ok {
		t.Fatalf("OnChange policy = %T", gotPolicy)
	}
}
```

Append to `simtest/invariants/checker_test.go` (this file's existing tests show how a Checker is built in tests; follow the same construction — if they build the struct directly, do the same; the assertion is only about the callback):

```go
func TestCheckerOnViolation(t *testing.T) {
	c := &Checker{}

	var got string
	c.OnViolation = func(v string) { got = v }
	c.addViolation("dual primary")

	if got != "dual primary" {
		t.Fatalf("OnViolation got %q", got)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd simtest && go test ./actors/ ./invariants/ -run 'TestEvLogOnLine|TestStoreOnChange|TestCheckerOnViolation' -v`
Expected: FAIL to build — the callback fields don't exist yet.

- [ ] **Step 3: Add the four callbacks**

In `simtest/actors/evlog.go`, add the field and refactor `Logf` to build the line once:

```go
type EvLog struct {
	mu sync.Mutex
	f  *os.File

	// OnLine, when set, receives every formatted line (the UI hub tee).
	// Called synchronously under the log mutex; keep it fast and never
	// call back into the EvLog.
	OnLine func(string)
}

func (l *EvLog) Logf(format string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	line := fmt.Sprintf("%s "+format,
		append([]any{time.Now().Format(time.RFC3339Nano)}, args...)...)
	fmt.Fprintln(l.f, line)

	if l.OnLine != nil {
		l.OnLine(line)
	}
}
```

(Keep the existing struct fields exactly as they are — only add `OnLine`; the snippet above shows the shape, match the real field names in the file.)

In `simtest/actors/policy.go`, add to `Store` (after its existing fields):

```go
	// OnChange, when set, is notified after every Set (the UI hub tee).
	OnChange func(Key, Policy)
```

and at the end of `Set`, outside any locked section if `Set` locks, add:

```go
	if s.OnChange != nil {
		s.OnChange(k, p)
	}
```

In `simtest/world/s3.go`, add to `S3Server`:

```go
	// OnChange, when set, is notified on every outage toggle.
	OnChange func(down bool)
```

and change `SetDown`:

```go
func (s *S3Server) SetDown(down bool) {
	s.down.Store(down)
	if s.OnChange != nil {
		s.OnChange(down)
	}
}
```

In `simtest/invariants/checker.go`, add to `Checker` (after its existing fields):

```go
	// OnViolation, when set, is notified once per recorded violation.
	OnViolation func(string)
```

and inside `addViolation`, after the violation is appended to the slice (respect the method's existing locking — call the hook after unlocking, or capture it before; do not call it while holding the mutex if `Violations()` shares that mutex):

```go
	if c.OnViolation != nil {
		c.OnViolation(v)
	}
```

- [ ] **Step 4: Run hook tests to verify they pass**

Run: `cd simtest && go test ./actors/ ./invariants/ -v`
Expected: PASS (new hook tests plus all pre-existing tests).

- [ ] **Step 5: Wire producers in world and tests**

In `simtest/world/world.go` `build()`, inside the `ui.Enabled()` block from Task 6, after `w.UI = u`, add (note: this block must move to AFTER `evlog` and `rt` exist — it already is, since Task 6 placed it after `w.Actors = rt`):

```go
			evlog.OnLine = u.Hub.ObserveActorEvent
			rt.Store.OnChange = func(k actors.Key, p actors.Policy) {
				_, isNormal := p.(actors.Normal)
				u.Hub.ObserveFault(k.String(), fmt.Sprintf("%T", p), !isNormal)
			}
			w.S3.OnChange = func(down bool) {
				u.Hub.ObserveFault("s3", "s3 outage", down)
			}
```

In `simtest/tests/main_test.go`, inside `getWorld`'s `worldOnce.Do`, after the checker is started, add:

```go
		sharedW.UIHub().RunStart("simtest")
		sharedC.OnViolation = sharedW.UIHub().ObserveInvariant
```

and add the helper at the bottom of the file:

```go
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
```

In `simtest/tests/matrix_test.go`, in `runCombo`, immediately inside the `t.Run(name, func(t *testing.T) {` closure, add:

```go
		uiScenario(t, w, name)
```

In `simtest/tests/baseline_test.go`, find each `t.Run(` subtest inside `TestBaselines` and add the same one-liner immediately inside the closure, using that subtest's name variable (read the file; the pattern is `uiScenario(t, w, <name expression>)`).

- [ ] **Step 6: Compile everything and run the fast tests**

Run: `cd simtest && go build ./... && go vet ./... && go test ./ui/ ./actors/ ./invariants/ ./world/ -run 'Test' -count=1 2>&1 | tail -20`
Expected: all listed packages PASS (envtest-backed tests in `tests/` are covered by Task 9).

- [ ] **Step 7: Commit**

```bash
git add simtest/actors/evlog.go simtest/actors/evlog_test.go \
        simtest/actors/policy.go simtest/actors/policy_test.go \
        simtest/world/s3.go simtest/invariants/checker.go \
        simtest/invariants/checker_test.go simtest/world/world.go \
        simtest/tests/main_test.go simtest/tests/matrix_test.go \
        simtest/tests/baseline_test.go
git commit -s -m "simtest: feed ui hub from producers

One-line callbacks on evlog, fault store, S3 outage, and the invariant
checker; scenario lifecycle reported from the test drivers. All hooks are
nil-safe no-ops when the UI is off.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 8: The page

**Files:**
- Modify: `simtest/ui/static/index.html` (replace the placeholder with the real page)
- Test: extend `simtest/ui/server_test.go`

**Interfaces:**
- Consumes: the wire format from Task 3 (`/api/snapshot` Snapshot JSON, `/api/stream` SSE) — field names exactly as the `json:` tags in Task 1's model.
- Produces: the page. Element ids `#tests`, `#stage`, `#inspector`, `#timeline`, `#run` are the smoke-test contract.

- [ ] **Step 1: Write the failing test**

Append to `simtest/ui/server_test.go`:

```go
func TestIndexHasAppRegions(t *testing.T) {
	_, s := startTestServer(t)
	resp, err := http.Get(s.URL() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := new(strings.Builder)
	if _, err := io.Copy(body, resp.Body); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{`id="run"`, `id="tests"`, `id="stage"`, `id="inspector"`, `id="timeline"`} {
		if !strings.Contains(body.String(), id) {
			t.Fatalf("index.html missing %s", id)
		}
	}
}
```

(Add `"io"` to the test file's imports.)

- [ ] **Step 2: Run test to verify it fails**

Run: `cd simtest && go test ./ui/ -run TestIndexHasAppRegions -v`
Expected: FAIL — placeholder page has none of the region ids.

- [ ] **Step 3: Write the page**

Replace `simtest/ui/static/index.html` entirely. The layout, palette, and behaviors implement the approved v3 mockup (spec: "Approved visual design"). Structure and script below are complete; keep ids stable:

```html
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>simtest</title>
<style>
  * { box-sizing: border-box; margin: 0; }
  body { background:#0b0e14; color:#e6edf3; font:12px ui-monospace,SFMono-Regular,Menlo,monospace; padding:10px; }
  .cols { display:flex; gap:10px; align-items:stretch; }
  .panel { background:#11151ccc; border:1px solid #2a3140; border-radius:6px; padding:10px; font-size:11px; line-height:1.7; }
  .panel h4 { font-size:9px; letter-spacing:1.5px; color:#7d8590; margin-bottom:4px; }
  .left { width:230px; flex-shrink:0; display:flex; flex-direction:column; gap:10px; }
  .right { width:210px; flex-shrink:0; }
  .kv { display:flex; justify-content:space-between; gap:8px; }
  .kv b { font-weight:600; text-align:right; }
  .ok { color:#2ea043; } .bad { color:#f85149; } .warn { color:#d29922; } .run { color:#58a6ff; }
  .bar { height:6px; background:#21262d; border-radius:3px; overflow:hidden; margin:3px 0 6px; }
  .bar div { height:100%; background:#58a6ff; transition:width .3s; }
  #tests { flex:1; min-height:120px; display:flex; flex-direction:column; }
  #tests .rows { overflow-y:auto; flex:1; }
  .trow { display:flex; gap:6px; align-items:center; padding:2px 4px; border-radius:4px; cursor:pointer; }
  .trow:hover { background:#1c2431; }
  .trow.pinned { outline:1px solid #58a6ff55; }
  .trow .dot { width:9px; height:9px; border-radius:50%; flex-shrink:0; }
  .trow .id { flex:1; overflow:hidden; text-overflow:ellipsis; white-space:nowrap; }
  .trow .why { color:#7d8590; font-size:9px; }
  .d-passed { background:#2ea043; } .d-failed { background:#f85149; }
  .d-running { border:2px solid #58a6ff; border-top-color:transparent; background:none; animation:spin 1s linear infinite; }
  .d-queued { border:1.5px solid #30363d; background:none; }
  @keyframes spin { to { transform:rotate(360deg); } }
  #stage { flex:1; position:relative; min-height:360px; }
  .cluster { position:absolute; width:180px; background:#141a23; border:1.5px solid #3d4657; border-radius:8px; padding:8px; }
  .cluster .name { font-size:11px; font-weight:700; letter-spacing:1px; }
  .cluster .sub { font-size:9px; color:#7d8590; }
  .pvcs { display:flex; gap:5px; margin-top:6px; flex-wrap:wrap; }
  .pvc { width:14px; height:14px; border-radius:3px; background:#21262d; border:1px solid #30363d; transition:background .6s, box-shadow .6s; }
  .pvc.Bound { background:#2ea043; border:none; box-shadow:0 0 5px #2ea04366; }
  .pvc.Pending, .pvc.Lost { background:#f85149; border:none; box-shadow:0 0 8px #f8514999; }
  #s3 { position:absolute; left:50%; top:54%; transform:translate(-50%,-50%); width:90px; text-align:center; background:#11251c; border:1.5px dashed #2ea043; border-radius:8px; padding:6px; font-size:10px; }
  #s3.down { background:#2d0f0f; border-color:#f85149; animation:blink 1.2s infinite; }
  @keyframes blink { 50% { border-color:#7d1a1a; } }
  #timeline { margin-top:10px; }
  .tlrow { display:flex; align-items:center; margin-bottom:4px; }
  .tllabel { width:110px; font-size:9px; color:#7d8590; flex-shrink:0; }
  .tlband { flex:1; display:flex; height:14px; border-radius:2px; overflow:hidden; background:#0e1117; }
  .seg { height:100%; position:relative; }
  .seg span { position:absolute; left:3px; top:1px; font-size:8px; color:#fff9; overflow:hidden; white-space:nowrap; max-width:95%; }
  #events { max-height:110px; overflow-y:auto; font-size:9px; color:#7d8590; }
</style>
</head>
<body>
<div class="cols">
  <div class="left">
    <div class="panel" id="run"></div>
    <div class="panel" id="tests"><h4>TESTS</h4><div class="rows"></div></div>
    <div class="panel" id="faults"></div>
  </div>
  <div id="stage">
    <div class="cluster" id="c-hub" style="left:50%;top:0;transform:translateX(-50%)"></div>
    <div class="cluster" id="c-dr1" style="left:0;bottom:0"></div>
    <div class="cluster" id="c-dr2" style="right:0;bottom:0"></div>
    <div id="s3">S3</div>
  </div>
  <div class="panel right" id="inspector"></div>
</div>
<div class="panel" id="timeline"></div>
<div class="panel" style="margin-top:10px"><h4>RECENT EVENTS</h4><div id="events"></div></div>

<script>
"use strict";
const S = { snap: null, pinned: null }; // pinned: scenario id or null = follow current

const PALETTE = ["#2ea043","#58a6ff","#a371f7","#d29922","#f85149","#8957e5","#1f6feb","#6e40c9"];
const colorOf = (() => { const m = new Map(); return v => {
  if (!v) return "#21262d";
  if (!m.has(v)) m.set(v, PALETTE[m.size % PALETTE.length]);
  return m.get(v);
};})();

const esc = s => String(s ?? "").replace(/[&<>"]/g, c => ({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;"}[c]));

async function refetch() {
  S.snap = await (await fetch("/api/snapshot")).json();
  render();
}

function connect() {
  const es = new EventSource("/api/stream");
  es.onmessage = () => {};       // typed events only
  ["run_started","test_started","test_finished","state_changed","actor_event",
   "fault_changed","manager_changed","invariant_violated"].forEach(t =>
    es.addEventListener(t, () => scheduleRefetch()));
  es.onerror = () => {};         // EventSource auto-reconnects
}

// Events arrive in bursts; coalesce into at most ~5 snapshot fetches/sec.
let fetchTimer = null;
function scheduleRefetch() {
  if (fetchTimer) return;
  fetchTimer = setTimeout(() => { fetchTimer = null; refetch(); }, 200);
}

function scenario() {
  if (!S.snap) return null;
  return S.pinned || S.snap.current || lastFinished();
}
function lastFinished() {
  const t = (S.snap.tests || []).filter(t => t.finishedAt);
  return t.length ? t[t.length - 1].id : null;
}

function render() {
  const s = S.snap; if (!s) return;
  renderRun(s); renderTests(s); renderFaults(s); renderStage(s);
  renderInspector(s); renderTimeline(s); renderEvents(s);
}

function renderRun(s) {
  const total = (s.tests || []).length;
  const done = (s.run.passed || 0) + (s.run.failed || 0);
  document.getElementById("run").innerHTML = `
    <h4>RUN · ${esc(s.run.name || "simtest")}</h4>
    <div class="kv"><span>progress</span><b>${done} / ${total}</b></div>
    <div class="bar"><div style="width:${total ? 100 * done / total : 0}%"></div></div>
    <div class="kv"><span><span class="ok">${s.run.passed || 0}✓</span>
      <span class="bad">${s.run.failed || 0}✗</span>
      <span class="warn">${s.run.violations || 0}⚑</span></span></div>`;
}

function renderTests(s) {
  const rows = document.querySelector("#tests .rows");
  rows.innerHTML = (s.tests || []).map(t => {
    const why = t.status === "failed" ? esc(t.reason) : "";
    const pin = scenario() === t.id ? " pinned" : "";
    return `<div class="trow${pin}" data-id="${esc(t.id)}">
      <span class="dot d-${t.status}"></span>
      <span class="id">${esc(t.id)}</span><span class="why">${why}</span></div>`;
  }).join("");
  rows.querySelectorAll(".trow").forEach(el =>
    el.onclick = () => { S.pinned = S.pinned === el.dataset.id ? null : el.dataset.id; render(); });
}

function renderFaults(s) {
  const f = (s.faults || []).map(f => `<div class="kv"><span>${esc(f.key)}</span>
    <b class="${f.active ? "bad" : "ok"}">${f.active ? "ACTIVE" : "clear"}</b></div>`).join("");
  const m = Object.entries(s.managers || {}).map(([n, alive]) =>
    `<div class="kv"><span>${esc(n)}</span><b class="${alive ? "ok" : "bad"}">${alive ? "●" : "✗"}</b></div>`).join("");
  document.getElementById("faults").innerHTML = `<h4>FAULTS · MANAGERS</h4>${f}${m}`;
}

function objectsFor(s, cluster, kind) {
  return (s.objects || []).filter(o => o.cluster === cluster && o.kind === kind);
}

function renderStage(s) {
  const drpc = objectsFor(s, "hub", "DRPlacementControl")[0];
  document.getElementById("c-hub").innerHTML = `<div class="name run">HUB</div>` + (drpc ? `
    <div class="sub">${esc(drpc.namespace)}/${esc(drpc.name)}</div>
    <div class="sub">phase <b>${esc(drpc.fields.phase)}</b></div>
    <div class="sub">progression ${esc(drpc.fields.progression)}</div>` : `<div class="sub">no DRPC</div>`);

  for (const c of ["dr1", "dr2"]) {
    const vrg = objectsFor(s, c, "VolumeReplicationGroup")[0];
    const pvcs = objectsFor(s, c, "PersistentVolumeClaim");
    document.getElementById("c-" + c).innerHTML = `
      <div class="name">${c.toUpperCase()}</div>
      <div class="sub">VRG: ${vrg ? esc(vrg.fields.state) : "—"}</div>
      <div class="pvcs">${pvcs.map(p =>
        `<div class="pvc ${esc(p.fields.phase)}" title="${esc(p.namespace)}/${esc(p.name)}"></div>`).join("")}</div>`;
  }

  const s3fault = (s.faults || []).find(f => f.key === "s3");
  document.getElementById("s3").className = s3fault && s3fault.active ? "down" : "";
  document.getElementById("s3").textContent = s3fault && s3fault.active ? "S3 · OUTAGE" : "S3";
}

function renderInspector(s) {
  const id = scenario();
  const drpc = objectsFor(s, "hub", "DRPlacementControl")[0];
  const conds = drpc ? Object.entries(drpc.fields)
    .filter(([k]) => k.startsWith("cond-"))
    .map(([k, v]) => `<div class="kv"><span>${esc(k.slice(5))}</span>
      <b class="${v === "True" ? "ok" : "bad"}">${esc(v)}</b></div>`).join("") : "";
  document.getElementById("inspector").innerHTML = `
    <h4>SELECTED · ${esc(id || "—")}</h4>` + (drpc ? `
    <div class="kv"><span>phase</span><b>${esc(drpc.fields.phase)}</b></div>
    <div class="kv"><span>progression</span><b>${esc(drpc.fields.progression)}</b></div>
    <h4 style="margin-top:8px">CONDITIONS</h4>${conds}` : `<div class="sub">no DRPC observed</div>`);
}

function renderTimeline(s) {
  const id = scenario();
  const tl = id && s.timelines ? s.timelines[id] : null;
  const el = document.getElementById("timeline");
  if (!tl) { el.innerHTML = `<h4>STATE TIMELINE</h4><div class="sub">no scenario selected</div>`; return; }

  let t0 = Infinity, t1 = -Infinity;
  for (const segs of Object.values(tl)) for (const g of segs) {
    t0 = Math.min(t0, Date.parse(g.start));
    t1 = Math.max(t1, Date.parse(g.end || new Date().toISOString()));
  }
  const span = Math.max(t1 - t0, 1);
  const order = ["drpc-phase", "drpc-progression", "vrg-dr1", "vrg-dr2", "s3", "invariants"];
  el.innerHTML = `<h4>STATE TIMELINE · ${esc(id)}</h4>` + order.filter(k => tl[k]).map(track => `
    <div class="tlrow"><div class="tllabel">${esc(track)}</div><div class="tlband">` +
    tl[track].map(g => {
      const a = Date.parse(g.start), b = Date.parse(g.end || new Date().toISOString());
      const w = 100 * (b - a) / span, x = 100 * (a - t0) / span;
      return `<div class="seg" style="margin-left:${x}%;width:${w}%;background:${colorOf(g.value)}"
        title="${esc(g.value)}"><span>${esc(g.value)}</span></div>`;
    }).join("") + `</div></div>`).join("");
}

function renderEvents(s) {
  document.getElementById("events").innerHTML = (s.events || []).slice(-40).reverse()
    .map(e => `<div>${esc((e.data && (e.data.line || e.data.violation)) || e.type)}</div>`).join("");
}

refetch();
connect();
</script>
</body>
</html>
```

Note on timeline segments: `margin-left` positions each segment relative to the previous one in a flex row — that double-counts. Use absolute positioning instead: give `.tlband` `position:relative` and each `.seg` `position:absolute; left:${x}%; width:${w}%; top:0;`. Make that correction when writing the file (the CSS block already needs `.tlband { position:relative; }` added).

- [ ] **Step 4: Run tests**

Run: `cd simtest && go test ./ui/ -v`
Expected: PASS including `TestIndexHasAppRegions`.

- [ ] **Step 5: Commit**

```bash
git add simtest/ui/static/index.html simtest/ui/server_test.go
git commit -s -m "simtest: ui page

Vanilla-JS implementation of the approved layout: tests rail, run
summary, faults/managers, world stage with PVC state colors and S3
outage flash, inspector, and per-scenario state timeline. Renders from
/api/snapshot, coalesced refetch on SSE events.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 9: End-to-end smoke and manual verification

**Files:**
- Test: `simtest/tests/ui_smoke_test.go` (create)

**Interfaces:**
- Consumes: everything. No new API.

- [ ] **Step 1: Write the smoke test**

Create `simtest/tests/ui_smoke_test.go` (runs only when envtest assets exist AND SIMTEST_UI is on, mirroring the suite's skip conventions):

```go
// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestUISmoke verifies that a UI-enabled world serves the page and that the
// snapshot reflects live world state. It self-skips unless SIMTEST_UI is
// set, so the default suite run is unaffected.
func TestUISmoke(t *testing.T) {
	w, _ := getWorld(t)
	if w.UI == nil {
		t.Skip("SIMTEST_UI not set; run: SIMTEST_UI=1 go test ./tests/ -run TestUISmoke")
	}

	resp, err := http.Get(w.UI.URL() + "/")
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("index status %d", resp.StatusCode)
	}

	uiScenario(t, w, "ui-smoke")

	// Managers are polled every 500ms; wait for them to appear.
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(w.UI.URL() + "/api/snapshot")
		if err != nil {
			t.Fatalf("get snapshot: %v", err)
		}
		var snap struct {
			Current  string          `json:"current"`
			Managers map[string]bool `json:"managers"`
		}
		err = json.NewDecoder(resp.Body).Decode(&snap)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if snap.Current == "ui-smoke" && len(snap.Managers) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot never converged: %+v", snap)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
```

- [ ] **Step 2: Run the smoke test (needs manager + assets, like the rest of the suite)**

Run: `cd simtest && make manager assets && SIMTEST_UI=1 go test ./tests/ -run TestUISmoke -v -count=1`
Expected: PASS (world bring-up takes ~30s). Without `SIMTEST_UI`: SKIP.

- [ ] **Step 3: Manual verification (human checkpoint)**

Run: `cd simtest && SIMTEST_UI=1 go test ./tests/ -run TestBaselines -v -count=1`, open the printed `simtest ui: http://127.0.0.1:PORT` URL, and check against the approved mockup:
- TESTS rail lists baseline scenarios, spinner on the running one.
- Stage shows hub DRPC phase changing, PVC squares turning green on dr2 during failover.
- Timeline accumulates bands for drpc-phase / drpc-progression / vrg tracks.
- `ui-events.jsonl` exists in the run's `.artifacts/<TestName>-<ts>/` dir.

Report what was seen to the user; do not claim success without having run this.

- [ ] **Step 4: Run the whole non-envtest test surface one last time**

Run: `cd simtest && go vet ./... && go test ./ui/ ./actors/ ./invariants/ ./world/ -count=1 2>&1 | tail -8`
Expected: PASS across the board.

- [ ] **Step 5: Commit**

```bash
git add simtest/tests/ui_smoke_test.go
git commit -s -m "simtest: ui end-to-end smoke test

Opt-in (SIMTEST_UI=1) world-level check: page serves, snapshot converges
on live manager and scenario state.

Assisted-by: Claude Code/claude-fable-5"
```

---

### Task 10: SIMTEST_UI_HOLD and docs

**Files:**
- Modify: `simtest/tests/main_test.go` (hold-after-run)
- Create: `simtest/ui/README.md`

**Interfaces:**
- Consumes: `world.StopShared` (existing), `World.UI` from Task 6.
- Produces: `SIMTEST_UI_HOLD=1` behavior; usage docs.

- [ ] **Step 1: Implement hold-after-run**

In `simtest/tests/main_test.go`, `TestMain` currently runs `m.Run()` then tears down via `world.StopShared()` (read the file for the exact shape). Insert the hold between them:

```go
	code := m.Run()

	if os.Getenv("SIMTEST_UI_HOLD") != "" && sharedW != nil && sharedW.UI != nil {
		fmt.Printf("simtest ui: holding at %s — Ctrl-C to exit\n", sharedW.UI.URL())
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
		<-ctx.Done()
		stop()
	}
```

(Add `"context"`, `"fmt"`, `"os"`, `"os/signal"` to imports as needed; keep the existing teardown call after this block so the world still shuts down cleanly.)

- [ ] **Step 2: Verify hold manually**

Run: `cd simtest && SIMTEST_UI=1 SIMTEST_UI_HOLD=1 go test ./tests/ -run TestUISmoke -v -count=1` — after PASS prints, the process must stay alive serving the final snapshot; Ctrl-C exits it. Then re-run WITHOUT the env vars and confirm the suite exits normally on its own.

- [ ] **Step 3: Write the README**

Create `simtest/ui/README.md`:

```markdown
# simtest live UI

Read-only observability for a simtest run. Design:
`docs/superpowers/specs/2026-08-18-simtest-ui-design.md`.

## Usage

    SIMTEST_UI=1 go test ./tests/ -run TestMatrix -v

The URL prints at world bring-up (`simtest ui: http://127.0.0.1:PORT`).
`SIMTEST_UI=:8090` picks a fixed port. `SIMTEST_UI_HOLD=1` keeps the
server (and final state) up after the run until Ctrl-C.

Every run with the UI on also writes `ui-events.jsonl` into the run's
artifacts dir — the raw event stream, one JSON object per line.

## Guarantees

- Read-only: the UI observes the world and cannot touch it.
- Never affects a test: a nil hub is a no-op, subscribers drop events
  when slow, and a UI launch failure logs and disables instead of
  failing the run.
```

- [ ] **Step 4: Commit**

```bash
git add simtest/tests/main_test.go simtest/ui/README.md
git commit -s -m "simtest: ui hold-after-run and usage docs

SIMTEST_UI_HOLD=1 keeps the server alive after the suite finishes for
post-mortem inspection of the final state.

Assisted-by: Claude Code/claude-fable-5"
```

---

## Plan Self-Review (completed)

- **Spec coverage:** activation/lifecycle → Task 6/10; hub + rules → Task 1; timelines → Tasks 1–2; endpoints/SSE → Task 3; persistence → Task 4; watches → Task 5; producer hooks incl. scenario lifecycle → Task 7; page per approved layout → Task 8; testing pyramid (hub unit / server httptest / smoke) → Tasks 1–5, 9; `SIMTEST_UI_HOLD` → Task 10. Non-goals respected: no control endpoints, no replay viewer, no new deps.
- **Known simplifications (deliberate, spec-conforming):** message-dot edge animations and the "waiting on" fault correlation in the inspector are rendered from data already in the snapshot but kept minimal in Task 8's page; both are frontend-only refinements that need no new Go surface and can iterate after the manual checkpoint.
- **Type consistency:** `UIHub()` naming avoids the `World.Hub` cluster-field collision; event type strings match between hub, server test, page, and persist; `ClusterRef` is defined once in `ui` and consumed by `world`.
```
