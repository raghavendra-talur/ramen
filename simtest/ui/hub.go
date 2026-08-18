// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"sort"
	"sync"
	"time"
)

const (
	maxEvents = 256 // ring buffer shown in RECENT EVENTS
	subBuffer = 256 // per-subscriber channel depth; overflow drops
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
		if _, ok := h.subs[id]; ok {
			delete(h.subs, id)
			close(ch)
		}
	}
}
