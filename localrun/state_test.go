// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestWriteReadStateRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	want := &supervisorState{
		PID:         123,
		StartedAt:   "2026-10-09T10:00:00Z",
		Envfile:     "/abs/regional-dr.yaml",
		Env:         "rdr",
		Reconcilers: []string{"vrg"},
		Managers: []managerState{{
			Cluster: "dr1", Reconciler: "vrg", PID: 456,
			HealthAddr: "127.0.0.1:9424", MetricsAddr: "127.0.0.1:9324",
			Log: "/abs/logs/dr1-vrg.log", StartedAt: "2026-10-09T10:00:01Z",
		}},
	}

	if err := writeState(path, want); err != nil {
		t.Fatal(err)
	}

	got, err := readState(path)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v, want %+v", got, want)
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 1 {
		t.Fatalf("expected only state.json in dir, got %d entries", len(entries))
	}
}

func TestStateJSONFieldNames(t *testing.T) {
	data, err := json.Marshal(supervisorState{Managers: []managerState{{}}})
	if err != nil {
		t.Fatal(err)
	}

	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}

	assertKeys(t, raw, "pid", "startedAt", "stoppedAt", "envfile", "env", "reconcilers", "managers")

	managers, ok := raw["managers"].([]any)
	if !ok || len(managers) != 1 {
		t.Fatalf("managers: %v", raw["managers"])
	}

	m, ok := managers[0].(map[string]any)
	if !ok {
		t.Fatalf("manager: %v", managers[0])
	}

	assertKeys(t, m, "cluster", "reconciler", "pid", "healthAddr", "metricsAddr", "log",
		"startedAt", "exitedAt", "exitErr")
}

func assertKeys(t *testing.T, m map[string]any, keys ...string) {
	t.Helper()

	if len(m) != len(keys) {
		t.Errorf("got %d keys %v, want %v", len(m), m, keys)
	}

	for _, k := range keys {
		if _, ok := m[k]; !ok {
			t.Errorf("missing key %q in %v", k, m)
		}
	}
}

func TestReadStateMissing(t *testing.T) {
	_, err := readState(filepath.Join(t.TempDir(), "state.json"))
	if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want os.ErrNotExist", err)
	}
}

func TestReadStateCorrupt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	if err := os.WriteFile(path, []byte("{"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := readState(path)
	if err == nil || errors.Is(err, os.ErrNotExist) {
		t.Fatalf("got %v, want parse error", err)
	}
}

func TestStateRecorderInitialState(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	newStateRecorder(path, "/abs/env.yaml", "rdr", nil)

	st := mustReadState(t, path)
	if st.PID != os.Getpid() || st.StartedAt == "" || st.StoppedAt != "" {
		t.Fatalf("initial state: %+v", st)
	}

	if st.Reconcilers == nil || st.Managers == nil {
		t.Fatalf("reconcilers and managers must be empty arrays, not null: %+v", st)
	}
}

func TestStateRecorderLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")

	r := newStateRecorder(path, "/abs/env.yaml", "rdr", nil)
	r.now = func() time.Time { return time.Date(2026, 10, 9, 10, 0, 0, 0, time.UTC) }

	idx := r.managerStarted(managerState{Cluster: "hub", Reconciler: "drpc", PID: 42})
	st := mustReadState(t, path)

	if len(st.Managers) != 1 || st.Managers[0].StartedAt != "2026-10-09T10:00:00Z" {
		t.Fatalf("after start: %+v", st.Managers)
	}

	r.managerExited(idx, errors.New("exit status 1"))
	r.stopped()

	st = mustReadState(t, path)
	m := st.Managers[0]

	if m.ExitedAt != "2026-10-09T10:00:00Z" || m.ExitErr != "exit status 1" {
		t.Fatalf("after exit: %+v", m)
	}

	if st.StoppedAt != "2026-10-09T10:00:00Z" {
		t.Fatalf("after stop: %+v", st)
	}
}

func TestStateRecorderConcurrentUpdates(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	r := newStateRecorder(path, "/abs/env.yaml", "rdr", []string{})

	const n = 20

	var wg sync.WaitGroup

	for i := range n {
		wg.Add(1)

		go func() {
			defer wg.Done()

			idx := r.managerStarted(managerState{PID: i + 1})
			r.managerExited(idx, nil)
		}()
	}

	wg.Wait()

	st := mustReadState(t, path)
	if len(st.Managers) != n {
		t.Fatalf("got %d managers, want %d", len(st.Managers), n)
	}

	for _, m := range st.Managers {
		if m.ExitedAt == "" || m.ExitErr != "" {
			t.Fatalf("manager not marked exited cleanly: %+v", m)
		}
	}
}

func mustReadState(t *testing.T, path string) *supervisorState {
	t.Helper()

	st, err := readState(path)
	if err != nil {
		t.Fatal(err)
	}

	return st
}
