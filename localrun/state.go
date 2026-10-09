// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The JSON shapes in this file are a contract with other programs (the
// ramendev dashboard); do not rename fields.

// supervisorState is the content of state.json, written by the supervisor
// (run or refresh) and read by status and stop.
type supervisorState struct {
	PID         int            `json:"pid"`
	StartedAt   string         `json:"startedAt"`
	StoppedAt   string         `json:"stoppedAt"`
	Envfile     string         `json:"envfile"`
	Env         string         `json:"env"`
	Reconcilers []string       `json:"reconcilers"`
	Managers    []managerState `json:"managers"`
}

// managerState records one manager process started by the supervisor.
type managerState struct {
	Cluster     string `json:"cluster"`
	Reconciler  string `json:"reconciler"`
	PID         int    `json:"pid"`
	HealthAddr  string `json:"healthAddr"`
	MetricsAddr string `json:"metricsAddr"`
	Log         string `json:"log"`
	StartedAt   string `json:"startedAt"`
	ExitedAt    string `json:"exitedAt"`
	ExitErr     string `json:"exitErr"`
}

func stateFilePath(root string) string {
	return filepath.Join(root, "localrun", "state.json")
}

func timestamp(t time.Time) string {
	return t.Format(time.RFC3339)
}

// readState reads a state file. A missing file is reported as an error
// matching os.ErrNotExist.
func readState(path string) (*supervisorState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var st supervisorState
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, fmt.Errorf("cannot parse state file %s: %w", path, err)
	}

	return &st, nil
}

// writeState writes st to path atomically: readers see either the old or
// the new content, never a partial file.
func writeState(path string, st *supervisorState) error {
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), ".state-*.json")
	if err != nil {
		return err
	}

	defer os.Remove(tmp.Name()) //nolint:errcheck // gone after a successful rename

	if _, err := tmp.Write(append(data, '\n')); err != nil {
		tmp.Close()

		return err
	}

	if err := tmp.Close(); err != nil {
		return err
	}

	return os.Rename(tmp.Name(), path)
}

// stateRecorder owns the state file for a running supervisor. All updates go
// through it so concurrent manager goroutines never interleave writes.
type stateRecorder struct {
	mu   sync.Mutex
	path string
	st   supervisorState
	now  func() time.Time
}

// newStateRecorder writes the initial state for this supervisor process.
func newStateRecorder(path, envfile, env string, reconcilers []string) *stateRecorder {
	if reconcilers == nil {
		reconcilers = []string{}
	}

	r := &stateRecorder{path: path, now: time.Now}
	r.st = supervisorState{
		PID:         os.Getpid(),
		Envfile:     envfile,
		Env:         env,
		Reconcilers: reconcilers,
		Managers:    []managerState{},
	}

	r.update(func(st *supervisorState) { st.StartedAt = timestamp(r.now()) })

	return r
}

// update applies fn and persists the result. Write errors are reported but
// do not stop the supervisor: losing visibility must not kill managers.
func (r *stateRecorder) update(fn func(*supervisorState)) {
	r.mu.Lock()
	defer r.mu.Unlock()

	fn(&r.st)

	if err := writeState(r.path, &r.st); err != nil {
		fmt.Fprintf(os.Stderr, "cannot write state file %s: %v\n", r.path, err)
	}
}

// managerStarted records a manager and returns its handle for
// managerExited.
func (r *stateRecorder) managerStarted(m managerState) int {
	var idx int

	r.update(func(st *supervisorState) {
		m.StartedAt = timestamp(r.now())
		st.Managers = append(st.Managers, m)
		idx = len(st.Managers) - 1
	})

	return idx
}

func (r *stateRecorder) managerExited(idx int, exitErr error) {
	r.update(func(st *supervisorState) {
		st.Managers[idx].ExitedAt = timestamp(r.now())
		if exitErr != nil {
			st.Managers[idx].ExitErr = exitErr.Error()
		}
	})
}

func (r *stateRecorder) stopped() {
	r.update(func(st *supervisorState) { st.StoppedAt = timestamp(r.now()) })
}
