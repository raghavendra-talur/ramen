// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// fakeController is a process table: listed pids are alive with the given
// command. Pids in ignoreTerm survive SIGTERM; SIGKILL always kills. A pid
// stopped by SIGTERM also takes its children with it, like the supervisor
// stopping its managers.
type fakeController struct {
	mu         sync.Mutex
	procs      fakeProcs
	ignoreTerm map[int]bool
	children   map[int][]int
	sent       []string
}

func (f *fakeController) Alive(pid int) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.procs.Alive(pid)
}

func (f *fakeController) Command(pid int) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.procs.Command(pid)
}

func (f *fakeController) Signal(pid int, sig syscall.Signal) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.sent = append(f.sent, fmt.Sprintf("%s %d", sigName(sig), pid))

	if sig == syscall.SIGKILL {
		delete(f.procs, pid)
	} else if !f.ignoreTerm[pid] {
		delete(f.procs, pid)

		for _, child := range f.children[pid] {
			delete(f.procs, child)
		}
	}

	return nil
}

func testStopper(procs *fakeController, out *bytes.Buffer) stopper {
	return stopper{bin: testBin, procs: procs, out: out, timeout: 20 * time.Millisecond, poll: time.Millisecond}
}

func writeTestState(t *testing.T, st *supervisorState) string {
	t.Helper()

	path := filepath.Join(t.TempDir(), "state.json")
	if err := writeState(path, st); err != nil {
		t.Fatal(err)
	}

	return path
}

func testState() *supervisorState {
	return &supervisorState{
		PID: 100,
		Managers: []managerState{
			{Cluster: "hub", Reconciler: "drpc", PID: 101, HealthAddr: "127.0.0.1:9412"},
			{Cluster: "dr1", Reconciler: "vrg", PID: 102, HealthAddr: "127.0.0.1:9424"},
		},
	}
}

func TestStopNoStateFile(t *testing.T) {
	var out bytes.Buffer

	procs := &fakeController{procs: fakeProcs{}}

	err := testStopper(procs, &out).stop(filepath.Join(t.TempDir(), "state.json"))
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out.String(), "nothing running (no state file") || len(procs.sent) != 0 {
		t.Fatalf("out=%q sent=%v", out.String(), procs.sent)
	}
}

func TestStopNothingRunning(t *testing.T) {
	var out bytes.Buffer

	// pid 100 and 101 were reused by unrelated programs.
	procs := &fakeController{procs: fakeProcs{100: "/usr/bin/vim", 101: "/bin/sleep 9"}}

	if err := testStopper(procs, &out).stop(writeTestState(t, testState())); err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(out.String(), "nothing running (supervisor pid 100") || len(procs.sent) != 0 {
		t.Fatalf("out=%q sent=%v", out.String(), procs.sent)
	}
}

func TestStopSupervisorStopsManagers(t *testing.T) {
	var out bytes.Buffer

	procs := &fakeController{
		procs: fakeProcs{
			100: "/tmp/go-build/exe/localrun run",
			101: managerCmd("127.0.0.1:9412"),
		},
		children: map[int][]int{100: {101}},
	}

	if err := testStopper(procs, &out).stop(writeTestState(t, testState())); err != nil {
		t.Fatal(err)
	}

	want := []string{"SIGTERM 100"}
	if !reflect.DeepEqual(procs.sent, want) {
		t.Fatalf("sent %v, want %v\n%s", procs.sent, want, out.String())
	}
}

func TestStopOrphanedManagers(t *testing.T) {
	var out bytes.Buffer

	// Supervisor gone (e.g. SIGKILLed); one manager left, one ignores SIGTERM.
	procs := &fakeController{
		procs: fakeProcs{
			101: managerCmd("127.0.0.1:9412"),
			102: managerCmd("127.0.0.1:9424"),
		},
		ignoreTerm: map[int]bool{102: true},
	}

	if err := testStopper(procs, &out).stop(writeTestState(t, testState())); err != nil {
		t.Fatal(err)
	}

	want := []string{"SIGTERM 101", "SIGTERM 102", "SIGKILL 102"}
	if !reflect.DeepEqual(procs.sent, want) {
		t.Fatalf("sent %v, want %v\n%s", procs.sent, want, out.String())
	}

	for _, line := range []string{
		"sent SIGTERM to manager hub:drpc (pid 101)",
		"manager hub:drpc (pid 101) exited",
		"manager dr1:vrg (pid 102) still running after 20ms",
		"sent SIGKILL to manager dr1:vrg (pid 102)",
	} {
		if !strings.Contains(out.String(), line) {
			t.Errorf("output missing %q:\n%s", line, out.String())
		}
	}
}

func TestStopStuckSupervisor(t *testing.T) {
	var out bytes.Buffer

	procs := &fakeController{
		procs: fakeProcs{
			100: "/usr/local/bin/localrun run",
			101: managerCmd("127.0.0.1:9412"),
		},
		ignoreTerm: map[int]bool{100: true},
	}

	if err := testStopper(procs, &out).stop(writeTestState(t, testState())); err != nil {
		t.Fatal(err)
	}

	want := []string{"SIGTERM 100", "SIGTERM 101", "SIGKILL 100"}
	if !reflect.DeepEqual(procs.sent, want) {
		t.Fatalf("sent %v, want %v\n%s", procs.sent, want, out.String())
	}
}
