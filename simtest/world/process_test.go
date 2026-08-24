// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestManagerProcessLifecycle(t *testing.T) {
	p, err := StartManager(ManagerOpts{
		Name: "fake", Bin: "/bin/sleep", Kubeconfig: "600", LogDir: t.TempDir(),
		ControllerType: "dr-hub", Reconcilers: "drpc",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Stop)

	if !p.Alive() {
		t.Fatal("process should be alive")
	}
	if err := p.Kill(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for p.Alive() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if p.Alive() {
		t.Fatal("process should be dead after Kill")
	}

	if err := p.Restart(); err != nil {
		t.Fatal(err)
	}
	if !p.Alive() {
		t.Fatal("process should be alive after Restart")
	}
}

// TestManagerProcessConcurrentAccess hammers Alive/Kill/Restart from many
// goroutines concurrently, mirroring how callers touch a ManagerProcess
// during crash-recovery scenarios. It is meant to be run with -race.
func TestManagerProcessConcurrentAccess(t *testing.T) {
	p, err := StartManager(ManagerOpts{
		Name: "fake-concurrent", Bin: "/bin/sleep", Kubeconfig: "600", LogDir: t.TempDir(),
		ControllerType: "dr-hub", Reconcilers: "drpc",
	})
	if err != nil {
		t.Fatal(err)
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup

	worker := func(fn func()) {
		defer wg.Done()

		for {
			select {
			case <-stop:
				return
			default:
				fn()
			}
		}
	}

	wg.Add(3)
	go worker(func() { p.Alive() })
	go worker(func() { _ = p.Kill() })
	go worker(func() { _ = p.Restart() })

	time.Sleep(time.Second)
	close(stop)
	wg.Wait()

	p.Stop()
}

// A dead manager must be reported by name so test drivers can abort a combo
// immediately instead of poisoning every later scenario with a half-dead
// world.
func TestManagersAliveReportsDeadManager(t *testing.T) {
	p, err := StartManager(ManagerOpts{
		Name: "fake", Bin: "/bin/sleep", Kubeconfig: "600", LogDir: t.TempDir(),
		ControllerType: "dr-hub", Reconcilers: "drpc",
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Stop)

	w := &World{procs: map[string]*ManagerProcess{"dr1": p}}

	if err := w.ManagersAlive(); err != nil {
		t.Fatalf("all managers alive, expected nil, got: %v", err)
	}

	if err := p.Kill(); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for p.Alive() && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	err = w.ManagersAlive()
	if err == nil {
		t.Fatal("expected error for dead manager")
	}
	if !strings.Contains(err.Error(), "dr1") {
		t.Fatalf("error must name the dead manager, got: %v", err)
	}
}

// The manager takes its options from command-line flags now (leader
// election defaults ON, metrics/probe to fixed ports): simtest must disable
// leader election (no in-cluster identity) and zero the listen addresses
// (three managers share one host).
func TestManagerArgsDisableLeaderElectionAndPorts(t *testing.T) {
	kc := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(kc, []byte("k"), 0o600); err != nil {
		t.Fatal(err)
	}

	args := managerArgs(ManagerOpts{Kubeconfig: kc})

	want := []string{"--kubeconfig=" + kc, "--leader-elect=false", "--metrics-bind-address=0", "--health-probe-bind-address=0"}
	for _, w := range want {
		found := false
		for _, a := range args {
			if a == w {
				found = true
			}
		}
		if !found {
			t.Errorf("args missing %q: %v", w, args)
		}
	}
}
