// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
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
