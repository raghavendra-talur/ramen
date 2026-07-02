// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
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
