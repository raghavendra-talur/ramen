// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ramendr/ramen/simtest/actors"
	"github.com/ramendr/ramen/simtest/ui"
)

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

// New brings up the full world: 3 envtest clusters, S3, bootstrap objects,
// framework actors, and the two ramen operator flavors as subprocesses. The
// returned World's teardown is registered via t.Cleanup, so it is torn down
// when t completes.
func New(t *testing.T) *World {
	t.Helper()
	w := build(t)
	t.Cleanup(w.Teardown)

	return w
}

// build does the actual work of bringing up the world but, unlike New, does
// NOT register a cleanup on t. Callers that need the world to outlive a
// single top-level test (e.g. SharedWorld) must arrange to call Teardown
// themselves.
func build(t *testing.T) *World {
	t.Helper()
	EnsureAssets(t)

	dir := filepath.Join(RepoRoot(), "simtest", ".artifacts",
		fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	w := &World{Dir: dir, procs: map[string]*ManagerProcess{}}

	for _, name := range []string{HubName, DR1Name, DR2Name} {
		c, err := StartCluster(name, dir)
		if err != nil {
			t.Fatalf("start cluster %s: %v", name, err)
		}
		switch name {
		case HubName:
			w.Hub = c
		case DR1Name:
			w.DR1 = c
		case DR2Name:
			w.DR2 = c
		}
	}

	w.S3 = StartS3(S3Bucket(DR1Name), S3Bucket(DR2Name))

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel

	if err := bootstrap(ctx, w.Hub, w.Managed(), w.S3.URL); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	evlog, err := actors.NewEvLog(filepath.Join(dir, "actors.log"))
	if err != nil {
		t.Fatal(err)
	}

	rt, err := actors.Start(ctx, NewScheme(),
		actors.ClusterRef{Name: w.Hub.Name, Cfg: w.Hub.Cfg},
		[]actors.ClusterRef{
			{Name: w.DR1.Name, Cfg: w.DR1.Cfg},
			{Name: w.DR2.Name, Cfg: w.DR2.Cfg},
		}, evlog)
	if err != nil {
		t.Fatalf("start actors: %v", err)
	}
	w.Actors = rt

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

	w.startManager(t, HubName, w.Hub.KubeconfigPath, "dr-hub", "drpolicy,drcluster,drpc")
	w.startManager(t, DR1Name, w.DR1.KubeconfigPath, "dr-cluster", "vrg,drclusterconfig")
	w.startManager(t, DR2Name, w.DR2.KubeconfigPath, "dr-cluster", "vrg,drclusterconfig")

	return w
}

func (w *World) startManager(t *testing.T, name, kubeconfig, ctype, reconcilers string) {
	t.Helper()

	p, err := StartManager(ManagerOpts{
		Name: name, Bin: ManagerBin(), Kubeconfig: kubeconfig, LogDir: w.Dir,
		ControllerType: ctype, Reconcilers: reconcilers,
	})
	if err != nil {
		t.Fatalf("start manager %s: %v", name, err)
	}

	w.procs[name] = p
}

func (w *World) Managed() []*Cluster { return []*Cluster{w.DR1, w.DR2} }

// UIHub returns the live hub or nil; a nil *ui.Hub is a no-op receiver, so
// callers never need to check.
func (w *World) UIHub() *ui.Hub {
	if w.UI == nil {
		return nil
	}
	return w.UI.Hub
}

func (w *World) Cluster(name string) *Cluster {
	switch name {
	case HubName:
		return w.Hub
	case DR1Name:
		return w.DR1
	case DR2Name:
		return w.DR2
	}

	return nil
}

func (w *World) KillManager(name string) error    { return w.procs[name].Kill() }
func (w *World) RestartManager(name string) error { return w.procs[name].Restart() }

// Teardown stops all managers, actors, and clusters owned by the world. It
// is safe to call directly for worlds built via build() that are not wired
// to t.Cleanup (e.g. the shared world; see StopShared).
func (w *World) Teardown() {
	if w.UI != nil {
		w.UI.Close()
	}
	for _, p := range w.procs {
		p.Stop()
	}
	if w.cancel != nil {
		w.cancel()
	}
	if w.S3 != nil {
		w.S3.Stop()
	}
	for _, c := range []*Cluster{w.DR1, w.DR2, w.Hub} {
		if c != nil {
			_ = c.Stop()
		}
	}
}

var (
	sharedMu sync.Mutex
	shared   *World
)

// SharedWorld returns a world shared by the whole test binary: the world is
// built at most once (lazily, on the first call) and is NOT torn down when
// the calling test's t completes — unlike New, it does not register a
// t.Cleanup. This is required because package-level test binaries run
// multiple independent top-level Test* functions against the same shared
// world, and t.Cleanup on the first caller's t would tear the world down as
// soon as that first top-level test finished, leaving a dead world for
// later sibling tests.
//
// The caller is responsible for arranging a single call to StopShared once
// all tests that might use the shared world have finished — normally from
// TestMain, after m.Run() returns. See tests/main_test.go for the contract.
func SharedWorld(t *testing.T) *World {
	sharedMu.Lock()
	defer sharedMu.Unlock()

	if shared == nil {
		shared = build(t)
	}

	return shared
}

// StopShared tears down the world created by SharedWorld, if any, and clears
// the singleton so a subsequent SharedWorld call would build a fresh world.
// It must be called from TestMain after m.Run() returns, not from any
// individual test's t.Cleanup.
func StopShared() {
	sharedMu.Lock()
	defer sharedMu.Unlock()

	if shared != nil {
		shared.Teardown()
		shared = nil
	}
}

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
