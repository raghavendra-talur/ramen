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
)

type World struct {
	Dir    string
	Hub    *Cluster
	DR1    *Cluster
	DR2    *Cluster
	S3     *S3Server
	Actors *actors.Runtime

	procs  map[string]*ManagerProcess
	cancel context.CancelFunc
}

// New brings up the full world: 3 envtest clusters, S3, bootstrap objects,
// framework actors, and the two ramen operator flavors as subprocesses.
func New(t *testing.T) *World {
	t.Helper()
	EnsureAssets(t)

	dir := filepath.Join(RepoRoot(), "simtest", ".artifacts",
		fmt.Sprintf("%s-%d", t.Name(), time.Now().UnixNano()))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}

	w := &World{Dir: dir, procs: map[string]*ManagerProcess{}}
	t.Cleanup(w.teardown)

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

func (w *World) teardown() {
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

// SharedWorld returns a world shared by the whole test binary. The first
// caller's t owns the cleanup, so it must be created from TestMain-driven
// code paths that outlive individual subtests — tests/main_test.go does this.
func SharedWorld(t *testing.T) *World {
	sharedMu.Lock()
	defer sharedMu.Unlock()

	if shared == nil {
		shared = New(t)
	}

	return shared
}
