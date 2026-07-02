// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package observe

import (
	"context"
	"fmt"
	"sync"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/simtest/world"
)

// Recorder watches a single DRPC and records every distinct progression and
// phase it observes, in order. Gates run against the recorded sequence, so a
// state that flashes by between two waits is still caught (the granularity is
// status updates, the finest available signal).
type Recorder struct {
	mu           sync.Mutex
	progressions []string
	phases       []string
	hooks        map[string][]func() // progression value -> one-shot hooks
	stop         context.CancelFunc
	done         chan struct{}
}

func NewRecorder(ctx context.Context, cfg *rest.Config, ns, name string) (*Recorder, error) {
	wc, err := client.NewWithWatch(cfg, client.Options{Scheme: world.NewScheme()})
	if err != nil {
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)
	r := &Recorder{hooks: map[string][]func(){}, stop: cancel, done: make(chan struct{})}

	list := &rmn.DRPlacementControlList{}
	wi, err := wc.Watch(wctx, list,
		client.InNamespace(ns),
		client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("metadata.name", name)})
	if err != nil {
		cancel()

		return nil, err
	}

	go func() {
		defer close(r.done)
		defer wi.Stop()

		for {
			select {
			case <-wctx.Done():
				return
			case ev, ok := <-wi.ResultChan():
				if !ok {
					return
				}
				drpc, isDRPC := ev.Object.(*rmn.DRPlacementControl)
				if !isDRPC {
					continue
				}
				r.record(string(drpc.Status.Progression), string(drpc.Status.Phase))
			}
		}
	}()

	return r, nil
}

func (r *Recorder) record(progression, phase string) {
	r.mu.Lock()

	var fire []func()

	if progression != "" && (len(r.progressions) == 0 || r.progressions[len(r.progressions)-1] != progression) {
		r.progressions = append(r.progressions, progression)
		fire = r.hooks[progression]
		delete(r.hooks, progression)
	}
	if phase != "" && (len(r.phases) == 0 || r.phases[len(r.phases)-1] != phase) {
		r.phases = append(r.phases, phase)
	}
	r.mu.Unlock()

	for _, f := range fire {
		f()
	}
}

func (r *Recorder) OnProgression(value string, fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for _, p := range r.progressions { // already seen? fire immediately
		if p == value {
			go fn()

			return
		}
	}
	r.hooks[value] = append(r.hooks[value], fn)
}

func (r *Recorder) seen(list []string, v string) bool {
	for _, s := range list {
		if s == v {
			return true
		}
	}

	return false
}

func (r *Recorder) WaitProgression(v string, timeout time.Duration) error {
	return r.wait(func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.seen(r.progressions, v) },
		timeout, "progression "+v)
}

func (r *Recorder) WaitPhase(v string, timeout time.Duration) error {
	return r.wait(func() bool { r.mu.Lock(); defer r.mu.Unlock(); return r.seen(r.phases, v) },
		timeout, "phase "+v)
}

func (r *Recorder) wait(cond func() bool, timeout time.Duration, what string) error {
	deadline := time.Now().Add(Scale(timeout))
	for time.Now().Before(deadline) {
		if cond() {
			return nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	return fmt.Errorf("timed out waiting for %s; progressions=%v phases=%v", what, r.progressions, r.phases)
}

func (r *Recorder) Progressions() []string {
	r.mu.Lock()
	defer r.mu.Unlock()

	out := make([]string, len(r.progressions))
	copy(out, r.progressions)

	return out
}

func (r *Recorder) Stop() { r.stop(); <-r.done }
