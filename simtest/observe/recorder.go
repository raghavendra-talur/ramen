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
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/simtest/world"
)

const (
	// watchReconnectDelay is how long the watch goroutine waits before
	// re-establishing a watch after the result channel closes (or after a
	// failed re-establish attempt).
	watchReconnectDelay = 500 * time.Millisecond

	// maxWatchReconnectAttempts caps consecutive failed re-establish
	// attempts before the recorder gives up and marks itself dead.
	maxWatchReconnectAttempts = 30
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
	dead         string              // set by the watch goroutine when it gives up; see wait()
	stop         context.CancelFunc
	stopOnce     sync.Once
	done         chan struct{}
}

func NewRecorder(ctx context.Context, cfg *rest.Config, ns, name string) (*Recorder, error) {
	wc, err := client.NewWithWatch(cfg, client.Options{Scheme: world.NewScheme()})
	if err != nil {
		return nil, err
	}

	wctx, cancel := context.WithCancel(ctx)
	r := &Recorder{hooks: map[string][]func(){}, stop: cancel, done: make(chan struct{})}

	opts := []client.ListOption{
		client.InNamespace(ns),
		client.MatchingFieldsSelector{Selector: fields.OneTermEqualSelector("metadata.name", name)},
	}

	wi, err := wc.Watch(wctx, &rmn.DRPlacementControlList{}, opts...)
	if err != nil {
		cancel()

		return nil, err
	}

	go r.watchLoop(wctx, wc, opts, wi)

	return r, nil
}

// watchLoop drains events from wi until its result channel closes, then
// re-establishes the watch and keeps going. This makes the recorder
// resilient to watch drops (e.g. apiserver restarts during long-running
// crash/recovery tests). On re-establish, the new watch replays the current
// object state (an empty ResourceVersion); record() dedupes consecutive
// identical values, so replay is harmless. If re-establishing fails
// repeatedly (maxWatchReconnectAttempts consecutive failures), the recorder
// marks itself dead and the goroutine exits; wait() then fails fast instead
// of timing out silently.
func (r *Recorder) watchLoop(wctx context.Context, wc client.WithWatch, opts []client.ListOption, wi watch.Interface) {
	defer close(r.done)

	for {
		r.drain(wctx, wi)
		wi.Stop()

		if wctx.Err() != nil {
			return
		}

		var err error

		wi, err = r.reconnect(wctx, wc, opts)
		if err != nil {
			return
		}
	}
}

// drain reads events from wi until the context is done or the result channel
// closes.
func (r *Recorder) drain(wctx context.Context, wi watch.Interface) {
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
}

// reconnect re-establishes the watch, retrying with a fixed backoff until it
// succeeds or the context is done. After maxWatchReconnectAttempts
// consecutive failures it records the last error in r.dead and returns an
// error so watchLoop can exit.
func (r *Recorder) reconnect(wctx context.Context, wc client.WithWatch, opts []client.ListOption) (watch.Interface, error) {
	for attempt := 1; ; attempt++ {
		select {
		case <-wctx.Done():
			return nil, wctx.Err()
		case <-time.After(watchReconnectDelay):
		}

		wi, err := wc.Watch(wctx, &rmn.DRPlacementControlList{}, opts...)
		if err == nil {
			return wi, nil
		}

		if attempt >= maxWatchReconnectAttempts {
			r.mu.Lock()
			r.dead = fmt.Sprintf("recorder watch failed to reconnect after %d attempts: %v", attempt, err)
			r.mu.Unlock()

			return nil, err
		}
	}
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

	// Hooks run on their own goroutine so a hook doing API calls (e.g.
	// DeleteApp) can't block event delivery on the watch-dispatch goroutine.
	// Hooks must be self-synchronized (t.Errorf and client calls are both
	// safe to call this way).
	for _, f := range fire {
		go f()
	}
}

// OnProgression registers fn to run once the recorder observes progression
// value. If value has already been observed, fn fires immediately. Either
// way, fn runs on its own goroutine (not the watch-dispatch goroutine), so it
// must be self-synchronized.
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
		r.mu.Lock()
		dead := r.dead
		r.mu.Unlock()

		if dead != "" {
			return fmt.Errorf("recorder watch is dead, cannot wait for %s: %s", what, dead)
		}

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

// Stop cancels the watch and waits for the watch goroutine to exit. It is
// safe to call more than once (and from multiple goroutines); only the first
// call has effect.
func (r *Recorder) Stop() {
	r.stopOnce.Do(func() {
		r.stop()
		<-r.done
	})
}
