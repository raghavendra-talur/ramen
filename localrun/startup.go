// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	// startupPoll is how often the supervisor probes a starting manager.
	startupPoll = 250 * time.Millisecond

	// startupTimeout bounds the wait for one manager before starting the
	// next, so a manager that never becomes ready cannot stall the rest.
	startupTimeout = 2 * time.Minute
)

// startInOrder starts a cluster's managers one at a time. At startup every
// manager rewrites the cluster's ramen config ConfigMap without retrying on
// conflict, so managers started together on one cluster make each other exit.
// start must not block; it runs the manager and calls settled once the
// manager is past startup (ready or exited). startInOrder waits for that
// before starting the next, and stops starting managers when ctx is done.
func startInOrder(ctx context.Context, recs []string, start func(rec string, settled func())) {
	for _, rec := range recs {
		done := make(chan struct{})

		var once sync.Once

		start(rec, func() { once.Do(func() { close(done) }) })

		select {
		case <-done:
		case <-ctx.Done():
			return
		}
	}
}

// waitStarted calls settled when the manager at addr reports ready, exits
// (exited is closed), ctx is done, or timeout elapses.
func waitStarted(
	ctx context.Context,
	p readinessProber,
	addr string,
	exited <-chan struct{},
	timeout time.Duration,
	settled func(),
) {
	defer settled()

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	tick := time.NewTicker(startupPoll)
	defer tick.Stop()

	for {
		if p.Ready(ctx, addr) == nil {
			return
		}

		select {
		case <-exited:
			return
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// managersEnded reports how the supervised managers ended: stopping because
// the supervisor was asked to stop is success; stopping on their own is not.
func managersEnded(ctx context.Context) error {
	if ctx.Err() == nil {
		return errors.New("all managers exited")
	}

	return nil
}
