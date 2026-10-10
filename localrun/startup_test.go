// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"
)

// TestStartInOrderWaitsForSettled verifies that a manager starts only after
// the previous one on the cluster has settled.
func TestStartInOrderWaitsForSettled(t *testing.T) {
	var (
		mu      sync.Mutex
		started []string
		pending int
	)

	start := func(rec string, settled func()) {
		mu.Lock()
		defer mu.Unlock()

		if pending != 0 {
			t.Errorf("%s started while %d manager(s) had not settled", rec, pending)
		}

		pending++

		started = append(started, rec)

		go func() {
			time.Sleep(10 * time.Millisecond)
			mu.Lock()
			pending--
			mu.Unlock()
			settled()
			settled() // settling twice must be harmless
		}()
	}

	startInOrder(context.Background(), []string{"vrg", "rgs", "rgd"}, start)

	if want := []string{"vrg", "rgs", "rgd"}; !reflect.DeepEqual(started, want) {
		t.Errorf("started = %v, want %v", started, want)
	}
}

// TestStartInOrderStopsOnCancel verifies that no further managers start once
// the supervisor is stopping.
func TestStartInOrderStopsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	var started []string

	start := func(rec string, _ func()) {
		started = append(started, rec)

		cancel() // never settles; the stop signal arrives instead
	}

	startInOrder(ctx, []string{"vrg", "rgs"}, start)

	if want := []string{"vrg"}; !reflect.DeepEqual(started, want) {
		t.Errorf("started = %v, want %v", started, want)
	}
}

func TestWaitStartedReady(t *testing.T) {
	p := &fakeProber{}
	settled := make(chan struct{})

	go waitStarted(context.Background(), p, "127.0.0.1:9411", make(chan struct{}), time.Minute,
		func() { close(settled) })

	select {
	case <-settled:
	case <-time.After(time.Second):
		t.Fatal("not settled after the manager reported ready")
	}
}

func TestWaitStartedExited(t *testing.T) {
	p := &fakeProber{errs: map[string]error{"127.0.0.1:9411": errors.New("connection refused")}}
	exited := make(chan struct{})
	settled := make(chan struct{})

	go waitStarted(context.Background(), p, "127.0.0.1:9411", exited, time.Minute,
		func() { close(settled) })

	close(exited)

	select {
	case <-settled:
	case <-time.After(time.Second):
		t.Fatal("not settled after the manager exited")
	}
}

func TestWaitStartedTimeout(t *testing.T) {
	p := &fakeProber{errs: map[string]error{"127.0.0.1:9411": errors.New("connection refused")}}
	settled := make(chan struct{})

	go waitStarted(context.Background(), p, "127.0.0.1:9411", make(chan struct{}), 50*time.Millisecond,
		func() { close(settled) })

	select {
	case <-settled:
	case <-time.After(time.Second):
		t.Fatal("not settled after the startup timeout")
	}
}

// TestManagersEnded verifies that the supervisor fails when its managers stop
// on their own, and succeeds when they stop because it was asked to stop.
func TestManagersEnded(t *testing.T) {
	if err := managersEnded(context.Background()); err == nil {
		t.Error("managers stopped without a stop signal: want an error, got nil")
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := managersEnded(ctx); err != nil {
		t.Errorf("managers stopped by a stop signal: want nil, got %v", err)
	}
}
