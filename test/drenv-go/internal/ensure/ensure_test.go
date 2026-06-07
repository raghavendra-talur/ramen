// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"context"
	"errors"
	"testing"
	"time"
)

// fakeStep returns the values in doneSeq on successive Done calls (repeating the
// last value), and records how many times Do/Done were called.
type fakeStep struct {
	name      string
	doneSeq   []bool
	doneErr   error
	doErr     error
	doCalls   int
	doneCalls int
}

func (f *fakeStep) Name() string { return f.name }

func (f *fakeStep) Done(ctx context.Context) (bool, error) {
	i := f.doneCalls
	f.doneCalls++
	if f.doneErr != nil {
		return false, f.doneErr
	}
	switch {
	case i < len(f.doneSeq):
		return f.doneSeq[i], nil
	case len(f.doneSeq) > 0:
		return f.doneSeq[len(f.doneSeq)-1], nil
	default:
		return false, nil
	}
}

func (f *fakeStep) Do(ctx context.Context) error {
	f.doCalls++
	return f.doErr
}

func fastOpts() Options {
	return Options{VerifyTimeout: 50 * time.Millisecond, VerifyInterval: time.Millisecond}
}

func TestEnsureSkipsWhenAlreadyDone(t *testing.T) {
	s := &fakeStep{name: "x", doneSeq: []bool{true}}
	res, err := Ensure(context.Background(), s, fastOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != Skipped {
		t.Fatalf("got %v, want Skipped", res)
	}
	if s.doCalls != 0 {
		t.Fatalf("Do called %d times, want 0", s.doCalls)
	}
}

func TestEnsureActsThenVerifies(t *testing.T) {
	s := &fakeStep{name: "x", doneSeq: []bool{false, true}}
	res, err := Ensure(context.Background(), s, fastOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != Changed {
		t.Fatalf("got %v, want Changed", res)
	}
	if s.doCalls != 1 {
		t.Fatalf("Do called %d times, want 1", s.doCalls)
	}
}

func TestEnsureFailsOnDoError(t *testing.T) {
	s := &fakeStep{name: "x", doneSeq: []bool{false}, doErr: errors.New("boom")}
	res, err := Ensure(context.Background(), s, fastOpts())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}

func TestEnsureFailsOnDoneError(t *testing.T) {
	s := &fakeStep{name: "x", doneErr: errors.New("read failed")}
	res, err := Ensure(context.Background(), s, fastOpts())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}

func TestEnsureFailsOnVerifyTimeout(t *testing.T) {
	s := &fakeStep{name: "x", doneSeq: []bool{false}} // never becomes done
	res, err := Ensure(context.Background(), s, fastOpts())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}
