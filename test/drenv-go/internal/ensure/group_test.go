// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// recordStep appends its name to rec when Do runs, then reports Done.
type recordStep struct {
	name  string
	rec   *[]string
	mu    *sync.Mutex
	done  bool
	doErr error
}

func (s *recordStep) Name() string { return s.name }

func (s *recordStep) Done(ctx context.Context) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.done, nil
}

func (s *recordStep) Do(ctx context.Context) error {
	if s.doErr != nil {
		return s.doErr
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	*s.rec = append(*s.rec, s.name)
	s.done = true
	return nil
}

func testOpts() Options {
	return Options{VerifyTimeout: time.Second, VerifyInterval: time.Millisecond}
}

func TestGroupSerialRunsInOrder(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu}
	b := &recordStep{name: "b", rec: &rec, mu: &mu}
	g := NewGroup("grp", Serial, testOpts(), a, b)

	res, err := Ensure(context.Background(), g, testOpts())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res != Changed {
		t.Fatalf("got %v, want Changed", res)
	}
	if len(rec) != 2 || rec[0] != "a" || rec[1] != "b" {
		t.Fatalf("got order %v, want [a b]", rec)
	}
}

func TestGroupParallelRunsAll(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu}
	b := &recordStep{name: "b", rec: &rec, mu: &mu}
	g := NewGroup("grp", Parallel, testOpts(), a, b)

	if _, err := Ensure(context.Background(), g, testOpts()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(rec) != 2 {
		t.Fatalf("got %d steps run, want 2", len(rec))
	}
}

func TestGroupDoneAggregates(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu, done: true}
	b := &recordStep{name: "b", rec: &rec, mu: &mu, done: false}
	g := NewGroup("grp", Serial, testOpts(), a, b)

	ok, err := g.Done(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Fatal("group Done = true, want false (one child not done)")
	}
}

func TestGroupPropagatesError(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu, doErr: errors.New("boom")}
	g := NewGroup("grp", Serial, testOpts(), a)

	res, err := Ensure(context.Background(), g, testOpts())
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}

func TestGroupParallelPropagatesError(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	failing := &recordStep{name: "fail", rec: &rec, mu: &mu, doErr: errors.New("boom")}
	ok := &recordStep{name: "ok", rec: &rec, mu: &mu}
	g := NewGroup("grp", Parallel, testOpts(), failing, ok)

	res, err := Ensure(context.Background(), g, testOpts())
	if err == nil {
		t.Fatal("expected error from parallel group, got nil")
	}
	if res != Failed {
		t.Fatalf("got %v, want Failed", res)
	}
}

func TestGroupDoneTrueWhenAllChildrenDone(t *testing.T) {
	var mu sync.Mutex
	var rec []string
	a := &recordStep{name: "a", rec: &rec, mu: &mu, done: true}
	b := &recordStep{name: "b", rec: &rec, mu: &mu, done: true}
	g := NewGroup("grp", Serial, testOpts(), a, b)

	ok, err := g.Done(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Fatal("group Done = false, want true (all children done)")
	}
}
