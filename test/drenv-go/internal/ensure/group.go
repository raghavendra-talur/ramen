// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"context"
	"sync/atomic"

	"golang.org/x/sync/errgroup"
)

// Mode controls how a Group ensures its children.
type Mode int

const (
	// Serial ensures children one at a time, in order.
	Serial Mode = iota
	// Parallel ensures children concurrently.
	Parallel
)

// Group is a Step composed of child steps. Its Do ensures the children under the
// group's Mode.
//
// Done has two modes:
//   - Plain group (gate == nil): Done is the conjunction of the children's Done.
//   - Gated group (gate != nil): Done is the gate — a cheap reality probe of the
//     group's end-state — so a satisfied group is skipped without touching any
//     child. After a successful Do the group latches Done=true, so even a gate
//     that under-reports (false negative) cannot turn a completed Do into a
//     verification timeout; the worst case is re-running the idempotent children.
type Group struct {
	name  string
	mode  Mode
	opts  Options
	steps []Step
	gate  func(context.Context) (bool, error)
	ran   atomic.Bool
}

// NewGroup builds a plain Group whose Done is the conjunction of its children.
// opts is used when ensuring children.
func NewGroup(name string, mode Mode, opts Options, steps ...Step) *Group {
	return &Group{name: name, mode: mode, opts: opts, steps: steps}
}

// NewGatedGroup builds a Group guarded by gate, a cheap reality probe. When gate
// reports true the whole group is skipped; otherwise its children are ensured
// and the group latches done. gate must not be nil.
func NewGatedGroup(name string, mode Mode, opts Options, gate func(context.Context) (bool, error), steps ...Step) *Group {
	return &Group{name: name, mode: mode, opts: opts, steps: steps, gate: gate}
}

func (g *Group) Name() string { return g.name }

// Steps returns the child steps of this group. This is intended for testing
// the composition shape of a tree without running it.
func (g *Group) Steps() []Step { return g.steps }

// GroupMode returns the execution mode (Serial or Parallel) of this group.
// It is named GroupMode (not Mode) to avoid collision with the Mode type.
func (g *Group) GroupMode() Mode { return g.mode }

// Done reports whether the group is satisfied. For a gated group it is the gate
// (a reality probe), short-circuited to true once Do has completed. For a plain
// group it is the conjunction of the children's Done.
func (g *Group) Done(ctx context.Context) (bool, error) {
	if g.gate != nil {
		if g.ran.Load() {
			return true, nil
		}
		return g.gate(ctx)
	}
	for _, s := range g.steps {
		ok, err := s.Done(ctx)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

// Do ensures all children according to the group's Mode. On success a gated
// group latches done so post-Do verification passes immediately.
func (g *Group) Do(ctx context.Context) error {
	if err := g.ensureChildren(ctx); err != nil {
		return err
	}
	if g.gate != nil {
		g.ran.Store(true)
	}
	return nil
}

func (g *Group) ensureChildren(ctx context.Context) error {
	if g.mode == Serial {
		for _, s := range g.steps {
			if _, err := Ensure(ctx, s, g.opts); err != nil {
				return err
			}
		}
		return nil
	}

	eg, egCtx := errgroup.WithContext(ctx)
	for _, s := range g.steps {
		eg.Go(func() error {
			_, err := Ensure(egCtx, s, g.opts)
			return err
		})
	}
	return eg.Wait()
}
