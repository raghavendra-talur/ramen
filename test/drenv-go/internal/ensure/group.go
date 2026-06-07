// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"context"

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

// Group is a Step composed of child steps. Its Done is the conjunction of its
// children's Done; its Do ensures the children under the group's Mode.
type Group struct {
	name  string
	mode  Mode
	opts  Options
	steps []Step
}

// NewGroup builds a Group. opts is used when ensuring children.
func NewGroup(name string, mode Mode, opts Options, steps ...Step) *Group {
	return &Group{name: name, mode: mode, opts: opts, steps: steps}
}

func (g *Group) Name() string { return g.name }

// Done reports whether every child is done.
func (g *Group) Done(ctx context.Context) (bool, error) {
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

// Do ensures all children according to the group's Mode.
func (g *Group) Do(ctx context.Context) error {
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
