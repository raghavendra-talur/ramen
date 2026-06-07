// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// idempotentStep is an ensure.Step for idempotent operations (e.g. kubectl
// apply, kubectl wait, rollout status). Its Done returns false initially so
// the operation is always executed, and true after Do succeeds so
// ensure.Ensure's post-Do verification completes immediately without timeout.
//
// This is the standard primitive for addon builders: use newApplyStep for
// direct unit tests of step primitives, but use this type inside builder
// functions that will be run through ensure.Ensure.
type idempotentStep struct {
	name    string
	applyFn func(ctx context.Context) error
	done    atomic.Bool
}

func (s *idempotentStep) Name() string { return s.name }

// Done returns false until Do has been called successfully, then true.
func (s *idempotentStep) Done(_ context.Context) (bool, error) { return s.done.Load(), nil }

// Do executes the apply function and marks the step as done on success.
func (s *idempotentStep) Do(ctx context.Context) error {
	if err := s.applyFn(ctx); err != nil {
		return err
	}
	s.done.Store(true)
	return nil
}

// newApplyStep returns an idempotentStep: Done=false initially (always runs),
// Done=true after Do completes (verification passes immediately). This is
// correct for kubectl apply, kubectl wait, rollout status, and mc operations
// which are idempotent and safe to re-run.
func newApplyStep(name string, applyFn func(ctx context.Context) error) ensure.Step {
	return &idempotentStep{name: name, applyFn: applyFn}
}

// waitStep is an ensure.Step whose Done checks a real readiness condition and
// whose Do executes a blocking wait command. This makes re-runs cheap: if the
// condition is already met, Done returns true and the step is skipped.
type waitStep struct {
	name   string
	doneFn func(ctx context.Context) (bool, error)
	doFn   func(ctx context.Context) error
}

func (s waitStep) Name() string                           { return s.name }
func (s waitStep) Done(ctx context.Context) (bool, error) { return s.doneFn(ctx) }
func (s waitStep) Do(ctx context.Context) error           { return s.doFn(ctx) }

// newWaitStep returns a waitStep with the given name, done check, and do action.
func newWaitStep(name string, doneFn func(ctx context.Context) (bool, error), doFn func(ctx context.Context) error) ensure.Step {
	return waitStep{name: name, doneFn: doneFn, doFn: doFn}
}

// ApplyTemplate reads a file from the AddonsDir and substitutes $key and ${key}
// variable references using the provided vars map (see ApplyTemplateBytes).
//
// The path is resolved relative to d.AddonsDir.
func ApplyTemplate(d Deps, path string, vars map[string]string) ([]byte, error) {
	full := filepath.Join(d.AddonsDir, path)
	data, err := os.ReadFile(full)
	if err != nil {
		return nil, err
	}
	return ApplyTemplateBytes(data, vars), nil
}

// ApplyTemplateBytes substitutes $key and ${key} variable references in a raw
// template using the provided vars map. Only the given keys are replaced; any
// other "$" text is left untouched (matching the rook addon templates, which
// only reference the cluster/name/pool variables we supply). This avoids the
// silent corruption os.Expand would cause by blanking unrecognised "$" tokens.
func ApplyTemplateBytes(template []byte, vars map[string]string) []byte {
	pairs := make([]string, 0, len(vars)*4)
	for k, v := range vars {
		pairs = append(pairs, "${"+k+"}", v, "$"+k, v)
	}
	return []byte(strings.NewReplacer(pairs...).Replace(string(template)))
}

// Serial returns a serial ensure.Group, which is a convenience alias for
// ensure.NewGroup(name, ensure.Serial, opts, steps...). Use this inside addon
// builders to keep the code readable.
func Serial(name string, opts ensure.Options, steps ...ensure.Step) ensure.Step {
	return ensure.NewGroup(name, ensure.Serial, opts, steps...)
}

// Parallel returns a parallel ensure.Group.
func Parallel(name string, opts ensure.Options, steps ...ensure.Step) ensure.Step {
	return ensure.NewGroup(name, ensure.Parallel, opts, steps...)
}
