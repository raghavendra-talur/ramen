// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

import (
	"context"
	"os"
	"path/filepath"
	"strings"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// applyStep is an ensure.Step that always runs its apply function (Done
// returns false). This is correct for idempotent apply operations — kubectl
// apply is safe to re-run every time.
type applyStep struct {
	name    string
	applyFn func(ctx context.Context) error
}

func (s applyStep) Name() string { return s.name }

// Done always returns false: apply is idempotent and cheap, so we always run
// it rather than trying to detect whether it's needed.
func (s applyStep) Done(_ context.Context) (bool, error) { return false, nil }

// Do executes the apply function.
func (s applyStep) Do(ctx context.Context) error { return s.applyFn(ctx) }

// newApplyStep returns an applyStep with the given name and apply function.
func newApplyStep(name string, applyFn func(ctx context.Context) error) ensure.Step {
	return applyStep{name: name, applyFn: applyFn}
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

// ApplyTemplate reads a file from the AddonsDir, substitutes $key and ${key}
// variable references using the provided vars map, and returns the resulting
// manifest bytes. The substitution follows os.Expand semantics (${key} and
// $key both work). Unrecognised variables are replaced with an empty string.
//
// The path is resolved relative to d.AddonsDir.
func ApplyTemplate(d Deps, path string, vars map[string]string) ([]byte, error) {
	full := filepath.Join(d.AddonsDir, path)
	data, err := os.ReadFile(full)
	if err != nil {
		return nil, err
	}
	result := os.Expand(string(data), func(key string) string {
		return vars[key]
	})
	return []byte(result), nil
}

// ApplyTemplateBytes substitutes variables in a raw template (not read from
// disk). It is the pure-function core used by ApplyTemplate and directly in
// tests.
func ApplyTemplateBytes(template []byte, vars map[string]string) []byte {
	result := os.Expand(string(template), func(key string) string {
		return vars[key]
	})
	return []byte(result)
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

// templateExpander provides Python string.Template-compatible substitution
// for $name and ${name} tokens using strings.NewReplacer. This is an
// alternative to os.Expand that may be more predictable for callers that need
// exact Python Template semantics (no partial expansion on unknown keys).
//
// Usage: expanded := expandTemplate(template, vars)
func expandTemplate(template string, vars map[string]string) string {
	// Build replacer pairs: both "${key}" and "$key" → value.
	pairs := make([]string, 0, len(vars)*4)
	for k, v := range vars {
		pairs = append(pairs, "${"+k+"}", v)
		pairs = append(pairs, "$"+k, v)
	}
	r := strings.NewReplacer(pairs...)
	return r.Replace(template)
}

// ExpandTemplate is the exported form of expandTemplate, for tests.
func ExpandTemplate(template string, vars map[string]string) string {
	return expandTemplate(template, vars)
}
