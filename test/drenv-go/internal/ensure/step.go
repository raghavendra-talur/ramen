// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package ensure provides an idempotent "ensure" model: every unit of work is a
// Step that checks reality, acts only if reality is wrong, then verifies.
package ensure

import "context"

// Result is the outcome of ensuring a Step.
type Result int

const (
	// Skipped means the step was already satisfied; no action was taken.
	Skipped Result = iota
	// Changed means the step performed work and is now satisfied.
	Changed
	// Failed means the step could not be satisfied.
	Failed
)

func (r Result) String() string {
	switch r {
	case Skipped:
		return "skipped"
	case Changed:
		return "changed"
	case Failed:
		return "failed"
	default:
		return "unknown"
	}
}

// Step is one idempotent unit of work. Done is a cheap, read-only check used both
// as the pre-condition (skip if already true) and as verification after Do.
type Step interface {
	Name() string
	Done(ctx context.Context) (bool, error)
	Do(ctx context.Context) error
}
