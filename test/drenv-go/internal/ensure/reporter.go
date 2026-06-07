// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"fmt"
	"io"
	"time"
)

// ConsoleReporter writes human-readable checkpoint lines to W.
type ConsoleReporter struct {
	W io.Writer
}

var _ Reporter = ConsoleReporter{}

// Start is a no-op: a checkpoint line is only emitted once its outcome is known.
func (c ConsoleReporter) Start(name string) {}

func (c ConsoleReporter) Skipped(name string, d time.Duration) {
	fmt.Fprintf(c.W, "✓ %s (skipped, already satisfied)\n", name)
}

func (c ConsoleReporter) Changed(name string, d time.Duration) {
	fmt.Fprintf(c.W, "✓ %s (done in %.1fs)\n", name, d.Seconds())
}

func (c ConsoleReporter) Failed(name string, d time.Duration, err error) {
	fmt.Fprintf(c.W, "✗ %s: %v\n", name, err)
}
