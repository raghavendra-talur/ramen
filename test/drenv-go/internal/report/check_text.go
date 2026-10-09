// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package report

import (
	"fmt"
	"io"
)

// WriteCheckText prints one line per step:
//
//	✓ dr1/addon/rook-operator
//	✗ dr1/addon/rook-pool (not-ready)
//	? dr1/addon/velero (error: ...)
//	- global/addon/rbd-mirror (skipped: ...)
//	? dr1/addon/foo (unimplemented)
func WriteCheckText(w io.Writer, c Check) error {
	for _, s := range c.Steps {
		if _, err := fmt.Fprintln(w, checkLine(s)); err != nil {
			return err
		}
	}
	return nil
}

func checkLine(s CheckStep) string {
	scope := s.Profile
	if s.Global {
		scope = "global"
	}
	label := scope + "/" + s.Name

	switch s.State {
	case StateReady:
		return "✓ " + label
	case StateNotReady:
		if s.Error != "" {
			return fmt.Sprintf("✗ %s (not-ready: %s)", label, s.Error)
		}
		return fmt.Sprintf("✗ %s (not-ready)", label)
	case StateSkipped:
		return fmt.Sprintf("- %s (skipped: %s)", label, s.Error)
	case StateUnimplemented:
		return fmt.Sprintf("? %s (unimplemented)", label)
	default:
		return fmt.Sprintf("? %s (%s: %s)", label, s.State, s.Error)
	}
}
