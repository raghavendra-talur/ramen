// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import "context"

// Velero wraps a Runner to issue velero CLI commands. All methods take a context
// so callers can cancel long-running operations.
type Velero struct {
	R Runner
}

// Install runs `velero install <flags...>`.
func (v Velero) Install(ctx context.Context, flags ...string) error {
	args := append([]string{"install"}, flags...)
	return v.R.Run(ctx, "velero", args...)
}
