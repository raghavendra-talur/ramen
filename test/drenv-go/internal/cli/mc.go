// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import "context"

// MC wraps a Runner to issue mc (MinIO Client) CLI commands. All methods take a
// context so callers can cancel long-running operations.
type MC struct {
	R Runner
}

// SetAlias runs `mc alias set <name> <url> <key> <secret>`.
func (m MC) SetAlias(ctx context.Context, name, url, key, secret string) error {
	return m.R.Run(ctx, "mc", "alias", "set", name, url, key, secret)
}

// MakeBucket runs `mc mb [--ignore-existing] <target>`.
func (m MC) MakeBucket(ctx context.Context, target string, ignoreExisting bool) error {
	args := []string{"mb"}
	if ignoreExisting {
		args = append(args, "--ignore-existing")
	}
	args = append(args, target)
	return m.R.Run(ctx, "mc", args...)
}
