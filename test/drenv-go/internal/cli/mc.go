// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"sync"
)

// mcConfigMu serializes mc alias updates. mc keeps every alias in one
// per-user config file and each update rewrites the whole file, so
// concurrent updates for different clusters lose each other's alias.
var mcConfigMu sync.Mutex

// MC wraps a Runner to issue mc (MinIO Client) CLI commands. All methods take a
// context so callers can cancel long-running operations.
type MC struct {
	R Runner
}

// SetAlias runs `mc alias set <name> <url> <key> <secret>`.
func (m MC) SetAlias(ctx context.Context, name, url, key, secret string) error {
	mcConfigMu.Lock()
	defer mcConfigMu.Unlock()

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

// Stat runs `mc stat <target>`; it fails when the target is unreachable or
// does not exist. Its output is discarded: Stat serves readiness probes,
// which run often and must not flood the log.
func (m MC) Stat(ctx context.Context, target string) error {
	_, err := m.R.Output(ctx, "mc", "stat", target)

	return err
}
