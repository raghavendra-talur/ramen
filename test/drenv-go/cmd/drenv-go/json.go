// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"io"
	"os"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

// runnerFor returns the command runner for a command: in --json mode live
// subprocess output goes to stderr so stdout carries only the JSON document.
func runnerFor(asJSON bool) cli.Exec {
	if asJSON {
		return cli.Exec{Stdout: os.Stderr}
	}
	return cli.Exec{}
}

// writeJSON writes v as an indented JSON document. In --json mode it must be
// the only thing a command writes to stdout.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
