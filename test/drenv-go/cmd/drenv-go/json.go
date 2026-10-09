// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"encoding/json"
	"io"
)

// writeJSON writes v as an indented JSON document. In --json mode it must be
// the only thing a command writes to stdout.
func writeJSON(w io.Writer, v any) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}
