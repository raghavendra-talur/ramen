// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestEvLogOnLine(t *testing.T) {
	l, err := NewEvLog(filepath.Join(t.TempDir(), "ev.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()

	var got string
	l.OnLine = func(line string) { got = line }
	l.Logf("actor %s did %s", "volrep", "fulfill")

	if !strings.Contains(got, "actor volrep did fulfill") {
		t.Fatalf("OnLine got %q", got)
	}
}
