// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ensure

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestConsoleReporterSkipped(t *testing.T) {
	var buf bytes.Buffer
	r := ConsoleReporter{W: &buf}
	r.Skipped("minikube/dr1", 0)
	out := buf.String()
	if !strings.Contains(out, "minikube/dr1") || !strings.Contains(out, "skipped") {
		t.Fatalf("output %q missing name or 'skipped'", out)
	}
}

func TestConsoleReporterChanged(t *testing.T) {
	var buf bytes.Buffer
	r := ConsoleReporter{W: &buf}
	r.Changed("addon/rook", 12300*time.Millisecond)
	out := buf.String()
	if !strings.Contains(out, "addon/rook") {
		t.Fatalf("output %q missing name", out)
	}
}

func TestConsoleReporterFailed(t *testing.T) {
	var buf bytes.Buffer
	r := ConsoleReporter{W: &buf}
	r.Failed("addon/velero", 0, errors.New("not ready"))
	out := buf.String()
	if !strings.Contains(out, "addon/velero") || !strings.Contains(out, "not ready") {
		t.Fatalf("output %q missing name or error", out)
	}
}
