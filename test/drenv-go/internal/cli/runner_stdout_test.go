// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

// TestExecStdoutRedirect pins that the live-output methods honour Exec.Stdout,
// so --json commands can keep subprocess output off the real stdout.
func TestExecStdoutRedirect(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		run  func(cli.Exec) error
	}{
		{"Run", func(e cli.Exec) error { return e.Run(ctx, "echo", "hi") }},
		{"RunStdin", func(e cli.Exec) error { return e.RunStdin(ctx, "hi\n", "cat") }},
		{"RunEnv", func(e cli.Exec) error { return e.RunEnv(ctx, []string{"X=hi"}, "sh", "-c", "echo $X") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := tt.run(cli.Exec{Stdout: &buf}); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got := buf.String(); got != "hi\n" {
				t.Fatalf("redirected stdout = %q, want %q", got, "hi\n")
			}
		})
	}
}
