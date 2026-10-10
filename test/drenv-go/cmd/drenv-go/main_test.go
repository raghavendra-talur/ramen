// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"errors"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func executeRoot(t *testing.T, sub *cobra.Command, args ...string) string {
	t.Helper()

	root := newRootCommand()
	root.AddCommand(sub)

	var out bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&out)
	root.SetArgs(args)

	if err := root.Execute(); err == nil {
		t.Fatalf("%v: expected an error", args)
	}

	return out.String()
}

func TestRuntimeErrorPrintsNoUsage(t *testing.T) {
	sub := &cobra.Command{
		Use:  "fail",
		RunE: func(*cobra.Command, []string) error { return errors.New("exit status 1") },
	}

	out := executeRoot(t, sub, "fail")
	if strings.Contains(out, "Usage:") {
		t.Errorf("runtime error printed usage:\n%s", out)
	}

	if !strings.Contains(out, "exit status 1") {
		t.Errorf("runtime error not printed:\n%s", out)
	}
}

func TestFlagErrorPrintsUsage(t *testing.T) {
	sub := &cobra.Command{Use: "fail", RunE: func(*cobra.Command, []string) error { return nil }}

	out := executeRoot(t, sub, "fail", "--no-such-flag")
	if !strings.Contains(out, "Usage:") {
		t.Errorf("flag error printed no usage:\n%s", out)
	}
}
