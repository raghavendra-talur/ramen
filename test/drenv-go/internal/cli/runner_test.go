// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"errors"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

// ---- Exec tests ----

func TestExecOutputEcho(t *testing.T) {
	ctx := context.Background()
	e := cli.Exec{}
	out, err := e.Output(ctx, "echo", "hi")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "hi" {
		t.Fatalf("expected %q, got %q", "hi", out)
	}
}

func TestExecRunTrue(t *testing.T) {
	ctx := context.Background()
	e := cli.Exec{}
	if err := e.Run(ctx, "true"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestExecRunFalse(t *testing.T) {
	ctx := context.Background()
	e := cli.Exec{}
	if err := e.Run(ctx, "false"); err == nil {
		t.Fatal("expected error from false, got nil")
	}
}

func TestExecOutputError(t *testing.T) {
	ctx := context.Background()
	e := cli.Exec{}
	_, err := e.Output(ctx, "false")
	if err == nil {
		t.Fatal("expected error from false, got nil")
	}
}

// ---- FakeRunner tests ----

func TestFakeRunnerRecordsCalls(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	_ = f.Run(ctx, "minikube", "start", "-p", "dr1")
	_ = f.Run(ctx, "minikube", "stop", "-p", "dr1")

	if len(f.Calls) != 2 {
		t.Fatalf("expected 2 calls, got %d", len(f.Calls))
	}
	if f.Calls[0].Name != "minikube" {
		t.Errorf("call[0].Name = %q, want %q", f.Calls[0].Name, "minikube")
	}
	if len(f.Calls[0].Args) != 3 || f.Calls[0].Args[0] != "start" {
		t.Errorf("call[0].Args = %v, want [start -p dr1]", f.Calls[0].Args)
	}
}

func TestFakeRunnerScriptedError(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Err: boom})

	err := f.Run(ctx, "minikube", "start")
	if !errors.Is(err, boom) {
		t.Fatalf("expected boom, got %v", err)
	}
}

func TestFakeRunnerScriptedOutput(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "hello world"})

	out, err := f.Output(ctx, "echo", "hello world")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "hello world" {
		t.Fatalf("expected %q, got %q", "hello world", out)
	}
}

func TestFakeRunnerDefaultNoError(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	// No scripted results — should return zero-value (no error, empty output)
	if err := f.Run(ctx, "anything"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	out, err := f.Output(ctx, "anything")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "" {
		t.Fatalf("expected empty output, got %q", out)
	}
}

func TestFakeRunnerMultipleScriptedResults(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "first"})
	f.Script(cli.FakeResult{Out: "second"})

	out1, _ := f.Output(ctx, "cmd")
	out2, _ := f.Output(ctx, "cmd")
	// After script exhausted, returns zero value
	out3, _ := f.Output(ctx, "cmd")

	if out1 != "first" {
		t.Errorf("call 1: got %q, want %q", out1, "first")
	}
	if out2 != "second" {
		t.Errorf("call 2: got %q, want %q", out2, "second")
	}
	if out3 != "" {
		t.Errorf("call 3: got %q, want empty", out3)
	}
}
