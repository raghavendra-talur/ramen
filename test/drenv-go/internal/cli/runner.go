// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package cli provides a Runner seam for executing external commands, a real
// Exec implementation, a FakeRunner for unit tests, and a Minikube wrapper.
package cli

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
)

// Runner is the seam for running external commands. Implementations include
// Exec (real) and FakeRunner (test double).
type Runner interface {
	// Run executes name with args, wiring its stdout and stderr to the
	// process's own descriptors so the caller sees live output. It returns
	// a non-nil error when the command exits with a non-zero status.
	Run(ctx context.Context, name string, args ...string) error

	// Output executes name with args and returns the trimmed combined output
	// (stdout + stderr). Errors are wrapped with the command line for easier
	// diagnosis. The returned output is populated even when the error is
	// non-nil, so callers can inspect failure text (e.g. to distinguish a
	// "not found" exit from a real failure).
	Output(ctx context.Context, name string, args ...string) (string, error)

	// RunStdin executes name with args, feeding stdin as the command's standard
	// input and wiring stdout/stderr to the process's own descriptors. It
	// returns a non-nil error when the command exits with a non-zero status.
	RunStdin(ctx context.Context, stdin string, name string, args ...string) error

	// RunEnv executes name with args in the environment of the current process
	// augmented with the extra key=value pairs in env. Stdout and stderr are
	// wired to the process's own descriptors. It returns a non-nil error when
	// the command exits with a non-zero status.
	//
	// Use this when a tool requires environment variables (e.g. KUBECONFIG for
	// argocd) that cannot be expressed as CLI flags.
	RunEnv(ctx context.Context, env []string, name string, args ...string) error
}

// Exec is the real Runner that shells out via os/exec.CommandContext.
type Exec struct{}

// Run wires stdout and stderr to the current process's file descriptors so
// that subcommand output is visible to the user in real time.
func (Exec) Run(ctx context.Context, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// Output captures the combined stdout+stderr of the command, trims
// surrounding whitespace, and returns it. On non-zero exit the error is
// wrapped with the command line string.
func (Exec) Output(ctx context.Context, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf

	// Capture output regardless of exit status: callers like Minikube.Status
	// rely on the failure text (e.g. "not found") emitted on a non-zero exit.
	err := cmd.Run()
	out := strings.TrimSpace(buf.String())
	if err != nil {
		return out, fmt.Errorf("%s: %w", cmdLine(name, args), err)
	}
	return out, nil
}

// RunStdin executes name with args, feeding stdin as the command's standard
// input. Stdout and stderr are wired to the current process's file descriptors
// so the caller sees live output. A non-zero exit status is returned as an error.
func (Exec) RunStdin(ctx context.Context, stdin string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Stdin = strings.NewReader(stdin)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// RunEnv executes name with args with the process environment augmented by the
// extra key=value pairs in env (e.g. []string{"KUBECONFIG=/tmp/kc"}). Stdout
// and stderr are wired to the current process's file descriptors.
func (Exec) RunEnv(ctx context.Context, env []string, name string, args ...string) error {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Env = append(os.Environ(), env...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd.Run()
}

// cmdLine formats a command name and its arguments as a single string for
// use in error messages.
func cmdLine(name string, args []string) string {
	if len(args) == 0 {
		return name
	}
	return name + " " + strings.Join(args, " ")
}
