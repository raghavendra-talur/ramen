// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

const (
	defaultStopTimeout = 15 * time.Second
	stopPollInterval   = 100 * time.Millisecond
)

// procController can also signal processes.
type procController interface {
	procInspector
	Signal(pid int, sig syscall.Signal) error
}

func (osProcs) Signal(pid int, sig syscall.Signal) error {
	if pid <= 0 {
		return fmt.Errorf("invalid pid %d", pid)
	}

	return syscall.Kill(pid, sig)
}

// stopTarget is a process to stop, with a check that still recognizes it
// (so a reused pid is never signaled).
type stopTarget struct {
	name    string
	pid     int
	running func() bool
}

type stopper struct {
	bin     string
	procs   procController
	out     io.Writer
	timeout time.Duration
	poll    time.Duration
}

func newStopper(opts *runOptions, out io.Writer, timeout time.Duration) stopper {
	return stopper{bin: opts.bin(), procs: osProcs{}, out: out, timeout: timeout, poll: stopPollInterval}
}

// stop stops the supervisor recorded in stateFile and any of its managers
// still running. It is idempotent: with nothing running it only says so.
func (s stopper) stop(stateFile string) error {
	st, err := readState(stateFile)
	if errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(s.out, "nothing running (no state file %s)\n", stateFile)

		return nil
	}

	if err != nil {
		return err
	}

	var leftovers []stopTarget

	supPID := st.PID
	sup := stopTarget{
		name:    "supervisor",
		pid:     supPID,
		running: func() bool { return isSupervisorProcess(s.procs, supPID) },
	}

	supRunning := sup.running()
	if supRunning {
		leftovers = s.terminate([]stopTarget{sup})

		// The supervisor recorded its managers' exits; use the latest state.
		if latest, err := readState(stateFile); err == nil {
			st = latest
		}
	}

	managers := s.runningManagers(st)

	if !supRunning && len(managers) == 0 {
		fmt.Fprintf(s.out, "nothing running (supervisor pid %d and its managers are not running)\n", supPID)

		return nil
	}

	leftovers = append(leftovers, s.terminate(managers)...)
	s.kill(leftovers)

	return nil
}

// runningManagers returns the recorded managers that are still running.
func (s stopper) runningManagers(st *supervisorState) []stopTarget {
	var targets []stopTarget

	for _, m := range st.Managers {
		if !isManagerProcess(s.procs, s.bin, m.PID, m.HealthAddr) {
			continue
		}

		targets = append(targets, stopTarget{
			name:    fmt.Sprintf("manager %s:%s", m.Cluster, m.Reconciler),
			pid:     m.PID,
			running: func() bool { return isManagerProcess(s.procs, s.bin, m.PID, m.HealthAddr) },
		})
	}

	return targets
}

// terminate sends SIGTERM to targets and waits up to the timeout. It returns
// the targets still running.
func (s stopper) terminate(targets []stopTarget) []stopTarget {
	if len(targets) == 0 {
		return nil
	}

	for _, t := range targets {
		s.signal(t, syscall.SIGTERM)
	}

	leftovers := s.waitStopped(targets)

	for _, t := range targets {
		if t.running() {
			fmt.Fprintf(s.out, "%s (pid %d) still running after %s\n", t.name, t.pid, s.timeout)
		} else {
			fmt.Fprintf(s.out, "%s (pid %d) exited\n", t.name, t.pid)
		}
	}

	return leftovers
}

func (s stopper) kill(targets []stopTarget) {
	for _, t := range targets {
		if t.running() {
			s.signal(t, syscall.SIGKILL)
		}
	}
}

func (s stopper) signal(t stopTarget, sig syscall.Signal) {
	if err := s.procs.Signal(t.pid, sig); err != nil {
		fmt.Fprintf(s.out, "cannot send %s to %s (pid %d): %v\n", sigName(sig), t.name, t.pid, err)

		return
	}

	fmt.Fprintf(s.out, "sent %s to %s (pid %d)\n", sigName(sig), t.name, t.pid)
}

// waitStopped polls until no target is running or the timeout expires, and
// returns the targets still running.
func (s stopper) waitStopped(targets []stopTarget) []stopTarget {
	deadline := time.Now().Add(s.timeout)

	for {
		var running []stopTarget

		for _, t := range targets {
			if t.running() {
				running = append(running, t)
			}
		}

		if len(running) == 0 || time.Now().After(deadline) {
			return running
		}

		time.Sleep(s.poll)
	}
}

var signalNames = map[syscall.Signal]string{
	syscall.SIGTERM: "SIGTERM",
	syscall.SIGKILL: "SIGKILL",
}

func sigName(sig syscall.Signal) string {
	if name, ok := signalNames[sig]; ok {
		return name
	}

	return sig.String()
}

func newStopCmd(opts *runOptions) *cobra.Command {
	var timeout time.Duration

	cmd := &cobra.Command{
		Use:   "stop",
		Short: "Stop the running supervisor and any of its managers still running",
		Run: func(cmd *cobra.Command, args []string) {
			if err := newStopper(opts, cmd.OutOrStdout(), timeout).stop(stateFilePath(opts.root)); err != nil {
				fatal("stop: %v", err)
			}
		},
	}

	cmd.Flags().DurationVar(&timeout, "timeout", defaultStopTimeout,
		"how long to wait for processes to exit after SIGTERM before SIGKILL")

	return cmd
}
