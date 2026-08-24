// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"
)

type ManagerOpts struct {
	Name           string // process label, used for the log file
	Bin            string
	Kubeconfig     string
	LogDir         string
	ControllerType string // dr-hub | dr-cluster
	Reconcilers    string // RAMEN_RECONCILERS value
}

// ManagerProcess runs one ramen operator as a subprocess. Kill/Restart enable
// crash-recovery scenarios.
type ManagerProcess struct {
	mu      sync.Mutex
	opts    ManagerOpts
	cmd     *exec.Cmd
	log     *os.File
	exited  bool // set by the reaper goroutine once cmd.Wait() returns
	stopped bool // set by Stop(); once true, Restart() refuses to resurrect
}

func StartManager(o ManagerOpts) (*ManagerProcess, error) {
	p := &ManagerProcess{opts: o}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.startLocked(); err != nil {
		return nil, err
	}

	return p, nil
}

// managerArgs builds the manager's command line. Manager options come from
// flags now (not the RamenConfig): leader election must be off (the process
// has no in-cluster identity to elect with) and the metrics/probe listeners
// must be disabled (three managers share one host, and the defaults bind
// fixed ports).
func managerArgs(o ManagerOpts) []string {
	if _, err := os.Stat(o.Kubeconfig); err != nil {
		// Test-harness path: Bin is a stand-in like /bin/sleep and
		// Kubeconfig is its literal argument, not a file.
		return []string{o.Kubeconfig}
	}

	return []string{
		"--kubeconfig=" + o.Kubeconfig,
		"--leader-elect=false",
		"--metrics-bind-address=0",
		"--health-probe-bind-address=0",
	}
}

// startLocked starts the subprocess. Callers must hold p.mu.
func (p *ManagerProcess) startLocked() error {
	logFile, err := os.OpenFile(
		filepath.Join(p.opts.LogDir, p.opts.Name+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	args := managerArgs(p.opts)

	cmd := exec.Command(p.opts.Bin, args...)
	cmd.Env = append(os.Environ(),
		"RAMEN_CONTROLLER_TYPE="+p.opts.ControllerType,
		"POD_NAMESPACE="+RamenSystemNS,
		"RAMEN_RECONCILERS="+p.opts.Reconcilers,
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()

		return fmt.Errorf("start %s: %w", p.opts.Name, err)
	}

	p.cmd, p.log, p.exited, p.stopped = cmd, logFile, false, false

	// The reaper must never touch cmd.ProcessState directly from Alive()'s
	// perspective: it records completion in p.exited under p.mu instead, so
	// Alive() (and everything else) only ever reads state guarded by the
	// mutex.
	go func(c *exec.Cmd) {
		_ = c.Wait()

		p.mu.Lock()
		if p.cmd == c {
			p.exited = true
		}
		p.mu.Unlock()
	}(cmd)

	return nil
}

func (p *ManagerProcess) Alive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.cmd != nil && !p.exited
}

// Kill sends SIGKILL to the process. Callers must not assume the process has
// exited when Kill returns; use Alive() to poll for that.
func (p *ManagerProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.killLocked()
}

// killLocked signals the process. Callers must hold p.mu.
func (p *ManagerProcess) killLocked() error {
	if p.cmd == nil || p.cmd.Process == nil || p.exited {
		return nil
	}

	err := p.cmd.Process.Signal(syscall.SIGKILL)
	if err != nil && (errors.Is(err, os.ErrProcessDone) || errors.Is(err, syscall.ESRCH)) {
		return nil
	}

	return err
}

// Restart kills the current process, waits for it to be reaped, and starts a
// fresh one, all under a single critical section (aside from bounded sleeps
// while waiting for the reaper) so a concurrent Stop() cannot race with the
// restart and resurrect an intentionally-stopped process.
func (p *ManagerProcess) Restart() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return fmt.Errorf("restart %s: process stopped", p.opts.Name)
	}

	if err := p.killLocked(); err != nil {
		return err
	}

	deadline := time.Now().Add(10 * time.Second)
	for !p.exited {
		if time.Now().After(deadline) {
			return fmt.Errorf("restart %s: timed out waiting for process to exit", p.opts.Name)
		}

		p.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
		p.mu.Lock()

		// A concurrent Stop() may have run while unlocked.
		if p.stopped {
			return fmt.Errorf("restart %s: process stopped", p.opts.Name)
		}
	}

	if p.log != nil {
		p.log.Close()
		p.log = nil
	}

	return p.startLocked()
}

// Stop terminates the process at teardown (SIGKILL is fine for tests).
func (p *ManagerProcess) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.stopped = true

	_ = p.killLocked()

	if p.log != nil {
		p.log.Close()
		p.log = nil
	}
}
