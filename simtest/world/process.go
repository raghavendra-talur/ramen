// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
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
	mu   sync.Mutex
	opts ManagerOpts
	cmd  *exec.Cmd
	log  *os.File
}

func StartManager(o ManagerOpts) (*ManagerProcess, error) {
	p := &ManagerProcess{opts: o}
	if err := p.start(); err != nil {
		return nil, err
	}

	return p, nil
}

func (p *ManagerProcess) start() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	logFile, err := os.OpenFile(
		filepath.Join(p.opts.LogDir, p.opts.Name+".log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}

	args := []string{p.opts.Kubeconfig}
	if _, err := os.Stat(p.opts.Kubeconfig); err == nil {
		args = []string{"--kubeconfig=" + p.opts.Kubeconfig}
	}

	cmd := exec.Command(p.opts.Bin, args...)
	cmd.Env = append(os.Environ(),
		"RAMEN_CONTROLLER_TYPE="+p.opts.ControllerType,
		"POD_NAMESPACE="+RamenSystemNS,
		"RAMEN_RECONCILERS="+p.opts.Reconcilers,
		"RAMEN_METRICS_BIND_ADDRESS=0",
		"RAMEN_HEALTH_BIND_ADDRESS=0",
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile

	if err := cmd.Start(); err != nil {
		logFile.Close()

		return fmt.Errorf("start %s: %w", p.opts.Name, err)
	}

	go func() { _ = cmd.Wait() }() // reap; Alive() checks the result

	p.cmd, p.log = cmd, logFile

	return nil
}

func (p *ManagerProcess) Alive() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.cmd != nil && p.cmd.ProcessState == nil
}

func (p *ManagerProcess) Kill() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}

	return p.cmd.Process.Signal(syscall.SIGKILL)
}

func (p *ManagerProcess) Restart() error {
	_ = p.Kill()
	p.mu.Lock()
	if p.log != nil {
		p.log.Close()
	}
	p.mu.Unlock()

	return p.start()
}

// Stop terminates the process at teardown (SIGKILL is fine for tests).
func (p *ManagerProcess) Stop() {
	_ = p.Kill()

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.log != nil {
		p.log.Close()
		p.log = nil
	}
}
