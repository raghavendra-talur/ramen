// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"
)

const (
	probeTimeout     = time.Second
	maxReadyErrorLen = 200
)

// The JSON shapes in this file are a contract with other programs (the
// ramendev dashboard); do not rename fields.

type statusReport struct {
	StateFile  string            `json:"stateFile"`
	Supervisor *supervisorStatus `json:"supervisor"`
	Env        string            `json:"env"`
	Envfile    string            `json:"envfile"`
	Managers   []managerStatus   `json:"managers"`
}

type supervisorStatus struct {
	PID       int    `json:"pid"`
	Alive     bool   `json:"alive"`
	StartedAt string `json:"startedAt"`
	StoppedAt string `json:"stoppedAt"`
}

type managerStatus struct {
	Cluster    string `json:"cluster"`
	Reconciler string `json:"reconciler"`
	Expected   bool   `json:"expected"`
	PID        int    `json:"pid"`
	Alive      bool   `json:"alive"`
	Ready      bool   `json:"ready"`
	ReadyError string `json:"readyError"`
	HealthAddr string `json:"healthAddr"`
	Log        string `json:"log"`
	StartedAt  string `json:"startedAt"`
	ExitedAt   string `json:"exitedAt"`
	ExitErr    string `json:"exitErr"`
}

// state classifies a manager for humans. A manager without a state record
// (never started by the current supervisor) has no startedAt.
func (m managerStatus) state() string {
	switch {
	case m.Ready:
		return "ready"
	case m.Alive:
		return "not-ready"
	case m.StartedAt == "":
		return "missing"
	default:
		return "dead"
	}
}

// procInspector looks at processes by pid.
type procInspector interface {
	// Alive reports whether a process with pid exists (signal 0).
	Alive(pid int) bool
	// Command returns the process command line, or "" if unknown.
	Command(pid int) string
}

// readinessProber checks a manager's health probe endpoint.
type readinessProber interface {
	Ready(ctx context.Context, addr string) error
}

type osProcs struct{}

func (osProcs) Alive(pid int) bool {
	if pid <= 0 {
		return false // signal 0 to pid 0 or -1 would target process groups
	}

	err := syscall.Kill(pid, 0)

	// EPERM: the process exists but belongs to another user.
	return err == nil || errors.Is(err, syscall.EPERM)
}

func (osProcs) Command(pid int) string {
	if pid <= 0 {
		return ""
	}

	out, err := exec.Command("ps", "-o", "command=", "-p", strconv.Itoa(pid)).Output() //nolint:noctx
	if err != nil {
		return ""
	}

	return strings.TrimSpace(string(out))
}

type httpProber struct {
	client *http.Client
}

func (p httpProber) Ready(ctx context.Context, addr string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+addr+"/readyz", nil)
	if err != nil {
		return err
	}

	resp, err := p.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxReadyErrorLen)) //nolint:errcheck // best effort detail

	return fmt.Errorf("HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
}

// managerKey identifies an expected manager.
type managerKey struct {
	cluster    string
	reconciler string
	healthAddr string
}

// expectedManagers returns the managers a supervisor started with filter
// (empty = all reconcilers) should be running for env, in start order.
func expectedManagers(env envFile, filter []string) ([]managerKey, error) {
	clusters, err := clusterTopology(env)
	if err != nil {
		return nil, err
	}

	var keys []managerKey

	for _, c := range clusters {
		for _, rec := range selectedReconcilers(c, filter) {
			keys = append(keys, managerKey{cluster: c.name, reconciler: rec, healthAddr: healthAddr(c, rec)})
		}
	}

	return keys, nil
}

// isSupervisorProcess reports whether pid is a live localrun process. The
// command check guards against pid reuse after the supervisor died.
func isSupervisorProcess(procs procInspector, pid int) bool {
	return procs.Alive(pid) && strings.Contains(procs.Command(pid), "localrun")
}

// isManagerProcess reports whether pid is a live manager started from bin
// with healthAddr, guarding against pid reuse.
func isManagerProcess(procs procInspector, bin string, pid int, healthAddr string) bool {
	if pid <= 0 || !procs.Alive(pid) {
		return false
	}

	cmd := procs.Command(pid)

	return strings.HasPrefix(cmd, bin+" ") &&
		strings.Contains(cmd, "--health-probe-bind-address="+healthAddr)
}

// mergeManagers lists the expected managers first, each with its latest
// state record if any, then records for managers not expected (for example
// after the envfile changed).
func mergeManagers(expected []managerKey, records []managerState) []managerStatus {
	latest := map[[2]string]managerState{}

	var order [][2]string

	for _, r := range records {
		k := [2]string{r.Cluster, r.Reconciler}
		if _, seen := latest[k]; !seen {
			order = append(order, k)
		}

		latest[k] = r
	}

	managers := make([]managerStatus, 0, len(expected)+len(order))

	for _, e := range expected {
		k := [2]string{e.cluster, e.reconciler}
		if r, ok := latest[k]; ok {
			managers = append(managers, statusFromRecord(r, true))

			delete(latest, k)

			continue
		}

		managers = append(managers, managerStatus{
			Cluster: e.cluster, Reconciler: e.reconciler, Expected: true, HealthAddr: e.healthAddr,
		})
	}

	for _, k := range order {
		if r, ok := latest[k]; ok {
			managers = append(managers, statusFromRecord(r, false))
		}
	}

	return managers
}

func statusFromRecord(r managerState, expected bool) managerStatus {
	return managerStatus{
		Cluster:    r.Cluster,
		Reconciler: r.Reconciler,
		Expected:   expected,
		PID:        r.PID,
		HealthAddr: r.HealthAddr,
		Log:        r.Log,
		StartedAt:  r.StartedAt,
		ExitedAt:   r.ExitedAt,
		ExitErr:    r.ExitErr,
	}
}

// statusChecker checks reality for a state file.
type statusChecker struct {
	bin    string
	procs  procInspector
	prober readinessProber
}

// report builds the status report. st may be nil (no state file) and
// expected may be empty (envfile unreadable).
func (s statusChecker) report(ctx context.Context, st *supervisorState, expected []managerKey) statusReport {
	r := statusReport{Managers: []managerStatus{}}

	var records []managerState

	if st != nil {
		r.Supervisor = &supervisorStatus{
			PID:       st.PID,
			Alive:     isSupervisorProcess(s.procs, st.PID),
			StartedAt: st.StartedAt,
			StoppedAt: st.StoppedAt,
		}
		r.Env = st.Env
		r.Envfile = st.Envfile
		records = st.Managers
	}

	r.Managers = append(r.Managers, mergeManagers(expected, records)...)

	for i := range r.Managers {
		m := &r.Managers[i]
		m.Alive = isManagerProcess(s.procs, s.bin, m.PID, m.HealthAddr)
	}

	s.probeAlive(ctx, r.Managers)

	return r
}

// probeAlive probes the alive managers in parallel.
func (s statusChecker) probeAlive(ctx context.Context, managers []managerStatus) {
	var wg sync.WaitGroup

	for i := range managers {
		m := &managers[i]
		if !m.Alive {
			continue
		}

		wg.Add(1)

		go func() {
			defer wg.Done()

			pctx, cancel := context.WithTimeout(ctx, probeTimeout)
			defer cancel()

			if err := s.prober.Ready(pctx, m.HealthAddr); err != nil {
				m.ReadyError = err.Error()
			} else {
				m.Ready = true
			}
		}()
	}

	wg.Wait()
}

func newStatusCmd(opts *runOptions) *cobra.Command {
	var jsonOutput bool

	cmd := &cobra.Command{
		Use:   "status",
		Short: "Show the supervisor and each manager: ready, not-ready, dead, or missing",
		Run: func(cmd *cobra.Command, args []string) {
			r := buildStatus(cmd.Context(), opts, cmd.Flags().Changed("envfile"))

			if jsonOutput {
				printStatusJSON(cmd.OutOrStdout(), r)
			} else {
				printStatusText(cmd.OutOrStdout(), r)
			}
		},
	}

	cmd.Flags().BoolVar(&jsonOutput, "json", false, "print the report as JSON")

	return cmd
}

// buildStatus gathers the inputs for the report. Problems that leave the
// report incomplete are written to stderr so stdout stays parseable.
func buildStatus(ctx context.Context, opts *runOptions, envfileFlagSet bool) statusReport {
	stateFile := stateFilePath(opts.root)

	st, err := readState(stateFile)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		fmt.Fprintf(os.Stderr, "warning: %v\n", err)
	}

	envfile := statusEnvfile(opts.envfile, st, envfileFlagSet)
	env, expected := expectedForState(envfile, st)

	checker := statusChecker{
		bin:    opts.bin(),
		procs:  osProcs{},
		prober: httpProber{client: &http.Client{Timeout: probeTimeout}},
	}

	r := checker.report(ctx, st, expected)
	r.StateFile = stateFile
	r.Envfile = envfile

	if r.Env == "" {
		r.Env = env.Name
	}

	return r
}

// statusEnvfile returns the absolute envfile to describe: without an explicit
// --envfile, the one the supervisor was started with.
func statusEnvfile(flagValue string, st *supervisorState, flagSet bool) string {
	envfile := flagValue
	if st != nil && st.Envfile != "" && !flagSet {
		envfile = st.Envfile
	}

	if abs, err := filepath.Abs(envfile); err == nil {
		envfile = abs
	}

	return envfile
}

// expectedForState parses envfile and returns the managers expected with the
// supervisor's reconciler filter (all reconcilers without a state file).
func expectedForState(envfile string, st *supervisorState) (envFile, []managerKey) {
	var filter []string
	if st != nil {
		filter = st.Reconcilers
	}

	env, err := parseEnvFile(envfile)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: cannot compute expected managers: %v\n", err)

		return env, nil
	}

	expected, err := expectedManagers(env, filter)
	if err != nil {
		fmt.Fprintf(os.Stderr, "warning: cannot compute expected managers: %v\n", err)
	}

	return env, expected
}

func printStatusJSON(w io.Writer, r statusReport) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	if err := enc.Encode(r); err != nil {
		fmt.Fprintf(os.Stderr, "cannot encode status: %v\n", err)
	}
}

func printStatusText(w io.Writer, r statusReport) {
	fmt.Fprintf(w, "supervisor: %s\n", describeSupervisor(r.Supervisor))
	fmt.Fprintf(w, "env:        %s (%s)\n\n", r.Env, r.Envfile)

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0) //nolint:mnd
	fmt.Fprintln(tw, "CLUSTER\tRECONCILER\tPID\tSTATE\tLOG")

	for _, m := range r.Managers {
		pid := "-"
		if m.PID > 0 {
			pid = strconv.Itoa(m.PID)
		}

		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", m.Cluster, m.Reconciler, pid, m.state(), m.Log)
	}

	tw.Flush()
}

func describeSupervisor(s *supervisorStatus) string {
	switch {
	case s == nil:
		return "not running (no state file)"
	case s.Alive:
		return fmt.Sprintf("running, pid %d, started %s", s.PID, s.StartedAt)
	case s.StoppedAt != "":
		return fmt.Sprintf("not running, pid %d stopped %s", s.PID, s.StoppedAt)
	default:
		return fmt.Sprintf("dead, pid %d started %s and exited without recording a stop", s.PID, s.StartedAt)
	}
}
