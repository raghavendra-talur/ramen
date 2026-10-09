// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
)

const testBin = "/repo/localrun/bin/manager"

func testEnv() envFile {
	var env envFile

	env.Name = "rdr"
	env.Ramen.Hub = "hub"
	env.Ramen.Clusters = []string{"dr1", "dr2"}

	return env
}

// fakeProcs maps pid to command line; listed pids are alive.
type fakeProcs map[int]string

func (f fakeProcs) Alive(pid int) bool {
	_, ok := f[pid]

	return ok
}

func (f fakeProcs) Command(pid int) string { return f[pid] }

// fakeProber reports ready unless the address has an error.
type fakeProber struct {
	mu     sync.Mutex
	errs   map[string]error
	probed []string
}

func (f *fakeProber) Ready(_ context.Context, addr string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.probed = append(f.probed, addr)

	return f.errs[addr]
}

func managerCmd(addr string) string {
	return testBin + " --kubeconfig=/kc --health-probe-bind-address=" + addr + " --leader-elect=false"
}

func TestParseReconcilerList(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"", []string{}},
		{"vrg", []string{"vrg"}},
		{" vrg , pvrgl", []string{"vrg", "pvrgl"}},
		{"vrg,,", []string{"vrg"}},
	}

	for _, c := range cases {
		if got := parseReconcilerList(c.in); !reflect.DeepEqual(got, c.want) {
			t.Errorf("parseReconcilerList(%q) = %#v, want %#v", c.in, got, c.want)
		}
	}
}

func TestExpectedManagersAll(t *testing.T) {
	keys, err := expectedManagers(testEnv(), nil)
	if err != nil {
		t.Fatal(err)
	}

	if len(keys) != len(hubReconcilers)+2*len(drClusterReconcilers) {
		t.Fatalf("got %d expected managers: %+v", len(keys), keys)
	}

	first := managerKey{cluster: "hub", reconciler: "drpolicy", healthAddr: "127.0.0.1:9410"}
	if keys[0] != first {
		t.Errorf("first = %+v, want %+v", keys[0], first)
	}

	last := managerKey{cluster: "dr2", reconciler: "rgs", healthAddr: "127.0.0.1:9436"}
	if keys[len(keys)-1] != last {
		t.Errorf("last = %+v, want %+v", keys[len(keys)-1], last)
	}
}

func TestExpectedManagersFiltered(t *testing.T) {
	keys, err := expectedManagers(testEnv(), []string{"vrg", "drpc"})
	if err != nil {
		t.Fatal(err)
	}

	want := []managerKey{
		{cluster: "hub", reconciler: "drpc", healthAddr: "127.0.0.1:9412"},
		{cluster: "dr1", reconciler: "vrg", healthAddr: "127.0.0.1:9424"},
		{cluster: "dr2", reconciler: "vrg", healthAddr: "127.0.0.1:9434"},
	}

	if !reflect.DeepEqual(keys, want) {
		t.Fatalf("got %+v, want %+v", keys, want)
	}
}

func TestMergeManagers(t *testing.T) {
	expected := []managerKey{
		{cluster: "hub", reconciler: "drpc", healthAddr: "a"},
		{cluster: "dr1", reconciler: "vrg", healthAddr: "b"},
	}
	records := []managerState{
		{Cluster: "hub", Reconciler: "drpc", PID: 1, StartedAt: "t1"},
		{Cluster: "dr9", Reconciler: "vrg", PID: 3, StartedAt: "t1"},
		{Cluster: "hub", Reconciler: "drpc", PID: 2, StartedAt: "t2"},
	}

	got := mergeManagers(expected, records)

	want := []managerStatus{
		{Cluster: "hub", Reconciler: "drpc", Expected: true, PID: 2, StartedAt: "t2"},
		{Cluster: "dr1", Reconciler: "vrg", Expected: true, HealthAddr: "b"},
		{Cluster: "dr9", Reconciler: "vrg", Expected: false, PID: 3, StartedAt: "t1"},
	}

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestManagerStatusState(t *testing.T) {
	cases := []struct {
		m    managerStatus
		want string
	}{
		{managerStatus{Alive: true, Ready: true, StartedAt: "t"}, "ready"},
		{managerStatus{Alive: true, StartedAt: "t"}, "not-ready"},
		{managerStatus{PID: 5, StartedAt: "t"}, "dead"},
		{managerStatus{StartedAt: "t", ExitErr: "start failed"}, "dead"},
		{managerStatus{}, "missing"},
	}

	for _, c := range cases {
		if got := c.m.state(); got != c.want {
			t.Errorf("%+v: got %q, want %q", c.m, got, c.want)
		}
	}
}

func TestIsManagerProcess(t *testing.T) {
	procs := fakeProcs{
		10: managerCmd("127.0.0.1:9412"),
		11: "/usr/bin/vim notes.txt",                 // pid reused by another program
		12: managerCmd("127.0.0.1:9424"),             // pid reused by another manager
		13: "/other/repo/localrun/bin/manager --x=1", // another checkout
	}

	cases := []struct {
		pid  int
		want bool
	}{
		{10, true},
		{11, false},
		{12, false},
		{13, false},
		{14, false}, // not running
		{0, false},
	}

	for _, c := range cases {
		if got := isManagerProcess(procs, testBin, c.pid, "127.0.0.1:9412"); got != c.want {
			t.Errorf("pid %d: got %v, want %v", c.pid, got, c.want)
		}
	}
}

func TestReportNoStateFile(t *testing.T) {
	expected, err := expectedManagers(testEnv(), nil)
	if err != nil {
		t.Fatal(err)
	}

	prober := &fakeProber{}
	checker := statusChecker{bin: testBin, procs: fakeProcs{}, prober: prober}

	r := checker.report(context.Background(), nil, expected)

	if r.Supervisor != nil {
		t.Errorf("supervisor = %+v, want nil", r.Supervisor)
	}

	if len(r.Managers) != 13 {
		t.Fatalf("got %d managers, want 13", len(r.Managers))
	}

	for _, m := range r.Managers {
		if !m.Expected || m.Alive || m.state() != "missing" {
			t.Errorf("manager %+v, want expected and missing", m)
		}
	}

	if len(prober.probed) != 0 {
		t.Errorf("probed %v, want no probes", prober.probed)
	}
}

func TestReportClassifiesManagers(t *testing.T) {
	expected := []managerKey{
		{cluster: "hub", reconciler: "drpolicy", healthAddr: "127.0.0.1:9410"},
		{cluster: "hub", reconciler: "drcluster", healthAddr: "127.0.0.1:9411"},
		{cluster: "hub", reconciler: "drpc", healthAddr: "127.0.0.1:9412"},
		{cluster: "dr1", reconciler: "vrg", healthAddr: "127.0.0.1:9424"},
	}
	st := &supervisorState{
		PID: 100, StartedAt: "t0", Env: "rdr", Envfile: "/abs/env.yaml",
		Managers: []managerState{
			{Cluster: "hub", Reconciler: "drpolicy", PID: 101, HealthAddr: "127.0.0.1:9410", StartedAt: "t1"},
			{Cluster: "hub", Reconciler: "drcluster", PID: 102, HealthAddr: "127.0.0.1:9411", StartedAt: "t1"},
			{Cluster: "hub", Reconciler: "drpc", PID: 103, HealthAddr: "127.0.0.1:9412", StartedAt: "t1"},
		},
	}
	procs := fakeProcs{
		100: "/tmp/go-build/exe/localrun run",
		101: managerCmd("127.0.0.1:9410"),
		102: managerCmd("127.0.0.1:9411"),
		103: "/bin/sleep 100", // reused pid
	}
	prober := &fakeProber{errs: map[string]error{"127.0.0.1:9411": errors.New("HTTP 500: not ready")}}
	checker := statusChecker{bin: testBin, procs: procs, prober: prober}

	r := checker.report(context.Background(), st, expected)

	want := &supervisorStatus{PID: 100, Alive: true, StartedAt: "t0"}
	if !reflect.DeepEqual(r.Supervisor, want) {
		t.Errorf("supervisor = %+v, want %+v", r.Supervisor, want)
	}

	states := make([]string, 0, len(r.Managers))
	for _, m := range r.Managers {
		states = append(states, m.state())
	}

	if got := strings.Join(states, ","); got != "ready,not-ready,dead,missing" {
		t.Errorf("states = %s", got)
	}

	if r.Managers[1].ReadyError != "HTTP 500: not ready" {
		t.Errorf("readyError = %q", r.Managers[1].ReadyError)
	}

	if len(prober.probed) != 2 {
		t.Errorf("probed %v, want only the 2 alive managers", prober.probed)
	}
}

func TestReportSupervisorPidReused(t *testing.T) {
	st := &supervisorState{PID: 100, StartedAt: "t0"}
	checker := statusChecker{bin: testBin, procs: fakeProcs{100: "/usr/bin/vim"}, prober: &fakeProber{}}

	r := checker.report(context.Background(), st, nil)

	if r.Supervisor == nil || r.Supervisor.Alive {
		t.Fatalf("supervisor = %+v, want present and dead", r.Supervisor)
	}
}

func TestStatusJSONShape(t *testing.T) {
	var buf bytes.Buffer

	printStatusJSON(&buf, statusReport{StateFile: "/s", Managers: []managerStatus{{}}})

	var raw map[string]any
	if err := json.Unmarshal(buf.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}

	assertKeys(t, raw, "stateFile", "supervisor", "env", "envfile", "managers")

	if raw["supervisor"] != nil {
		t.Errorf("supervisor = %v, want null", raw["supervisor"])
	}

	managers, ok := raw["managers"].([]any)
	if !ok || len(managers) != 1 {
		t.Fatalf("managers: %v", raw["managers"])
	}

	m, ok := managers[0].(map[string]any)
	if !ok {
		t.Fatalf("manager: %v", managers[0])
	}

	assertKeys(t, m, "cluster", "reconciler", "expected", "pid", "alive", "ready", "readyError",
		"healthAddr", "log", "startedAt", "exitedAt", "exitErr")
}

func TestPrintStatusText(t *testing.T) {
	var buf bytes.Buffer

	printStatusText(&buf, statusReport{
		Env: "rdr",
		Managers: []managerStatus{
			{Cluster: "hub", Reconciler: "drpc", PID: 7, Alive: true, Ready: true, StartedAt: "t", Log: "/l"},
			{Cluster: "dr1", Reconciler: "vrg"},
		},
	})

	out := buf.String()
	for _, want := range []string{"no state file", "hub  ", "7", "ready", "/l", "missing"} {
		if !strings.Contains(out, want) {
			t.Errorf("output missing %q:\n%s", want, out)
		}
	}
}

func TestHTTPProber(t *testing.T) {
	healthy := true

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/readyz" {
			http.NotFound(w, r)

			return
		}

		if !healthy {
			http.Error(w, "readyz check failed", http.StatusInternalServerError)
		}
	}))
	defer srv.Close()

	p := httpProber{client: srv.Client()}
	addr := strings.TrimPrefix(srv.URL, "http://")

	if err := p.Ready(context.Background(), addr); err != nil {
		t.Errorf("healthy: %v", err)
	}

	healthy = false

	err := p.Ready(context.Background(), addr)
	if err == nil || !strings.Contains(err.Error(), "HTTP 500: readyz check failed") {
		t.Errorf("unhealthy: got %v", err)
	}
}
