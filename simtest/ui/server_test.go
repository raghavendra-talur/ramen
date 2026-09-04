// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (*Hub, *Server) {
	t.Helper()
	h := New()
	s, err := Serve(h, "", "")
	if err != nil {
		t.Fatalf("serve: %v", err)
	}
	t.Cleanup(s.Close)
	return h, s
}

func TestIndexServes(t *testing.T) {
	_, s := startTestServer(t)
	resp, err := http.Get(s.URL() + "/")
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/html") {
		t.Fatalf("content-type %q", ct)
	}
}

func TestSnapshotEndpoint(t *testing.T) {
	h, s := startTestServer(t)
	h.RunStart("run")
	h.ScenarioStart("s1")

	resp, err := http.Get(s.URL() + "/api/snapshot")
	if err != nil {
		t.Fatalf("get snapshot: %v", err)
	}
	defer resp.Body.Close()
	var snap Snapshot
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if snap.Run.Name != "run" || snap.Current != "s1" || len(snap.Tests) != 1 {
		t.Fatalf("snapshot: %+v", snap)
	}
}

func TestObjectEndpoint(t *testing.T) {
	h, s := startTestServer(t)
	h.ObserveObject(ObjectState{
		Cluster: "dr1", Kind: "VolumeReplicationGroup",
		Namespace: "app", Name: "vrg",
		Fields: map[string]string{"state": "primary"},
		Raw: json.RawMessage(`{"metadata":{"name":"vrg","namespace":"app"},` +
			`"status":{"conditions":[{"type":"DataReady","status":"True","reason":"Ready"}]}}`),
	})

	resp, err := http.Get(s.URL() +
		"/api/object?cluster=dr1&kind=VolumeReplicationGroup&namespace=app&name=vrg")
	if err != nil {
		t.Fatalf("get object: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var d ObjectDetail
	if err := json.NewDecoder(resp.Body).Decode(&d); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if d.Kind != "VolumeReplicationGroup" || d.Name != "vrg" {
		t.Fatalf("detail: %+v", d)
	}
	if len(d.Conditions) != 1 || d.Conditions[0].Type != "DataReady" ||
		d.Conditions[0].Status != "True" {
		t.Fatalf("conditions: %+v", d.Conditions)
	}
	if !strings.Contains(d.YAML, "name: vrg") || !strings.Contains(d.YAML, "type: DataReady") {
		t.Fatalf("yaml: %q", d.YAML)
	}
}

func TestObjectEndpointUnknown(t *testing.T) {
	h, s := startTestServer(t)
	// Known object without raw JSON must 404 too, not serve an empty drawer.
	h.ObserveObject(ObjectState{Cluster: "dr1", Kind: "PersistentVolumeClaim",
		Namespace: "app", Name: "bare", Fields: map[string]string{}})

	for _, q := range []string{
		"cluster=dr1&kind=PersistentVolumeClaim&namespace=app&name=missing",
		"cluster=dr1&kind=PersistentVolumeClaim&namespace=app&name=bare",
	} {
		resp, err := http.Get(s.URL() + "/api/object?" + q)
		if err != nil {
			t.Fatalf("get object: %v", err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusNotFound {
			t.Fatalf("%s: status %d, want 404", q, resp.StatusCode)
		}
	}
}

func TestStreamDeliversEvents(t *testing.T) {
	h, s := startTestServer(t)

	resp, err := http.Get(s.URL() + "/api/stream")
	if err != nil {
		t.Fatalf("get stream: %v", err)
	}
	defer resp.Body.Close()
	if ct := resp.Header.Get("Content-Type"); !strings.Contains(ct, "text/event-stream") {
		t.Fatalf("content-type %q", ct)
	}

	// The handler subscribes asynchronously; poll until the event arrives.
	go func() {
		for i := 0; i < 50; i++ {
			h.ScenarioStart("s1")
			time.Sleep(20 * time.Millisecond)
		}
	}()

	r := bufio.NewReader(resp.Body)
	deadline := time.After(5 * time.Second)
	lines := make(chan string, 16)
	go func() {
		for {
			l, err := r.ReadString('\n')
			if err != nil {
				return
			}
			lines <- strings.TrimRight(l, "\n")
		}
	}()

	var sawEventLine, sawDataLine bool
	for !(sawEventLine && sawDataLine) {
		select {
		case l := <-lines:
			if l == "event: test_started" {
				sawEventLine = true
			}
			if strings.HasPrefix(l, "data: ") {
				var ev Event
				if err := json.Unmarshal([]byte(strings.TrimPrefix(l, "data: ")), &ev); err != nil {
					t.Fatalf("bad data line %q: %v", l, err)
				}
				if ev.Type == "test_started" {
					sawDataLine = true
				}
			}
		case <-deadline:
			t.Fatal("no SSE event within deadline")
		}
	}
}

func TestIndexHasAppRegions(t *testing.T) {
	_, s := startTestServer(t)
	resp, err := http.Get(s.URL() + "/")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body := new(strings.Builder)
	if _, err := io.Copy(body, resp.Body); err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{`id="run"`, `id="tests"`, `id="stage"`, `id="inspector"`, `id="timeline"`} {
		if !strings.Contains(body.String(), id) {
			t.Fatalf("index.html missing %s", id)
		}
	}
}

// The logs endpoint serves a bounded tail of the run's log files (the ramen
// manager logs above all), by whitelisted source name only.
func TestLogsEndpoint(t *testing.T) {
	dir := t.TempDir()
	lines := ""
	for i := 1; i <= 50; i++ {
		lines += fmt.Sprintf("2026-08-26T01:02:%02d.000-0400\tINFO\tvrg\tctrl/x.go:%d\tline %d\n", i%60, i, i)
	}
	if err := os.WriteFile(filepath.Join(dir, "dr1.log"), []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Serve(New(), "", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	get := func(path string) (int, string) {
		resp, err := http.Get(s.URL() + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)

		return resp.StatusCode, string(b)
	}

	code, body := get("/api/logs?src=dr1&tail=10")
	if code != 200 {
		t.Fatalf("status %d", code)
	}
	got := strings.Split(strings.TrimRight(body, "\n"), "\n")
	if len(got) != 10 || !strings.HasSuffix(got[9], "line 50") || !strings.HasSuffix(got[0], "line 41") {
		t.Fatalf("tail wrong: %d lines, first %q last %q", len(got), got[0], got[len(got)-1])
	}

	if code, _ := get("/api/logs?src=../../etc/passwd"); code != 400 {
		t.Fatalf("traversal not rejected: %d", code)
	}
	if code, _ := get("/api/logs?src=hub"); code != 404 {
		t.Fatalf("missing file should 404: %d", code)
	}
}

// Because the framework runs the tests itself, every scenario has exact
// start/finish timestamps — the logs endpoint aligns to them: since/until
// select the window (scanning the whole file, not just the tail), and
// untimestamped continuation lines (stack traces) stick with the
// timestamped line before them.
func TestLogsEndpointWindow(t *testing.T) {
	dir := t.TempDir()
	log := "2026-08-26T01:00:05.000-0400\tINFO\tvrg\tc/x.go:1\tbefore\n" +
		"2026-08-26T01:00:10.000-0400\tINFO\tvrg\tc/x.go:2\tinside one\n" +
		"goroutine 12 [running]: continuation line\n" +
		"2026-08-26T01:00:20.000-0400\tINFO\tvrg\tc/x.go:3\tinside two\n" +
		"2026-08-26T01:00:40.000-0400\tINFO\tvrg\tc/x.go:4\tafter\n"
	if err := os.WriteFile(filepath.Join(dir, "hub.log"), []byte(log), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Serve(New(), "", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	resp, err := http.Get(s.URL() +
		"/api/logs?src=hub&since=2026-08-26T05:00:08Z&until=2026-08-26T05:00:30Z")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)

	got := strings.Split(strings.TrimRight(string(b), "\n"), "\n")
	want := []string{"inside one", "continuation", "inside two"}
	if len(got) != 3 {
		t.Fatalf("got %d lines: %q", len(got), got)
	}
	for i, w := range want {
		if !strings.Contains(got[i], w) {
			t.Fatalf("line %d = %q, want containing %q", i, got[i], w)
		}
	}
}

// Scenario alignment for events: the live snapshot holds only a bounded
// ring, but every event is persisted to ui-events.jsonl — the events
// endpoint serves a scenario's exact window from there.
func TestEventsEndpointWindow(t *testing.T) {
	dir := t.TempDir()
	mk := func(sec int, line string) string {
		return fmt.Sprintf(`{"seq":%d,"at":"2026-08-26T01:00:%02dZ","type":"actor_event","data":{"line":"%s"}}`,
			sec, sec, line) + "\n"
	}
	blob := mk(5, "before") + mk(10, "inside one") + mk(20, "inside two") + mk(40, "after")
	if err := os.WriteFile(filepath.Join(dir, "ui-events.jsonl"), []byte(blob), 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := Serve(New(), "", dir)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	resp, err := http.Get(s.URL() +
		"/api/events?since=2026-08-26T01:00:08Z&until=2026-08-26T01:00:30Z")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	var evs []Event
	if err := json.NewDecoder(resp.Body).Decode(&evs); err != nil {
		t.Fatal(err)
	}
	if len(evs) != 2 || evs[0].Data["line"] != "inside one" || evs[1].Data["line"] != "inside two" {
		t.Fatalf("window wrong: %+v", evs)
	}
}
