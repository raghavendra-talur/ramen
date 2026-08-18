// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"bufio"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

func startTestServer(t *testing.T) (*Hub, *Server) {
	t.Helper()
	h := New()
	s, err := Serve(h, "")
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
