// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestPersistWritesEventLines(t *testing.T) {
	h := New()
	path := filepath.Join(t.TempDir(), "ui-events.jsonl")
	stop, err := StartPersist(h, path)
	if err != nil {
		t.Fatalf("start persist: %v", err)
	}

	h.ScenarioStart("s1")
	h.ScenarioEnd("s1", "passed", "")

	// The writer goroutine is async; poll for both lines.
	deadline := time.Now().Add(5 * time.Second)
	var types []string
	for time.Now().Before(deadline) {
		types = types[:0]
		f, err := os.Open(path)
		if err == nil {
			sc := bufio.NewScanner(f)
			for sc.Scan() {
				var ev Event
				if err := json.Unmarshal(sc.Bytes(), &ev); err != nil {
					t.Fatalf("bad line %q: %v", sc.Text(), err)
				}
				types = append(types, ev.Type)
			}
			f.Close()
		}
		if len(types) >= 2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	stop()

	if len(types) < 2 || types[0] != "test_started" || types[1] != "test_finished" {
		t.Fatalf("persisted types: %v", types)
	}
}
