// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package ui

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

func TestEnabled(t *testing.T) {
	t.Setenv("SIMTEST_UI", "")
	if Enabled() {
		t.Fatal("enabled with empty SIMTEST_UI")
	}
	t.Setenv("SIMTEST_UI", "1")
	if !Enabled() {
		t.Fatal("not enabled with SIMTEST_UI=1")
	}
}

func TestLaunchServesAndPersists(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// No clusters: Launch with zero ClusterRefs still serves and persists.
	u, err := Launch(ctx, Options{Addr: "1", Dir: dir})
	if err != nil {
		t.Fatalf("launch: %v", err)
	}
	defer u.Close()

	resp, err := http.Get(u.URL() + "/api/snapshot")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	resp.Body.Close()

	u.Hub.ScenarioStart("s1")
	u.Close() // flushes the persist goroutine

	if _, err := os.Stat(filepath.Join(dir, "ui-events.jsonl")); err != nil {
		t.Fatalf("ui-events.jsonl missing: %v", err)
	}
}
