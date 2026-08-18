// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"
)

// TestUISmoke verifies that a UI-enabled world serves the page and that the
// snapshot reflects live world state. It self-skips unless SIMTEST_UI is
// set, so the default suite run is unaffected.
func TestUISmoke(t *testing.T) {
	w, _ := getWorld(t)
	if w.UI == nil {
		t.Skip("SIMTEST_UI not set; run: SIMTEST_UI=1 go test ./tests/ -run TestUISmoke")
	}

	resp, err := http.Get(w.UI.URL() + "/")
	if err != nil {
		t.Fatalf("get /: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("index status %d", resp.StatusCode)
	}

	uiScenario(t, w, "ui-smoke")

	// Managers are polled every 500ms; wait for them to appear.
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(w.UI.URL() + "/api/snapshot")
		if err != nil {
			t.Fatalf("get snapshot: %v", err)
		}
		var snap struct {
			Current  string          `json:"current"`
			Managers map[string]bool `json:"managers"`
		}
		err = json.NewDecoder(resp.Body).Decode(&snap)
		resp.Body.Close()
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if snap.Current == "ui-smoke" && len(snap.Managers) == 3 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("snapshot never converged: %+v", snap)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
