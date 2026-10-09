// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"slices"
	"testing"
)

func TestAddonsJSONIsOnlyDocument(t *testing.T) {
	cmd := newAddonsCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--json"})
	if err := cmd.Execute(); err != nil {
		t.Fatalf("addons --json: %v", err)
	}

	var got addonsReport
	dec := json.NewDecoder(&out)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&got); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if dec.More() {
		t.Fatal("stdout holds more than one JSON document")
	}
	var names []string
	for _, a := range got.Addons {
		names = append(names, a.Name)
	}
	if !slices.IsSorted(names) || !slices.Contains(names, "rook-operator") {
		t.Fatalf("addons = %v, want sorted list including rook-operator", names)
	}
}

func TestRunnerForRoutesJSONOutputToStderr(t *testing.T) {
	if r := runnerFor(true); r.Stdout != os.Stderr {
		t.Errorf("json runner Stdout = %v, want os.Stderr", r.Stdout)
	}
	if r := runnerFor(false); r.Stdout != nil {
		t.Errorf("text runner Stdout = %v, want nil (os.Stdout)", r.Stdout)
	}
}
