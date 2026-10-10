// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"reflect"
	"strings"
	"testing"
)

func TestBuildCommandArgs(t *testing.T) {
	cmd := buildCommand("/src/ramen", "/src/ramen/bin/manager")

	want := []string{"go", "build", "-o", "/src/ramen/bin/manager", "./cmd"}
	if !reflect.DeepEqual(cmd.Args, want) {
		t.Errorf("args = %v, want %v", cmd.Args, want)
	}

	if cmd.Dir != "/src/ramen" {
		t.Errorf("dir = %q, want %q", cmd.Dir, "/src/ramen")
	}
}

// TestBuildCommandDisablesCgo verifies the manager is built without cgo, like
// the container image, so the build does not depend on the host C toolchain,
// even when the caller's environment enables cgo.
func TestBuildCommandDisablesCgo(t *testing.T) {
	t.Setenv("CGO_ENABLED", "1")

	cmd := buildCommand("/src/ramen", "/src/ramen/bin/manager")

	// exec uses the last value when a variable is set more than once.
	got := ""

	for _, kv := range cmd.Env {
		if v, ok := strings.CutPrefix(kv, "CGO_ENABLED="); ok {
			got = v
		}
	}

	if got != "0" {
		t.Errorf("CGO_ENABLED = %q, want %q", got, "0")
	}
}
