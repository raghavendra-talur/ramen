// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// RepoRoot returns the ramen repository root (parent of simtest/).
func RepoRoot() string {
	_, thisFile, _, _ := runtime.Caller(0)

	return filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", ".."))
}

// EnsureAssets sets KUBEBUILDER_ASSETS from <repo>/testbin/testassets.txt,
// mirroring internal/controller/suite_test.go. Skips the test with guidance
// if assets are missing.
func EnsureAssets(t *testing.T) {
	t.Helper()

	if _, set := os.LookupEnv("KUBEBUILDER_ASSETS"); set {
		return
	}

	content, err := os.ReadFile(filepath.Join(RepoRoot(), "testbin", "testassets.txt"))
	if err != nil {
		t.Skipf("envtest assets missing, run 'make assets' in simtest/: %v", err)
	}

	t.Setenv("KUBEBUILDER_ASSETS", strings.TrimSpace(string(content)))
}

// ManagerBin returns the path of the ramen manager binary.
func ManagerBin() string {
	return filepath.Join(RepoRoot(), "bin", "manager")
}
