// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRepoRoot(t *testing.T) {
	root := RepoRoot()
	for _, p := range []string{"config/crd/bases", "hack/test", "cmd/main.go"} {
		if _, err := os.Stat(filepath.Join(root, p)); err != nil {
			t.Fatalf("RepoRoot()=%q missing %s: %v", root, p, err)
		}
	}
}
