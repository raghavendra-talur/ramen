// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"path/filepath"
)

// set variables for the test environment
var (
	testbinDir string
	// binDir            string
	testassets        string
	crdDirectoryPaths []string
)

func init() {
	moduleRoot, err := FindModuleRoot()
	if err != nil {
		panic(err)
	}

	testbinDir = filepath.Join(moduleRoot, "testbin")
	testassets = filepath.Join(testbinDir, "testassets.txt")
	// binDir = filepath.Join(moduleRoot, "bin")
	crdDirectoryPaths = []string{
		filepath.Join(moduleRoot, "config", "crd", "bases"),
		filepath.Join(moduleRoot, "hack", "test"),
	}
}
