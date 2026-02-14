// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import "github.com/onsi/gomega/format"

// ConfigureGinkgo sets up Ginkgo/Gomega defaults for test output.
// This is the ONLY function in testutils that should interact with Ginkgo/Gomega.
// It should be called from BeforeSuite in the test files.
func ConfigureGinkgo() {
	// Disable output truncation for better error messages
	// onsi.github.io/gomega/#adjusting-output
	format.MaxLength = 0
}
