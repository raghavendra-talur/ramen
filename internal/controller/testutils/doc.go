// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package testutils provides test utilities for the Ramen controller tests.
//
// IMPORTANT: Functions in this package MUST NOT use Ginkgo or Gomega directly.
// All functions should return errors that the test code can handle using
// Ginkgo's Expect() or other assertions.
//
// This separation ensures:
//   - Utility functions are reusable and testable independently
//   - Test assertions are explicit and visible in the test code
//   - Better error messages and debugging
//   - Cleaner test structure
package testutils
