// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"fmt"
	"os"
)

// BackendKind selects how the world's three clusters are provided.
type BackendKind string

const (
	// BackendEnvtest (the default) boots etcd + kube-apiserver per cluster:
	// fast and deterministic, but nothing beyond the apiserver runs — the
	// janitor actor stands in for kube-controller-manager.
	BackendEnvtest BackendKind = "envtest"

	// BackendKind_ boots one kind cluster per world cluster: a real
	// kube-controller-manager (garbage collector, protection finalizers,
	// namespace controller) runs, and the janitor does not — the absence is
	// itself a test, catching ramen bugs the janitor's sweeps would mask.
	// Requires the kind binary and a docker or podman runtime; slower to
	// boot (~40s/cluster) and subject to real-controller timing.
	BackendKind_ BackendKind = "kind"
)

// Backend reads SIMTEST_BACKEND; empty means envtest.
func Backend() (BackendKind, error) {
	switch v := os.Getenv("SIMTEST_BACKEND"); v {
	case "", string(BackendEnvtest):
		return BackendEnvtest, nil
	case string(BackendKind_):
		return BackendKind_, nil
	default:
		return "", fmt.Errorf("SIMTEST_BACKEND=%q: unknown backend (want envtest or kind)", v)
	}
}
