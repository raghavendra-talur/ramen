// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"path/filepath"
	"strings"
	"testing"
)

// The repo's hack/test CRD for the public groupsnapshot.storage.k8s.io group
// serves only v1beta1, while the ramen binary's public VGS client is v1.
// Ramen prefers the public API whenever that CRD exists, so installing it
// makes the drclusterconfig informer cache-sync time out and kills the
// dr-cluster manager two minutes into every run. The world must therefore
// install everything from hack/test EXCEPT the public groupsnapshot CRDs,
// leaving the private openshift.io variant whose version matches.
func TestHackTestCRDPathsExcludePublicGroupSnapshot(t *testing.T) {
	paths, err := hackTestCRDPaths()
	if err != nil {
		t.Fatal(err)
	}
	if len(paths) == 0 {
		t.Fatal("no CRD paths returned from hack/test")
	}

	var sawPrivate, sawOther bool
	for _, p := range paths {
		base := filepath.Base(p)
		if strings.HasPrefix(base, "groupsnapshot.storage.k8s.io_") {
			t.Errorf("public groupsnapshot CRD must be excluded: %s", p)
		}
		if strings.HasPrefix(base, "groupsnapshot.storage.openshift.io_") {
			sawPrivate = true
		}
		if strings.HasPrefix(base, "recipes.ramendr.openshift.io") {
			sawOther = true
		}
	}
	if !sawPrivate {
		t.Error("private openshift.io groupsnapshot CRDs must remain")
	}
	if !sawOther {
		t.Error("unrelated hack/test CRDs (e.g. recipes) must remain")
	}
}
