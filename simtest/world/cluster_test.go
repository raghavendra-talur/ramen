// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"context"
	"os"
	"testing"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
)

func TestStartCluster(t *testing.T) {
	EnsureAssets(t)

	c, err := StartCluster("dr1", t.TempDir())
	if err != nil {
		t.Fatalf("StartCluster: %v", err)
	}
	t.Cleanup(func() { _ = c.Stop() })

	if _, err := os.Stat(c.KubeconfigPath); err != nil {
		t.Fatalf("kubeconfig not written: %v", err)
	}

	// Ramen CRDs must be installed and typed client usable.
	vrg := &rmn.VolumeReplicationGroup{}
	err = c.Client.Get(context.Background(), types.NamespacedName{Name: "nope", Namespace: "default"}, vrg)
	if err == nil || !isNotFound(err) {
		t.Fatalf("expected NotFound for missing VRG (CRD installed), got: %v", err)
	}
}
