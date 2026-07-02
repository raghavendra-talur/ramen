// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ocmworkv1 "open-cluster-management.io/api/work/v1"
	viewv1beta1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/view/v1beta1"

	"github.com/ramendr/ramen/simtest/actors"
	"github.com/ramendr/ramen/simtest/world"
)

func eventually(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestOCMAgent(t *testing.T) {
	world.EnsureAssets(t)

	dir := t.TempDir()
	hub, err := world.StartCluster("hub", dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = hub.Stop() })

	dr1, err := world.StartCluster("dr1", dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = dr1.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	log, _ := actors.NewEvLog(filepath.Join(dir, "events.log"))
	_, err = actors.Start(ctx, world.NewScheme(),
		actors.ClusterRef{Name: hub.Name, Cfg: hub.Cfg},
		[]actors.ClusterRef{{Name: dr1.Name, Cfg: dr1.Cfg}}, log)
	if err != nil {
		t.Fatal(err)
	}

	// MW namespace on hub = managed cluster name.
	if err := hub.Client.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "dr1"}}); err != nil {
		t.Fatal(err)
	}

	// --- work agent: MW carrying a ConfigMap manifest ---
	cm := &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Name: "from-mw", Namespace: "default"},
		Data:       map[string]string{"k": "v"},
	}
	raw, _ := json.Marshal(cm)
	mw := &ocmworkv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{Name: "test-default-cm-mw", Namespace: "dr1"},
		Spec: ocmworkv1.ManifestWorkSpec{Workload: ocmworkv1.ManifestsTemplate{
			Manifests: []ocmworkv1.Manifest{{RawExtension: runtime.RawExtension{Raw: raw}}},
		}},
	}
	if err := hub.Client.Create(ctx, mw); err != nil {
		t.Fatal(err)
	}

	eventually(t, 30*time.Second, func() bool {
		got := &corev1.ConfigMap{}

		return dr1.Client.Get(ctx, types.NamespacedName{Name: "from-mw", Namespace: "default"}, got) == nil
	}, "manifest never applied to managed cluster")

	eventually(t, 30*time.Second, func() bool {
		_ = hub.Client.Get(ctx, types.NamespacedName{Name: mw.Name, Namespace: "dr1"}, mw)

		return meta.IsStatusConditionTrue(mw.Status.Conditions, ocmworkv1.WorkApplied) &&
			meta.IsStatusConditionTrue(mw.Status.Conditions, ocmworkv1.WorkAvailable)
	}, "MW status never Applied+Available")

	// --- view agent: read that ConfigMap back ---
	mcv := &viewv1beta1.ManagedClusterView{
		ObjectMeta: metav1.ObjectMeta{Name: "test-default-cm-mcv", Namespace: "dr1"},
		Spec: viewv1beta1.ViewSpec{Scope: viewv1beta1.ViewScope{
			Kind: "ConfigMap", Version: "v1", Name: "from-mw", Namespace: "default",
		}},
	}
	if err := hub.Client.Create(ctx, mcv); err != nil {
		t.Fatal(err)
	}

	eventually(t, 30*time.Second, func() bool {
		_ = hub.Client.Get(ctx, types.NamespacedName{Name: mcv.Name, Namespace: "dr1"}, mcv)
		if len(mcv.Status.Conditions) != 1 || mcv.Status.Conditions[0].Type != viewv1beta1.ConditionViewProcessing {
			return false
		}

		return mcv.Status.Conditions[0].Status == metav1.ConditionTrue && len(mcv.Status.Result.Raw) > 0
	}, "MCV never fulfilled")

	// --- MW deletion removes the applied object ---
	if err := hub.Client.Delete(ctx, mw); err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, func() bool {
		got := &corev1.ConfigMap{}
		err := dr1.Client.Get(ctx, types.NamespacedName{Name: "from-mw", Namespace: "default"}, got)

		return err != nil
	}, "applied object not cleaned up on MW delete")
}
