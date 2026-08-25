// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors_test

import (
	"context"
	"encoding/json"
	"path/filepath"
	"slices"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"

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

	// --- field removal: an updated manifest that drops a spec field must
	// drop it on the managed cluster too. Ramen's DRPC relies on this to
	// clear spec.volSync.rdSpec after a failover restore (and the manifest
	// it writes carries `volSync: {}`, which server-side apply rejects on
	// the VRG CRD — the real OCM agent uses update semantics). Metadata
	// added on the managed side (ramen's own VRG finalizer) must survive.
	vrgManifest := func(withRDSpec bool) []byte {
		vrg := &rmn.VolumeReplicationGroup{
			TypeMeta:   metav1.TypeMeta{APIVersion: rmn.GroupVersion.String(), Kind: "VolumeReplicationGroup"},
			ObjectMeta: metav1.ObjectMeta{Name: "mw-vrg", Namespace: "default"},
			Spec: rmn.VolumeReplicationGroupSpec{
				PVCSelector:      metav1.LabelSelector{MatchLabels: map[string]string{"app": "x"}},
				ReplicationState: rmn.Primary,
				S3Profiles:       []string{"s3profile-mock"},
				Async:            &rmn.VRGAsyncSpec{SchedulingInterval: "1m"},
			},
		}
		if withRDSpec {
			vrg.Spec.VolSync.RDSpec = []rmn.VolSyncReplicationDestinationSpec{
				{ProtectedPVC: rmn.ProtectedPVC{Name: "data", Namespace: "default"}},
			}
		}

		raw, err := json.Marshal(vrg)
		if err != nil {
			t.Fatal(err)
		}

		return raw
	}

	vrgMW := &ocmworkv1.ManifestWork{
		ObjectMeta: metav1.ObjectMeta{Name: "test-vrg-mw", Namespace: "dr1"},
		Spec: ocmworkv1.ManifestWorkSpec{Workload: ocmworkv1.ManifestsTemplate{
			Manifests: []ocmworkv1.Manifest{{RawExtension: runtime.RawExtension{Raw: vrgManifest(true)}}},
		}},
	}
	if err := hub.Client.Create(ctx, vrgMW); err != nil {
		t.Fatal(err)
	}

	vrgKey := types.NamespacedName{Name: "mw-vrg", Namespace: "default"}
	eventually(t, 30*time.Second, func() bool {
		got := &rmn.VolumeReplicationGroup{}

		return dr1.Client.Get(ctx, vrgKey, got) == nil && len(got.Spec.VolSync.RDSpec) == 1
	}, "VRG with RDSpec never applied")

	// Simulate the managed-side controller marking its object.
	appliedVRG := &rmn.VolumeReplicationGroup{}
	if err := dr1.Client.Get(ctx, vrgKey, appliedVRG); err != nil {
		t.Fatal(err)
	}

	appliedVRG.Finalizers = append(appliedVRG.Finalizers, "ramendr.openshift.io/test-protection")
	if err := dr1.Client.Update(ctx, appliedVRG); err != nil {
		t.Fatal(err)
	}

	if err := hub.Client.Get(ctx, types.NamespacedName{Name: vrgMW.Name, Namespace: "dr1"}, vrgMW); err != nil {
		t.Fatal(err)
	}

	vrgMW.Spec.Workload.Manifests[0] = ocmworkv1.Manifest{RawExtension: runtime.RawExtension{Raw: vrgManifest(false)}}
	if err := hub.Client.Update(ctx, vrgMW); err != nil {
		t.Fatal(err)
	}

	eventually(t, 30*time.Second, func() bool {
		got := &rmn.VolumeReplicationGroup{}

		return dr1.Client.Get(ctx, vrgKey, got) == nil && len(got.Spec.VolSync.RDSpec) == 0
	}, "RDSpec never removed from applied VRG")

	got := &rmn.VolumeReplicationGroup{}
	if err := dr1.Client.Get(ctx, vrgKey, got); err != nil {
		t.Fatal(err)
	}

	if !slices.Contains(got.Finalizers, "ramendr.openshift.io/test-protection") {
		t.Fatal("managed-side finalizer clobbered by manifest apply")
	}
}
