// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package user

import (
	"context"
	"fmt"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/ptr"
	clrapiv1beta1 "open-cluster-management.io/api/cluster/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/simtest/world"
)

type App struct{ Name string }

func (a App) Namespace() string           { return a.Name + "-ns" }
func (a App) PVCName() string             { return a.Name + "-data" }
func (a App) ManagementNamespace() string { return world.RamenOpsNS }

// CreateApp creates the app namespace on both managed clusters (failover
// targets need it) and a labeled PVC on the given cluster. No pods: envtest
// has no kubelet, and the VRG in-use checks pass with none.
func CreateApp(ctx context.Context, w *world.World, app App, cluster string) error {
	for _, m := range w.Managed() {
		ns := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: app.Namespace()}}
		if err := client.IgnoreAlreadyExists(m.Client.Create(ctx, ns)); err != nil {
			return err
		}
	}

	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: app.PVCName(), Namespace: app.Namespace(),
			Labels: map[string]string{world.AppLabelKey: app.Name},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: ptr.To(world.StorageClassName),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
	}

	return client.IgnoreAlreadyExists(w.Cluster(cluster).Client.Create(ctx, pvc))
}

// DeleteApp removes the app PVC on a cluster — the manual cleanup ramen waits
// for at ProgressionWaitOnUserToCleanUp for discovered apps.
func DeleteApp(ctx context.Context, w *world.World, app App, cluster string) error {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: app.PVCName(), Namespace: app.Namespace()},
	}

	err := w.Cluster(cluster).Client.Delete(ctx, pvc)
	if errors.IsNotFound(err) {
		return nil
	}

	return err
}

func EnableProtection(ctx context.Context, w *world.World, app App) error {
	placement := &clrapiv1beta1.Placement{
		ObjectMeta: metav1.ObjectMeta{
			Name: app.Name, Namespace: app.ManagementNamespace(),
			Annotations: map[string]string{world.OcmSchedulingDisable: "true"},
		},
		Spec: clrapiv1beta1.PlacementSpec{NumberOfClusters: ptr.To(int32(1))},
	}
	if err := client.IgnoreAlreadyExists(w.Hub.Client.Create(ctx, placement)); err != nil {
		return err
	}

	drpc := &rmn.DRPlacementControl{
		ObjectMeta: metav1.ObjectMeta{
			Name: app.Name, Namespace: app.ManagementNamespace(),
			Labels: map[string]string{"app": app.Name},
		},
		Spec: rmn.DRPlacementControlSpec{
			PreferredCluster: world.DR1Name,
			DRPolicyRef:      corev1.ObjectReference{Name: world.DRPolicyName},
			PlacementRef: corev1.ObjectReference{
				Kind: "Placement", Name: app.Name, Namespace: app.ManagementNamespace(),
			},
			PVCSelector: metav1.LabelSelector{
				MatchLabels: map[string]string{world.AppLabelKey: app.Name},
			},
			ProtectedNamespaces: &[]string{app.Namespace()},
		},
	}

	return client.IgnoreAlreadyExists(w.Hub.Client.Create(ctx, drpc))
}

func Failover(ctx context.Context, w *world.World, app App, target string) error {
	return patchDRPC(ctx, w, app, func(drpc *rmn.DRPlacementControl) {
		drpc.Spec.Action = rmn.ActionFailover
		drpc.Spec.FailoverCluster = target
	})
}

func Relocate(ctx context.Context, w *world.World, app App, preferred string) error {
	return patchDRPC(ctx, w, app, func(drpc *rmn.DRPlacementControl) {
		drpc.Spec.Action = rmn.ActionRelocate
		drpc.Spec.PreferredCluster = preferred
	})
}

func patchDRPC(ctx context.Context, w *world.World, app App, mutate func(*rmn.DRPlacementControl)) error {
	return retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		drpc := &rmn.DRPlacementControl{}
		key := types.NamespacedName{Name: app.Name, Namespace: app.ManagementNamespace()}
		if err := w.Hub.Client.Get(ctx, key, drpc); err != nil {
			return err
		}
		mutate(drpc)

		return w.Hub.Client.Update(ctx, drpc)
	})
}

// Disable removes DR protection: annotate to keep PVCs, delete DRPC, wait for
// it to be gone, delete the Placement.
func Disable(ctx context.Context, w *world.World, app App, timeout time.Duration) error {
	if err := patchDRPC(ctx, w, app, func(drpc *rmn.DRPlacementControl) {
		if drpc.Annotations == nil {
			drpc.Annotations = map[string]string{}
		}
		drpc.Annotations["drplacementcontrol.ramendr.openshift.io/do-not-delete-pvc"] = "true"
	}); err != nil {
		return err
	}

	key := types.NamespacedName{Name: app.Name, Namespace: app.ManagementNamespace()}
	drpc := &rmn.DRPlacementControl{ObjectMeta: metav1.ObjectMeta{Name: app.Name, Namespace: app.ManagementNamespace()}}
	if err := w.Hub.Client.Delete(ctx, drpc); err != nil && !errors.IsNotFound(err) {
		return err
	}

	deadline := time.Now().Add(timeout)
	for {
		err := w.Hub.Client.Get(ctx, key, drpc)
		if errors.IsNotFound(err) {
			break
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("DRPC %s not deleted; finalizer stuck (last: %+v)", key, drpc.Status.Progression)
		}
		time.Sleep(200 * time.Millisecond)
	}

	placement := &clrapiv1beta1.Placement{
		ObjectMeta: metav1.ObjectMeta{Name: app.Name, Namespace: app.ManagementNamespace()},
	}

	err := w.Hub.Client.Delete(ctx, placement)
	if errors.IsNotFound(err) {
		return nil
	}

	return err
}
