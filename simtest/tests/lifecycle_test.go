// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package tests

import (
	"context"
	"strings"
	"testing"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ocmworkv1 "open-cluster-management.io/api/work/v1"
	viewv1beta1 "open-cluster-management.io/multicloud-operators-subscription/pkg/apis/view/v1beta1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/simtest/observe"
	"github.com/ramendr/ramen/simtest/user"
	"github.com/ramendr/ramen/simtest/world"
)

// stage is one step of an app's DR journey. Scenarios are ordered stage
// lists, so complex cases (repeated failover/relocate ping-pong, partial
// journeys) are just longer or shorter slices.
type stage string

const (
	stgCreate   stage = "create"
	stgEnroll   stage = "enroll"
	stgFailover stage = "failover"
	stgRelocate stage = "relocate"
	stgUnenroll stage = "unenroll"
	stgDelete   stage = "delete"
)

// fullLifecycle is the canonical journey: born, protected, failed over,
// brought home, unprotected, deleted. Every matrix combo runs all of it:
// the world ends each combo as clean as it started (no accumulating apps),
// and the enroll/unenroll/delete paths are exercised by every combo instead
// of by one baseline.
var fullLifecycle = []stage{stgCreate, stgEnroll, stgFailover, stgRelocate, stgUnenroll, stgDelete}

// runLifecycle drives app through stages in order, tracking which cluster
// is primary: create places the app on DR1 and every move flips it. hooks
// fire during the move of hookStage only ("" hooks nothing). It returns the
// progression sequences observed per move stage; the matrix seed run uses
// them to discover checkpoints.
func runLifecycle(t *testing.T, w *world.World, app user.App,
	stages []stage, hookStage stage, hooks ...Hook,
) map[stage][]string {
	t.Helper()
	ctx := context.Background()

	cur := world.DR1Name
	other := func() string {
		if cur == world.DR1Name {
			return world.DR2Name
		}

		return world.DR1Name
	}

	seqs := map[stage][]string{}

	for _, st := range stages {
		var mv []Hook
		if st == hookStage {
			mv = hooks
		}

		switch st {
		case stgCreate:
			if err := user.CreateApp(ctx, w, app, cur); err != nil {
				t.Fatalf("create app: %v", err)
			}
		case stgEnroll:
			runEnroll(t, w, app)
		case stgFailover:
			target := other()
			seqs[st] = runMove(t, w, app,
				func(ctx context.Context) error { return user.Failover(ctx, w, app, target) },
				cur, rmn.FailedOver, mv)
			cur = target
		case stgRelocate:
			target := other()
			seqs[st] = runMove(t, w, app,
				func(ctx context.Context) error { return user.Relocate(ctx, w, app, target) },
				cur, rmn.Relocated, mv)
			cur = target
		case stgUnenroll:
			if err := user.Disable(ctx, w, app, observe.Scale(2*time.Minute)); err != nil {
				t.Fatalf("unenroll: %v", err)
			}
		case stgDelete:
			if err := user.DeleteApp(ctx, w, app, cur); err != nil {
				t.Fatalf("delete app on %s: %v", cur, err)
			}
			assertAppClean(t, w, app)
		default:
			t.Fatalf("unknown stage %q", st)
		}
	}

	return seqs
}

// assertAppClean is the per-app leak check that the full lifecycle makes
// possible: after unenroll+delete, nothing of the app may remain — no DRPC,
// no VRG or PVC on either managed cluster, and no hub-side ManifestWork or
// ManagedClusterView carrying the app's name. Residue here is either a
// ramen cleanup bug or a framework leak; both must fail the combo loudly.
// (App namespaces are excluded: envtest runs no namespace controller, so
// namespace deletion never completes there by design.)
func assertAppClean(t *testing.T, w *world.World, app user.App) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(observe.Scale(2 * time.Minute))

	for {
		left := appResidue(ctx, w, app)
		if len(left) == 0 {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("app %s left residue after delete: %v", app.Name, left)
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func appResidue(ctx context.Context, w *world.World, app user.App) []string {
	left := []string{}

	drpc := &rmn.DRPlacementControl{}
	if err := w.Hub.Client.Get(ctx,
		types.NamespacedName{Namespace: app.ManagementNamespace(), Name: app.Name}, drpc); err == nil {
		left = append(left, "drpc")
	}

	for _, m := range w.Managed() {
		vrg := &rmn.VolumeReplicationGroup{}
		if err := m.Client.Get(ctx,
			types.NamespacedName{Namespace: app.ManagementNamespace(), Name: app.Name}, vrg); err == nil {
			left = append(left, "vrg@"+m.Name)
		}

		pvc := &corev1.PersistentVolumeClaim{}
		if err := m.Client.Get(ctx,
			types.NamespacedName{Namespace: app.Namespace(), Name: app.PVCName()}, pvc); err == nil {
			left = append(left, "pvc@"+m.Name)
		}
	}

	for _, cl := range []string{world.DR1Name, world.DR2Name} {
		mws := &ocmworkv1.ManifestWorkList{}
		if err := w.Hub.Client.List(ctx, mws, client.InNamespace(cl)); err == nil {
			for i := range mws.Items {
				if strings.HasPrefix(mws.Items[i].Name, app.Name+"-") {
					left = append(left, "mw:"+cl+"/"+mws.Items[i].Name)
				}
			}
		}

		mcvs := &viewv1beta1.ManagedClusterViewList{}
		if err := w.Hub.Client.List(ctx, mcvs, client.InNamespace(cl)); err == nil {
			for i := range mcvs.Items {
				if strings.HasPrefix(mcvs.Items[i].Name, app.Name+"-") {
					left = append(left, "mcv:"+cl+"/"+mcvs.Items[i].Name)
				}
			}
		}
	}

	return left
}
