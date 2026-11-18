// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package stepper

import (
	"fmt"
	"time"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/ramendr/ramen/e2e/app"
	"github.com/ramendr/ramen/e2e/config"
	"github.com/ramendr/ramen/e2e/dractions"
	"github.com/ramendr/ramen/e2e/test"
	"github.com/ramendr/ramen/e2e/types"
	"github.com/ramendr/ramen/e2e/util"
)

const (
	// ReadTimeout limits the reads from a single cluster.
	ReadTimeout = 5 * time.Second

	// readGrace is the extra time to wait for readers that do not honor the read timeout.
	readGrace = 2 * time.Second

	// decisionTimeout limits waiting for a placement decision. A missing decision is not an error, so we do not
	// want to wait long.
	decisionTimeout = 3 * time.Second
)

// Unobserved returns the state of a test that could not be observed, for example when the clusters are not
// reachable.
func Unobserved(ctx *test.Context, err error) *State {
	s := newState(ctx)
	s.DRPC.readFailed = true
	s.Errors = append(s.Errors, err.Error())
	s.finalize()
	s.PositionReason = "could not observe the clusters: " + err.Error()

	return s
}

// reader reads part of the state from one cluster. read must not modify the shared state; it returns a function
// applying its observations. failed marks the part as unread when read did not complete in time, and returns the
// error.
type reader struct {
	read   func() (apply func(*State), errs []string)
	failed func(*State) string
}

type readResult struct {
	index int
	apply func(*State)
	errs  []string
}

// Observe reads the test application DR state from the hub and the managed clusters without modifying anything.
// All clusters are read in parallel, each with a short timeout. Failed lookups are reported in the state errors.
func Observe(ctx *test.Context) *State {
	s := newState(ctx)
	env := ctx.Env()

	s.Clusters = []ClusterState{
		{Name: env.C1.Name, Role: "c1"},
		{Name: env.C2.Name, Role: "c2"},
	}

	managed := []*types.Cluster{env.C1, env.C2}

	hubReaders := []reader{drpcReader(ctx, s.DRPC), decisionReader(ctx)}

	readers := make([]reader, 0, len(hubReaders)+len(managed))
	readers = append(readers, hubReaders...)

	for i, cluster := range managed {
		readers = append(readers, clusterReader(ctx, cluster, i, s.Clusters[i]))
	}

	results := collect(readers, ReadTimeout+readGrace)

	for i, r := range readers {
		if res := results[i]; res != nil {
			res.apply(s)
			s.Errors = append(s.Errors, res.errs...)
		} else {
			s.Errors = append(s.Errors, r.failed(s))
		}
	}

	s.finalize()

	return s
}

// collect runs all readers in parallel and returns their results by reader index. A reader that did not complete
// before the deadline has a nil result. Some client operations, such as API discovery, do not honor the context, so
// we cannot rely only on the readers' timeouts; abandoned readers finish in the background.
func collect(readers []reader, deadline time.Duration) []*readResult {
	ch := make(chan readResult, len(readers))

	for i, r := range readers {
		go func() {
			apply, errs := r.read()
			ch <- readResult{index: i, apply: apply, errs: errs}
		}()
	}

	results := make([]*readResult, len(readers))
	timer := time.NewTimer(deadline)

	defer timer.Stop()

	for range readers {
		select {
		case res := <-ch:
			results[res.index] = &res
		case <-timer.C:
			return results
		}
	}

	return results
}

func drpcReader(ctx *test.Context, drpc DRPCState) reader {
	return reader{
		read: func() (func(*State), []string) {
			errs := readDRPC(ctx, &drpc)

			return func(s *State) { s.DRPC = drpc }, errs
		},
		failed: func(s *State) string {
			s.DRPC.readFailed = true

			return fmt.Sprintf("timed out getting drpc \"%s/%s\" in cluster %q",
				s.DRPC.Namespace, s.DRPC.Name, ctx.Env().Hub.Name)
		},
	}
}

func decisionReader(ctx *test.Context) reader {
	return reader{
		read: func() (func(*State), []string) {
			cluster, errs := readDecision(ctx)

			return func(s *State) { s.decisionCluster = cluster }, errs
		},
		failed: func(*State) string {
			return fmt.Sprintf("timed out getting placement decision for \"%s/%s\" in cluster %q",
				ctx.ManagementNamespace(), ctx.Name(), ctx.Env().Hub.Name)
		},
	}
}

func clusterReader(ctx *test.Context, cluster *types.Cluster, index int, state ClusterState) reader {
	return reader{
		read: func() (func(*State), []string) {
			errs := readCluster(ctx, cluster, &state)

			return func(s *State) { s.Clusters[index] = state }, errs
		},
		failed: func(s *State) string {
			s.Clusters[index].readFailed = true

			return fmt.Sprintf("timed out reading application \"%s\" state in cluster %q",
				ctx.AppNamespace(), cluster.Name)
		},
	}
}

func newState(ctx *test.Context) *State {
	managementNamespace := resolvedManagementNamespace(ctx)

	return &State{
		Test:                ctx.Name(),
		Namespace:           ctx.AppNamespace(),
		ManagementNamespace: managementNamespace,
		DRPC: DRPCState{
			Name:      ctx.Name(),
			Namespace: managementNamespace,
		},
	}
}

// resolvedManagementNamespace returns the test management namespace, or an empty string if it depends on the
// distro and the distro is unknown (not configured and not detected yet).
func resolvedManagementNamespace(ctx *test.Context) string {
	if ctx.Config().Distro != "" {
		return ctx.ManagementNamespace()
	}

	k8s := managementNamespaceWith(ctx, config.K8sNamespaces)
	if k8s != managementNamespaceWith(ctx, config.OcpNamespaces) {
		return ""
	}

	return k8s
}

// managementNamespaceWith returns the test management namespace using the specified distro namespaces.
func managementNamespaceWith(ctx *test.Context, namespaces config.Namespaces) string {
	cfg := *ctx.Config()
	cfg.Namespaces = namespaces
	parent := app.NewContext(ctx.Context(), &cfg, ctx.Env(), ctx.Logger())
	tctx := test.NewContext(parent, ctx.Name(), ctx.Workload(), ctx.Deployer())

	return tctx.ManagementNamespace()
}

func readDRPC(ctx *test.Context, state *DRPCState) []string {
	timedCtx, cancel := ctx.WithTimeout(ReadTimeout)
	defer cancel()

	hub := ctx.Env().Hub
	drpc := &ramen.DRPlacementControl{}
	key := k8stypes.NamespacedName{Namespace: state.Namespace, Name: state.Name}

	if err := hub.Client.Get(timedCtx.Context(), key, drpc); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}

		state.readFailed = true

		return []string{fmt.Sprintf("failed to get drpc \"%s/%s\" in cluster %q: %s",
			state.Namespace, state.Name, hub.Name, err)}
	}

	state.Exists = true
	state.deleting = drpc.DeletionTimestamp != nil
	state.Phase = string(drpc.Status.Phase)
	state.Progression = string(drpc.Status.Progression)
	state.Action = string(drpc.Spec.Action)
	state.PreferredCluster = drpc.Spec.PreferredCluster
	state.FailoverCluster = drpc.Spec.FailoverCluster
	state.Conditions = conditions(drpc.Status.Conditions)

	return nil
}

// readDecision returns the cluster in the placement decision. A missing placement or decision is not an error;
// discovered applications have no placement before protection is enabled.
func readDecision(ctx *test.Context) (string, []string) {
	timedCtx, cancel := ctx.WithTimeout(decisionTimeout)
	defer cancel()

	if _, err := util.GetPlacement(timedCtx, ctx.ManagementNamespace(), ctx.Name()); err != nil {
		if k8serrors.IsNotFound(err) {
			return "", nil
		}

		return "", []string{fmt.Sprintf("failed to get placement \"%s/%s\" in cluster %q: %s",
			ctx.ManagementNamespace(), ctx.Name(), ctx.Env().Hub.Name, err)}
	}

	cluster, err := util.GetCurrentCluster(timedCtx, ctx.ManagementNamespace(), ctx.Name())
	if err != nil {
		// util.GetCurrentCluster waits for a decision, and fails when the context times out.
		if timedCtx.Context().Err() != nil {
			ctx.Logger().Debugf("No placement decision for \"%s/%s\": %s", ctx.ManagementNamespace(), ctx.Name(), err)

			return "", nil
		}

		return "", []string{fmt.Sprintf("failed to get placement decision for \"%s/%s\": %s",
			ctx.ManagementNamespace(), ctx.Name(), err)}
	}

	return cluster.Name, nil
}

func readCluster(ctx *test.Context, cluster *types.Cluster, state *ClusterState) []string {
	timedCtx, cancel := ctx.WithTimeout(ReadTimeout)
	defer cancel()

	var errs []string

	status, err := ctx.Workload().StatusForCluster(timedCtx, cluster)
	if err != nil {
		state.readFailed = true

		errs = append(errs, fmt.Sprintf("failed to get application \"%s/%s\" status in cluster %q: %s",
			ctx.AppNamespace(), ctx.Workload().GetAppName(), cluster.Name, err))
	} else {
		state.app = status.Status
	}

	if err := readVRG(timedCtx, cluster, &state.VRG); err != nil {
		errs = append(errs, err.Error())
	}

	if err := readPVCs(timedCtx, cluster, state); err != nil {
		errs = append(errs, err.Error())
	}

	if err := readPods(timedCtx, cluster, state); err != nil {
		errs = append(errs, err.Error())
	}

	return errs
}

func readVRG(ctx *test.Context, cluster *types.Cluster, state *VRGState) error {
	vrg := &ramen.VolumeReplicationGroup{}
	key := k8stypes.NamespacedName{Namespace: dractions.VRGNamespace(ctx), Name: ctx.Name()}

	if err := cluster.Client.Get(ctx.Context(), key, vrg); err != nil {
		if k8serrors.IsNotFound(err) {
			return nil
		}

		return fmt.Errorf("failed to get vrg \"%s/%s\" in cluster %q: %w", key.Namespace, key.Name, cluster.Name, err)
	}

	state.Exists = true
	state.State = string(vrg.Status.State)
	state.ReplicationState = string(vrg.Spec.ReplicationState)
	state.Conditions = conditions(vrg.Status.Conditions)

	return nil
}

func readPVCs(ctx *test.Context, cluster *types.Cluster, state *ClusterState) error {
	pvcs := &corev1.PersistentVolumeClaimList{}

	if err := cluster.Client.List(ctx.Context(), pvcs, client.InNamespace(ctx.AppNamespace())); err != nil {
		return fmt.Errorf("failed to list pvcs in namespace %q in cluster %q: %w",
			ctx.AppNamespace(), cluster.Name, err)
	}

	for i := range pvcs.Items {
		pvc := &pvcs.Items[i]
		state.PVCs = append(state.PVCs, PVCState{Name: pvc.Name, Phase: string(pvc.Status.Phase)})
	}

	return nil
}

func readPods(ctx *test.Context, cluster *types.Cluster, state *ClusterState) error {
	pods := &corev1.PodList{}

	if err := cluster.Client.List(ctx.Context(), pods, client.InNamespace(ctx.AppNamespace())); err != nil {
		return fmt.Errorf("failed to list pods in namespace %q in cluster %q: %w",
			ctx.AppNamespace(), cluster.Name, err)
	}

	for i := range pods.Items {
		pod := &pods.Items[i]
		state.Pods = append(state.Pods, PodState{
			Name:  pod.Name,
			Phase: string(pod.Status.Phase),
			Ready: podReady(pod),
		})
	}

	return nil
}

func podReady(pod *corev1.Pod) bool {
	for _, c := range pod.Status.Conditions {
		if c.Type == corev1.PodReady {
			return c.Status == corev1.ConditionTrue
		}
	}

	return false
}

func conditions(in []metav1.Condition) []Condition {
	out := make([]Condition, 0, len(in))
	for _, c := range in {
		out = append(out, Condition{
			Type:    c.Type,
			Status:  string(c.Status),
			Reason:  c.Reason,
			Message: c.Message,
		})
	}

	return out
}
