// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package observe

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Scale multiplies timeouts by SIMTEST_TIMEOUT_SCALE (default 1) so slow CI
// machines can widen every gate from one knob.
func Scale(d time.Duration) time.Duration {
	if s := os.Getenv("SIMTEST_TIMEOUT_SCALE"); s != "" {
		if f, err := strconv.ParseFloat(s, 64); err == nil && f > 0 {
			return time.Duration(float64(d) * f)
		}
	}

	return d
}

// WaitDRPCReady mirrors e2e's waitDRPCReady: Available, PeerReady,
// progression Completed, and a non-nil lastGroupSyncTime.
func WaitDRPCReady(ctx context.Context, c client.Client, ns, name string, timeout time.Duration) error {
	deadline := time.Now().Add(Scale(timeout))

	var last string

	for time.Now().Before(deadline) {
		drpc := &rmn.DRPlacementControl{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: name}, drpc); err == nil {
			available := meta.IsStatusConditionTrue(drpc.Status.Conditions, rmn.ConditionAvailable)
			peerReady := meta.IsStatusConditionTrue(drpc.Status.Conditions, rmn.ConditionPeerReady)
			completed := drpc.Status.Progression == rmn.ProgressionCompleted
			synced := drpc.Status.LastGroupSyncTime != nil

			if available && peerReady && completed && synced {
				return nil
			}

			last = fmt.Sprintf("available=%v peerReady=%v progression=%s synced=%v",
				available, peerReady, drpc.Status.Progression, synced)
		}
		time.Sleep(200 * time.Millisecond)
	}

	return fmt.Errorf("DRPC %s/%s not ready: %s", ns, name, last)
}
