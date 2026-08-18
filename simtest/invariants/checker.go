// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package invariants

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"

	"github.com/ramendr/ramen/simtest/world"
)

// Checker continuously enforces global safety properties across all clusters,
// independent of what the current scenario asserts.
type Checker struct {
	mu         sync.Mutex
	violations []string
	lastProg   map[string]string // drpc key -> last progression, for edge recording
	edgeFile   *os.File
	w          *world.World
	cancel     context.CancelFunc
	done       chan struct{}

	// OnViolation, when set, is notified once per recorded violation.
	OnViolation func(string)
}

func StartChecker(ctx context.Context, w *world.World, edgePath string) (*Checker, error) {
	f, err := os.OpenFile(edgePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	cctx, cancel := context.WithCancel(ctx)
	c := &Checker{lastProg: map[string]string{}, edgeFile: f, w: w, cancel: cancel, done: make(chan struct{})}

	go func() {
		defer close(c.done)

		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()

		for {
			select {
			case <-cctx.Done():
				return
			case <-ticker.C:
				c.check(cctx)
			}
		}
	}()

	return c, nil
}

func (c *Checker) check(ctx context.Context) {
	drpcs := &rmn.DRPlacementControlList{}
	if err := c.w.Hub.Client.List(ctx, drpcs); err != nil {
		return
	}

	for i := range drpcs.Items {
		drpc := &drpcs.Items[i]
		c.recordEdge(drpc)
		c.checkSinglePrimary(ctx, drpc)
	}
}

func (c *Checker) recordEdge(drpc *rmn.DRPlacementControl) {
	key := drpc.Namespace + "/" + drpc.Name
	cur := string(drpc.Status.Progression)

	c.mu.Lock()
	defer c.mu.Unlock()

	if prev, ok := c.lastProg[key]; ok && prev != cur && cur != "" {
		fmt.Fprintf(c.edgeFile, "%s: %s -> %s\n", key, prev, cur)
	}
	if cur != "" {
		c.lastProg[key] = cur
	}
}

func (c *Checker) checkSinglePrimary(ctx context.Context, drpc *rmn.DRPlacementControl) {
	primaries := 0

	for _, m := range c.w.Managed() {
		vrg := &rmn.VolumeReplicationGroup{}
		key := types.NamespacedName{Name: drpc.Name, Namespace: world.RamenOpsNS}
		if err := m.Client.Get(ctx, key, vrg); err != nil {
			continue
		}
		if vrg.Spec.ReplicationState == rmn.Primary {
			primaries++
		}
	}

	if violatesSinglePrimary(primaries, drpc) {
		c.addViolation(fmt.Sprintf("single-primary violated for %s/%s: %d primaries in phase %s",
			drpc.Namespace, drpc.Name, primaries, drpc.Status.Phase))
	}
}

// violatesSinglePrimary encodes the documented transitional window: during
// failover the old primary is presumed unreachable and stays spec-primary
// until post-failover cleanup completes (PeerReady). Everywhere else, two
// primaries is split-brain.
func violatesSinglePrimary(primaries int, drpc *rmn.DRPlacementControl) bool {
	if primaries <= 1 {
		return false
	}

	switch drpc.Status.Phase {
	case rmn.FailingOver:
		return false
	case rmn.FailedOver:
		return peerReady(drpc)
	default:
		return true
	}
}

func peerReady(drpc *rmn.DRPlacementControl) bool {
	return meta.IsStatusConditionTrue(drpc.Status.Conditions, rmn.ConditionPeerReady)
}

func (c *Checker) addViolation(v string) {
	if !c.recordViolation(v) {
		return
	}

	if c.OnViolation != nil {
		c.OnViolation(v)
	}
}

// recordViolation appends v to the violation log under the lock if it is not
// already present, returning whether it was newly added. Violations() shares
// c.mu, so the lock must be released before addViolation invokes OnViolation.
func (c *Checker) recordViolation(v string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, existing := range c.violations {
		if existing == v {
			return false
		}
	}
	c.violations = append(c.violations, v)

	return true
}

func (c *Checker) Violations() []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := make([]string, len(c.violations))
	copy(out, c.violations)

	return out
}

func (c *Checker) AssertClean(t interface{ Fatalf(string, ...any) }) {
	if v := c.Violations(); len(v) > 0 {
		t.Fatalf("invariant violations: %v", v)
	}
}

func (c *Checker) Stop() {
	c.cancel()
	<-c.done
	c.edgeFile.Close()
}
