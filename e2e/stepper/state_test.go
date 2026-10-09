// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Disabling testpackage linter to build states with unexported observations.
//
//nolint:testpackage
package stepper

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/ramendr/ramen/e2e/types"
)

const (
	c1 = "dr1"
	c2 = "dr2"
)

type clusterObs struct {
	app        types.ApplicationStatus
	readFailed bool
}

var (
	notFound = clusterObs{app: types.ApplicationNotFound}
	found    = clusterObs{app: types.ApplicationFound}
	partial  = clusterObs{app: types.ApplicationPartial}
	unread   = clusterObs{readFailed: true}
)

func newTestState(drpc DRPCState, decision string, obs1, obs2 clusterObs) *State {
	s := &State{
		Test:            "disapp-deploy-rbd",
		DRPC:            drpc,
		decisionCluster: decision,
		Clusters: []ClusterState{
			{Name: c1, Role: "c1", app: obs1.app, readFailed: obs1.readFailed},
			{Name: c2, Role: "c2", app: obs2.app, readFailed: obs2.readFailed},
		},
	}
	s.finalize()

	return s
}

func settledDRPC(phase, preferred, failover string) DRPCState {
	return DRPCState{
		Exists:           true,
		Phase:            phase,
		Progression:      "Completed",
		PreferredCluster: preferred,
		FailoverCluster:  failover,
	}
}

//nolint:funlen
func TestPosition(t *testing.T) {
	cases := []struct {
		name     string
		drpc     DRPCState
		decision string
		c1, c2   clusterObs
		position Position
		reason   string
		home     string
		current  string
	}{
		{
			name: "undeployed", c1: notFound, c2: notFound,
			position: PositionUndeployed, reason: "no application resources",
		},
		{
			name: "deployed", c1: found, c2: notFound,
			position: PositionDeployed, reason: `running on cluster "dr1"`, home: c1, current: c1,
		},
		{
			name: "deployed with placement decision", decision: c1, c1: found, c2: notFound,
			position: PositionDeployed, home: c1, current: c1,
		},
		{
			name: "no drpc, cluster unreadable", c1: found, c2: unread,
			position: PositionUnknown, reason: `could not read the application on cluster(s) ["dr2"]`,
			home: c1, current: c1,
		},
		{
			name: "no drpc, partial resources", c1: partial, c2: notFound,
			position: PositionUnknown, reason: "partial application resources",
		},
		{
			name: "no drpc, found and partial", c1: found, c2: partial,
			position: PositionUnknown, reason: "partial", home: c1, current: c1,
		},
		{
			name: "no drpc, running on both", c1: found, c2: found,
			position: PositionUnknown, reason: "multiple clusters",
		},
		{
			name: "drpc read failed", drpc: DRPCState{readFailed: true}, c1: found, c2: notFound,
			position: PositionUnknown, reason: "could not read the DRPC", current: c1,
		},
		{
			name: "protected", drpc: settledDRPC("Deployed", c1, ""), decision: c1, c1: found, c2: partial,
			position: PositionProtected, reason: `running on cluster "dr1"`, home: c1, current: c1,
		},
		{
			name: "protected on the second cluster", drpc: settledDRPC("Deployed", c2, ""), decision: c2,
			c1: notFound, c2: found,
			position: PositionProtected, reason: `running on cluster "dr2"`, home: c2, current: c2,
		},
		{
			name: "failed over", drpc: settledDRPC("FailedOver", c1, c2), decision: c2, c1: notFound, c2: found,
			position: PositionFailedOver, home: c1, current: c2,
		},
		{
			name: "relocated", drpc: settledDRPC("Relocated", c1, c2), decision: c1, c1: found, c2: notFound,
			position: PositionRelocated, home: c1, current: c1,
		},
		{
			name: "failed over, other cluster unreadable", drpc: settledDRPC("FailedOver", c1, c2),
			decision: c2, c1: unread, c2: found,
			position: PositionFailedOver, reason: `could not verify the application on cluster(s) ["dr1"]`,
			home: c1, current: c2,
		},
		{
			name: "failing over", drpc: DRPCState{
				Exists: true, Phase: "FailingOver", Progression: "WaitingForResourceRestore",
				Action: "Failover", PreferredCluster: c1, FailoverCluster: c2,
			},
			decision: c2, c1: notFound, c2: partial,
			position: PositionUnknown, reason: `action "Failover" in progress`, home: c1, current: c2,
		},
		{
			name: "failed over, cleaning up", drpc: DRPCState{
				Exists: true, Phase: "FailedOver", Progression: "Cleaning Up",
				Action: "Failover", PreferredCluster: c1, FailoverCluster: c2,
			},
			decision: c2, c1: found, c2: found,
			position: PositionUnknown, reason: `progression "Cleaning Up" is not "Completed"`, home: c1, current: c2,
		},
		{
			name: "drpc without phase", drpc: DRPCState{Exists: true, PreferredCluster: c1}, c1: found, c2: notFound,
			position: PositionUnknown, reason: `DRPC phase ""`, home: c1, current: c1,
		},
		{
			name: "drpc deleting", drpc: func() DRPCState {
				d := settledDRPC("Deployed", c1, "")
				d.deleting = true

				return d
			}(), c1: found, c2: notFound,
			position: PositionUnknown, reason: "being deleted", home: c1, current: c1,
		},
		{
			name: "protected, but app missing", drpc: settledDRPC("Deployed", c1, ""), decision: c1,
			c1: notFound, c2: notFound,
			position: PositionUnknown, reason: "not running on any managed cluster", home: c1, current: c1,
		},
		{
			name: "failed over, but app on wrong cluster", drpc: settledDRPC("FailedOver", c1, c2),
			c1: found, c2: notFound,
			position: PositionUnknown, reason: `expecting the application on cluster "dr2"`, home: c1, current: c1,
		},
		{
			name: "relocated, but running on both", drpc: settledDRPC("Relocated", c1, c2), decision: c1,
			c1: found, c2: found,
			position: PositionUnknown, reason: "multiple clusters", home: c1, current: c1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestState(tc.drpc, tc.decision, tc.c1, tc.c2)

			if s.Position != tc.position {
				t.Errorf("position %q (%s), expected %q", s.Position, s.PositionReason, tc.position)
			}

			if !strings.Contains(s.PositionReason, tc.reason) {
				t.Errorf("reason %q does not contain %q", s.PositionReason, tc.reason)
			}

			if s.HomeCluster != tc.home {
				t.Errorf("home cluster %q, expected %q", s.HomeCluster, tc.home)
			}

			if s.CurrentCluster != tc.current {
				t.Errorf("current cluster %q, expected %q", s.CurrentCluster, tc.current)
			}
		})
	}
}

func TestStateJSONContract(t *testing.T) {
	s := &State{
		Test:                "disapp-deploy-rbd",
		Namespace:           "test-disapp-deploy-rbd",
		ManagementNamespace: "ramen-ops",
		DRPC: DRPCState{
			Exists:           true,
			Name:             "disapp-deploy-rbd",
			Namespace:        "ramen-ops",
			Phase:            "FailedOver",
			Progression:      "Completed",
			Action:           "Failover",
			PreferredCluster: c1,
			FailoverCluster:  c2,
			Conditions:       []Condition{{Type: "Available", Status: "True", Reason: "r", Message: "m"}},
		},
		Clusters: []ClusterState{
			{
				Name: c1, Role: "c1", app: types.ApplicationNotFound,
				VRG: VRGState{Exists: true, State: "Secondary", ReplicationState: "secondary"},
			},
			{
				Name: c2, Role: "c2", app: types.ApplicationFound,
				VRG:  VRGState{Exists: true, State: "Primary", ReplicationState: "primary"},
				PVCs: []PVCState{{Name: "busybox-pvc", Phase: "Bound"}},
				Pods: []PodState{{Name: "busybox-xyz", Phase: "Running", Ready: true}},
			},
		},
		decisionCluster: c2,
	}
	s.finalize()

	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"test":"disapp-deploy-rbd","namespace":"test-disapp-deploy-rbd",` +
		`"managementNamespace":"ramen-ops","position":"failed-over",` +
		`"positionReason":"DRPC phase \"FailedOver\", progression \"Completed\", ` +
		`application running on cluster \"dr2\"",` +
		`"homeCluster":"dr1","currentCluster":"dr2",` +
		`"drpc":{"exists":true,"name":"disapp-deploy-rbd","namespace":"ramen-ops","phase":"FailedOver",` +
		`"progression":"Completed","action":"Failover","preferredCluster":"dr1","failoverCluster":"dr2",` +
		`"conditions":[{"type":"Available","status":"True","reason":"r","message":"m"}]},` +
		`"clusters":[` +
		`{"name":"dr1","role":"c1","vrg":{"exists":true,"state":"Secondary","replicationState":"secondary",` +
		`"conditions":[]},"pvcs":[],"pods":[]},` +
		`{"name":"dr2","role":"c2","vrg":{"exists":true,"state":"Primary","replicationState":"primary",` +
		`"conditions":[]},"pvcs":[{"name":"busybox-pvc","phase":"Bound"}],` +
		`"pods":[{"name":"busybox-xyz","phase":"Running","ready":true}]}],` +
		`"errors":[]}`

	if string(data) != expected {
		t.Fatalf("unexpected state:\n got: %s\nwant: %s", data, expected)
	}
}

func TestEmptyStateHasNoNulls(t *testing.T) {
	s := &State{Clusters: []ClusterState{{Name: c1}}}
	s.finalize()

	data, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}

	if strings.Contains(string(data), "null") {
		t.Fatalf("state contains null: %s", data)
	}
}
