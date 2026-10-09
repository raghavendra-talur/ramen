// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package stepper

import (
	"fmt"
	"slices"
	"strings"

	ramen "github.com/ramendr/ramen/api/v1alpha1"

	"github.com/ramendr/ramen/e2e/types"
)

// Position is where a test application is in the DR lifecycle.
type Position string

const (
	// PositionUndeployed: no DRPC and no application resources on any managed cluster.
	PositionUndeployed = Position("undeployed")
	// PositionDeployed: application running on one managed cluster, no DRPC.
	PositionDeployed = Position("deployed")
	// PositionProtected: DRPC phase Deployed (initial placement) and progression Completed.
	PositionProtected = Position("protected")
	// PositionFailedOver: DRPC phase FailedOver and progression Completed.
	PositionFailedOver = Position("failed-over")
	// PositionRelocated: DRPC phase Relocated and progression Completed.
	PositionRelocated = Position("relocated")
	// PositionUnknown: an action is in progress, the observations are contradictory, or a lookup needed to decide
	// failed. The position reason explains what was seen.
	PositionUnknown = Position("unknown")
)

// settledPositions maps DRPC phases where no action is in progress to positions.
var settledPositions = map[string]Position{
	string(ramen.Deployed):   PositionProtected,
	string(ramen.FailedOver): PositionFailedOver,
	string(ramen.Relocated):  PositionRelocated,
}

// The JSON field names of the types below are a contract with programs consuming `e2e-step state --json`. Slices are
// always non-nil so they are encoded as [] and never as null.

// State is a read-only observation of a test application DR state.
type State struct {
	Test                string         `json:"test"`
	Namespace           string         `json:"namespace"`
	ManagementNamespace string         `json:"managementNamespace"`
	Position            Position       `json:"position"`
	PositionReason      string         `json:"positionReason"`
	HomeCluster         string         `json:"homeCluster"`
	CurrentCluster      string         `json:"currentCluster"`
	DRPC                DRPCState      `json:"drpc"`
	Clusters            []ClusterState `json:"clusters"`
	Errors              []string       `json:"errors"`

	// decisionCluster is the cluster in the placement decision, empty if there is no decision.
	decisionCluster string
}

// Condition is a status condition of a DRPC or VRG.
type Condition struct {
	Type    string `json:"type"`
	Status  string `json:"status"`
	Reason  string `json:"reason"`
	Message string `json:"message"`
}

// DRPCState is the observed DRPC on the hub.
type DRPCState struct {
	Exists           bool        `json:"exists"`
	Name             string      `json:"name"`
	Namespace        string      `json:"namespace"`
	Phase            string      `json:"phase"`
	Progression      string      `json:"progression"`
	Action           string      `json:"action"`
	PreferredCluster string      `json:"preferredCluster"`
	FailoverCluster  string      `json:"failoverCluster"`
	Conditions       []Condition `json:"conditions"`

	// readFailed is true if we could not tell if the DRPC exists.
	readFailed bool
	// deleting is true if the DRPC has a deletion timestamp.
	deleting bool
}

// ClusterState is the observed test application state on a managed cluster.
type ClusterState struct {
	Name string     `json:"name"`
	Role string     `json:"role"`
	VRG  VRGState   `json:"vrg"`
	PVCs []PVCState `json:"pvcs"`
	Pods []PodState `json:"pods"`

	// app is the workload status on this cluster, valid only if readFailed is false.
	app types.ApplicationStatus
	// readFailed is true if we could not tell if the application exists on this cluster.
	readFailed bool
}

// VRGState is the observed VRG on a managed cluster.
type VRGState struct {
	Exists           bool        `json:"exists"`
	State            string      `json:"state"`
	ReplicationState string      `json:"replicationState"`
	Conditions       []Condition `json:"conditions"`
}

// PVCState is an observed PVC in the application namespace.
type PVCState struct {
	Name  string `json:"name"`
	Phase string `json:"phase"`
}

// PodState is an observed pod in the application namespace.
type PodState struct {
	Name  string `json:"name"`
	Phase string `json:"phase"`
	Ready bool   `json:"ready"`
}

// finalize derives the position, home and current clusters from the observations, and ensures that all slices are
// non-nil.
func (s *State) finalize() {
	s.ensureSlices()
	s.CurrentCluster = s.currentCluster()
	s.HomeCluster = s.homeCluster()
	s.Position, s.PositionReason = s.position()
}

func (s *State) ensureSlices() {
	if s.Clusters == nil {
		s.Clusters = []ClusterState{}
	}

	if s.Errors == nil {
		s.Errors = []string{}
	}

	if s.DRPC.Conditions == nil {
		s.DRPC.Conditions = []Condition{}
	}

	for i := range s.Clusters {
		c := &s.Clusters[i]
		if c.PVCs == nil {
			c.PVCs = []PVCState{}
		}

		if c.Pods == nil {
			c.Pods = []PodState{}
		}

		if c.VRG.Conditions == nil {
			c.VRG.Conditions = []Condition{}
		}
	}
}

// currentCluster returns the cluster in the placement decision. If there is no decision, it returns the only
// cluster where the application was found. Otherwise it returns an empty string.
func (s *State) currentCluster() string {
	if s.decisionCluster != "" {
		return s.decisionCluster
	}

	if found := s.clustersWithApp(types.ApplicationFound); len(found) == 1 {
		return found[0]
	}

	return ""
}

// homeCluster returns the DRPC preferred cluster. Without a DRPC the application was not moved yet, so the home
// cluster is the current cluster.
func (s *State) homeCluster() string {
	if s.DRPC.Exists {
		return s.DRPC.PreferredCluster
	}

	if s.DRPC.readFailed {
		return ""
	}

	return s.CurrentCluster
}

// position derives the position from the observations. Rules:
//
//   - DRPC lookup failed: unknown.
//   - No DRPC: undeployed if no application resources are found on any managed cluster, deployed if the application
//     is found on exactly one cluster and no resources exist on the other cluster, otherwise unknown. Any failure
//     to read the application on a cluster makes the position unknown.
//   - DRPC being deleted: unknown.
//   - DRPC phase Deployed, FailedOver or Relocated with progression Completed: protected, failed-over or relocated,
//     if the application is found only on the expected cluster (failoverCluster after failover, preferredCluster
//     otherwise). If the application is found elsewhere, nowhere, or on multiple clusters, unknown. If a cluster
//     could not be read, the DRPC is conclusive and the reason says what could not be verified.
//   - Any other DRPC phase or progression means an action is in progress: unknown.
func (s *State) position() (Position, string) {
	if s.DRPC.readFailed {
		return PositionUnknown, "could not read the DRPC on the hub"
	}

	if !s.DRPC.Exists {
		return s.positionWithoutDRPC()
	}

	return s.positionWithDRPC()
}

func (s *State) positionWithoutDRPC() (Position, string) {
	if failed := s.unreadClusters(); len(failed) > 0 {
		return PositionUnknown, fmt.Sprintf("no DRPC, and could not read the application on cluster(s) %s",
			quoteList(failed))
	}

	found := s.clustersWithApp(types.ApplicationFound)
	partial := s.clustersWithApp(types.ApplicationPartial)

	switch {
	case len(found) == 0 && len(partial) == 0:
		return PositionUndeployed, "no DRPC and no application resources on any managed cluster"
	case len(found) == 1 && len(partial) == 0:
		return PositionDeployed, fmt.Sprintf("no DRPC, application running on cluster %q", found[0])
	case len(found) > 1:
		return PositionUnknown, fmt.Sprintf("no DRPC, but the application is running on multiple clusters %s",
			quoteList(found))
	default:
		return PositionUnknown, fmt.Sprintf("no DRPC, application found on cluster(s) %s and partial "+
			"application resources on cluster(s) %s (deploy or undeploy in progress?)",
			quoteList(found), quoteList(partial))
	}
}

func (s *State) positionWithDRPC() (Position, string) {
	drpc := &s.DRPC

	if drpc.deleting {
		return PositionUnknown, fmt.Sprintf("DRPC is being deleted (phase %q, progression %q)",
			drpc.Phase, drpc.Progression)
	}

	position, settled := settledPositions[drpc.Phase]
	if !settled {
		return PositionUnknown, fmt.Sprintf("DRPC phase %q, progression %q: action %q in progress",
			drpc.Phase, drpc.Progression, drpc.Action)
	}

	if drpc.Progression != string(ramen.ProgressionCompleted) {
		return PositionUnknown, fmt.Sprintf("DRPC phase %q but progression %q is not %q: action %q in progress",
			drpc.Phase, drpc.Progression, ramen.ProgressionCompleted, drpc.Action)
	}

	expected := drpc.PreferredCluster
	if drpc.Phase == string(ramen.FailedOver) {
		expected = drpc.FailoverCluster
	}

	summary := fmt.Sprintf("DRPC phase %q, progression %q", drpc.Phase, drpc.Progression)

	if failed := s.unreadClusters(); len(failed) > 0 {
		return position, fmt.Sprintf("%s, expecting the application on cluster %q; could not verify the "+
			"application on cluster(s) %s", summary, expected, quoteList(failed))
	}

	found := s.clustersWithApp(types.ApplicationFound)

	switch {
	case len(found) == 0:
		return PositionUnknown, fmt.Sprintf("%s, but the application is not running on any managed cluster",
			summary)
	case len(found) > 1:
		return PositionUnknown, fmt.Sprintf("%s, but the application is running on multiple clusters %s",
			summary, quoteList(found))
	case found[0] != expected:
		return PositionUnknown, fmt.Sprintf("%s, expecting the application on cluster %q, but it is running "+
			"on cluster %q", summary, expected, found[0])
	default:
		return position, fmt.Sprintf("%s, application running on cluster %q", summary, found[0])
	}
}

// clustersWithApp returns the names of the readable clusters where the application status is status.
func (s *State) clustersWithApp(status types.ApplicationStatus) []string {
	var names []string

	for i := range s.Clusters {
		c := &s.Clusters[i]
		if !c.readFailed && c.app == status {
			names = append(names, c.Name)
		}
	}

	return names
}

// unreadClusters returns the names of the clusters where the application could not be read.
func (s *State) unreadClusters() []string {
	var names []string

	for i := range s.Clusters {
		if s.Clusters[i].readFailed {
			names = append(names, s.Clusters[i].Name)
		}
	}

	return names
}

func quoteList(names []string) string {
	if len(names) == 0 {
		return "[]"
	}

	quoted := slices.Clone(names)
	for i := range quoted {
		quoted[i] = fmt.Sprintf("%q", quoted[i])
	}

	return "[" + strings.Join(quoted, ", ") + "]"
}
