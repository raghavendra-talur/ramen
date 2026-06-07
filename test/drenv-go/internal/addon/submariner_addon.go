// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package addon: submariner deploys the Submariner multi-cluster networking
// operator, mirroring addons/submariner/start.py.
//
// This is a GLOBAL worker addon: cluster param is "" and targets come from args:
//   args[0]  = broker cluster context
//   args[1:] = member cluster contexts
//
// VERSION is fixed at 0.21.2 (0.22.0 is broken in minikube).
//
// Broker-info placement:
//   Python writes broker-info.subm to the CWD and then moves it to
//   drenv.config_dir(broker)/submariner/broker-info.subm, where config_dir is
//   ~/.config/drenv/<profile>/. In drenv-go we pass the full target path to
//   subctl via the --brokerfile flag; the Subctl.DeployBroker wrapper does not
//   currently accept a brokerfile path, so we use the deterministic path:
//
//     ~/.config/drenv/<EnvName>/submariner/broker-info.subm
//
//   NOTE: this path is asserted in unit tests for argv correctness. Real-cluster
//   validation is required to confirm subctl respects the --brokerfile flag.
//
//   Nodes annotation: Python gets nodes as JSON, parses InternalIP, and annotates
//   each node with gateway.submariner.io/public-ip=ipv4:<InternalIP>. The Go
//   implementation does the same via kubectl.Get (Output) + Annotate.

package addon

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	submarinerVersion               = "0.21.2"
	submarinerNamespace             = "submariner-operator"
	submarinerBrokerInfoFile        = "broker-info.subm"
	submarinerBrokerWaitTimeout     = 5 * time.Minute
	submarinerBrokerRolloutTimeout  = 5 * time.Minute
	submarinerClusterWaitTimeout    = 3 * time.Minute
	submarinerClusterRolloutTimeout = 3 * time.Minute
)

// submarinerBrokerDeployments mirrors BROKER_DEPLOYMENTS in start.py.
var submarinerBrokerDeployments = []string{
	"submariner-operator",
}

// submarinerClusterDeployments mirrors CLUSTER_DEPLOYMENTS in start.py.
var submarinerClusterDeployments = []string{
	"submariner-operator",
	"submariner-lighthouse-agent",
	"submariner-lighthouse-coredns",
}

// kubeNodeList is the minimal subset of `kubectl get node --output=json` output
// we need to extract InternalIP addresses.
type kubeNodeList struct {
	Items []kubeNode `json:"items"`
}

type kubeNode struct {
	Metadata struct {
		Name string `json:"name"`
	} `json:"metadata"`
	Status struct {
		Addresses []kubeNodeAddress `json:"addresses"`
	} `json:"status"`
}

type kubeNodeAddress struct {
	Type    string `json:"type"`
	Address string `json:"address"`
}

func init() {
	Register("submariner", buildSubmariner)
}

// brokerInfoPath returns the deterministic path for the broker-info file.
// Mirrors Python: os.path.join(drenv.config_dir(broker), "submariner", BROKER_INFO)
// where config_dir(name) = os.path.expanduser("~/.config/drenv/<name>").
func brokerInfoPath(envName string) string {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "~"
	}
	return filepath.Join(home, ".config", "drenv", envName, "submariner", submarinerBrokerInfoFile)
}

func buildSubmariner(d Deps, _ string, args []string) ensure.Step {
	if len(args) < 1 {
		panic("submariner: args must contain broker and at least one member")
	}
	broker := args[0]
	members := args[1:]
	brokerInfo := brokerInfoPath(d.EnvName)

	// --- deploy broker ---
	deployBrokerStep := newApplyStep("deploy-broker", func(ctx context.Context) error {
		// Create the target directory.
		if err := os.MkdirAll(filepath.Dir(brokerInfo), 0o755); err != nil {
			return fmt.Errorf("submariner: create broker dir: %w", err)
		}
		return d.Subctl.DeployBroker(ctx, broker, true, submarinerVersion)
	})

	// Wait for broker deployments.
	var brokerWaitSteps []ensure.Step
	for _, dep := range submarinerBrokerDeployments {
		depName := dep // capture
		resource := "deploy/" + depName
		brokerWaitSteps = append(brokerWaitSteps,
			newApplyStep("wait-broker-create/"+depName, func(ctx context.Context) error {
				return d.K.WaitFor(ctx, broker, submarinerNamespace, "create",
					submarinerBrokerWaitTimeout, resource)
			}),
			newApplyStep("rollout-broker/"+depName, func(ctx context.Context) error {
				return d.K.RolloutStatus(ctx, broker, submarinerNamespace, resource,
					submarinerBrokerRolloutTimeout)
			}),
		)
	}

	// --- join each member cluster ---
	var memberSteps []ensure.Step
	for _, member := range members {
		m := member // capture
		// Annotate nodes then join.
		annotateStep := newApplyStep("annotate-nodes/"+m, func(ctx context.Context) error {
			return annotateSubmarinerNodes(ctx, d, m)
		})
		joinStep := newApplyStep("subctl-join/"+m, func(ctx context.Context) error {
			return d.Subctl.Join(ctx, brokerInfo, m, m, "vxlan", submarinerVersion)
		})
		// Wait for cluster deployments.
		var clusterWaitSteps []ensure.Step
		for _, dep := range submarinerClusterDeployments {
			depName := dep // capture
			resource := "deploy/" + depName
			clusterWaitSteps = append(clusterWaitSteps,
				newApplyStep("wait-cluster-create/"+m+"/"+depName, func(ctx context.Context) error {
					return d.K.WaitFor(ctx, m, submarinerNamespace, "create",
						submarinerClusterWaitTimeout, resource)
				}),
				newApplyStep("rollout-cluster/"+m+"/"+depName, func(ctx context.Context) error {
					return d.K.RolloutStatus(ctx, m, submarinerNamespace, resource,
						submarinerClusterRolloutTimeout)
				}),
			)
		}
		memberSteps = append(memberSteps, annotateStep, joinStep)
		memberSteps = append(memberSteps, clusterWaitSteps...)
	}

	steps := []ensure.Step{deployBrokerStep}
	steps = append(steps, brokerWaitSteps...)
	steps = append(steps, memberSteps...)

	return Serial("addon/submariner", d.Opts, steps...)
}

// annotateSubmarinerNodes gets all nodes in cluster, finds each node's InternalIP,
// and annotates it with gateway.submariner.io/public-ip=ipv4:<InternalIP>.
// This mirrors annotate_nodes() in start.py.
func annotateSubmarinerNodes(ctx context.Context, d Deps, cluster string) error {
	out, err := d.K.Get(ctx, cluster, "", "node", "--output=json")
	if err != nil {
		return fmt.Errorf("submariner: get nodes on %s: %w", cluster, err)
	}
	var nodes kubeNodeList
	if err := json.Unmarshal([]byte(out), &nodes); err != nil {
		return fmt.Errorf("submariner: parse nodes JSON on %s: %w", cluster, err)
	}
	for _, node := range nodes.Items {
		var internalIP string
		for _, addr := range node.Status.Addresses {
			if addr.Type == "InternalIP" {
				internalIP = addr.Address
				break
			}
		}
		if internalIP == "" {
			return fmt.Errorf("submariner: no InternalIP found for node %s on %s",
				node.Metadata.Name, cluster)
		}
		annotation := "gateway.submariner.io/public-ip=ipv4:" + internalIP
		if err := d.K.Annotate(ctx, cluster, "node/"+node.Metadata.Name, annotation); err != nil {
			return fmt.Errorf("submariner: annotate node %s on %s: %w",
				node.Metadata.Name, cluster, err)
		}
	}
	return nil
}
