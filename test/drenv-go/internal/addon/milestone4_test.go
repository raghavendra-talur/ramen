// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon_test

// milestone4_test.go exercises the Milestone-4 addon builders (velero, volsync,
// ocm/hub, ocm/cluster, submariner, argocd) via FakeRunner, asserting argv-level
// correctness against the Python source. No real clusters are used.

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// testDepsFull builds an addon.Deps that includes all M4 clients.
func testDepsFull(f *cli.FakeRunner, addonsDir, envName string) addon.Deps {
	opts := ensure.Options{
		VerifyTimeout:  2 * time.Second,
		VerifyInterval: 10 * time.Millisecond,
	}
	return addon.Deps{
		K:          &cli.Kubectl{R: f},
		MC:         &cli.MC{R: f},
		Helm:       &cli.Helm{R: f},
		Clusteradm: &cli.Clusteradm{R: f},
		Subctl:     &cli.Subctl{R: f},
		Velero:     &cli.Velero{R: f},
		Argocd:     &cli.Argocd{R: f},
		AddonsDir:  addonsDir,
		EnvName:    envName,
		Opts:       opts,
	}
}

// runStepFull invokes an addon builder and runs ensure.Ensure on the result.
func runStepFull(t *testing.T, f *cli.FakeRunner, addonsDir, envName, name, cluster string, args []string) {
	t.Helper()
	b, ok := addon.Lookup(name)
	if !ok {
		t.Fatalf("addon %q not registered", name)
	}
	d := testDepsFull(f, addonsDir, envName)
	step := b(d, cluster, args)
	if _, err := ensure.Ensure(context.Background(), step, d.Opts); err != nil {
		t.Fatalf("Ensure(%q): %v", name, err)
	}
}

// callAt returns the i-th recorded call, failing if out of range.
func callAt(t *testing.T, f *cli.FakeRunner, i int) cli.Call {
	t.Helper()
	if i >= len(f.Calls) {
		t.Fatalf("call[%d]: only %d calls recorded\n%s", i, len(f.Calls), dumpCalls(f))
	}
	return f.Calls[i]
}

// dumpCalls returns a human-readable dump of all recorded calls.
func dumpCalls(f *cli.FakeRunner) string {
	var sb strings.Builder
	for i, c := range f.Calls {
		sb.WriteString("[")
		sb.WriteString(itoa(i))
		sb.WriteString("] ")
		sb.WriteString(c.Name)
		for _, a := range c.Args {
			sb.WriteString(" ")
			sb.WriteString(a)
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := ""
	if n < 0 {
		neg = "-"
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	return neg + string(digits)
}

// assertCall fails if the i-th call doesn't match name+args exactly.
func assertCall(t *testing.T, label string, f *cli.FakeRunner, i int, wantName string, wantArgs []string) {
	t.Helper()
	c := callAt(t, f, i)
	if c.Name != wantName {
		t.Errorf("%s call[%d]: name=%q want %q", label, i, c.Name, wantName)
	}
	if !reflect.DeepEqual(c.Args, wantArgs) {
		t.Errorf("%s call[%d] args:\n got  %v\n want %v", label, i, c.Args, wantArgs)
	}
}

// assertCallContains is the subsequence version.
func assertCallContains(t *testing.T, label string, f *cli.FakeRunner, i int, want ...string) {
	t.Helper()
	c := callAt(t, f, i)
	assertArgsContain(t, label, c.Args, want...)
}

// ---- velero ----

// TestVeleroArgv verifies the velero builder: MinioServiceURL (2 kubectl calls)
// then velero install with exact flags from start.py.
//
// Python start.py flags:
//
//	velero install
//	  --provider=aws
//	  --image=quay.io/prd/velero:v1.16.1
//	  --plugins=quay.io/prd/velero-plugin-for-aws:v1.12.0,quay.io/kubevirt/kubevirt-velero-plugin:v0.8.0
//	  --bucket=bucket
//	  --secret-file=<AddonsDir>/velero/start-data/credentials.conf
//	  --use-volume-snapshots=false
//	  --backup-location-config=region=minio,s3ForcePathStyle=true,s3Url=<url>
//	  --kubecontext=<cluster>
//	  --wait
func TestVeleroArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}

	// Script MinioServiceURL calls: hostIP then nodePort.
	f.Script(cli.FakeResult{Out: "10.0.0.1"}) // kubectl get pod ... hostIP
	f.Script(cli.FakeResult{Out: "30001"})    // kubectl get service ... nodePort

	runStepFull(t, f, addonsDir, "testenv", "velero", "dr1", nil)

	if len(f.Calls) != 3 {
		t.Fatalf("expected 3 calls (hostIP, nodePort, velero install), got %d:\n%s",
			len(f.Calls), dumpCalls(f))
	}

	// call[0]: kubectl get pod (hostIP) — checked via assertArgsContain
	assertCallContains(t, "get-hostIP", f, 0,
		"--context", "dr1", "-n", "minio", "get", "pod",
		"--selector=component=minio",
		"--output=jsonpath={.items[0].status.hostIP}",
	)

	// call[1]: kubectl get service (nodePort)
	assertCallContains(t, "get-nodePort", f, 1,
		"--context", "dr1", "-n", "minio", "get",
		"service/minio",
		"--output=jsonpath={.spec.ports[0].nodePort}",
	)

	// call[2]: velero install — exact argv
	credFile := filepath.Join(addonsDir, "velero", "start-data", "credentials.conf")
	assertCall(t, "velero-install", f, 2, "velero", []string{
		"install",
		"--provider=aws",
		"--image=quay.io/prd/velero:v1.16.1",
		"--plugins=quay.io/prd/velero-plugin-for-aws:v1.12.0,quay.io/kubevirt/kubevirt-velero-plugin:v0.8.0",
		"--bucket=bucket",
		"--secret-file=" + credFile,
		"--use-volume-snapshots=false",
		"--backup-location-config=region=minio,s3ForcePathStyle=true,s3Url=http://10.0.0.1:30001",
		"--kubecontext=dr1",
		"--wait",
	})
}

// ---- volsync ----

// TestVolsyncArgv verifies the volsync builder with two clusters:
//  1. helm repo add --force-update backube https://backube.github.io/helm-charts/
//  2. helm upgrade --install volsync backube/volsync --kube-context dr1 --create-namespace --namespace volsync-system
//  3. helm upgrade --install volsync backube/volsync --kube-context dr2 --create-namespace --namespace volsync-system
//  4. kubectl rollout status volsync-system deploy/volsync on dr1
//  5. kubectl rollout status volsync-system deploy/volsync on dr2
func TestVolsyncArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}
	clusters := []string{"dr1", "dr2"}

	runStepFull(t, f, addonsDir, "testenv", "volsync", "", clusters)

	// 1 repo-add + 2 installs + 2 rollout = 5 calls
	if len(f.Calls) != 5 {
		t.Fatalf("expected 5 calls, got %d:\n%s", len(f.Calls), dumpCalls(f))
	}

	// call[0]: helm repo add
	assertCall(t, "helm-repo-add", f, 0, "helm", []string{
		"repo", "add", "--force-update",
		"backube", "https://backube.github.io/helm-charts/",
	})

	// call[1]: helm upgrade --install dr1
	assertCall(t, "helm-install-dr1", f, 1, "helm", []string{
		"upgrade", "--install",
		"volsync", "backube/volsync",
		"--kube-context", "dr1",
		"--create-namespace",
		"--namespace", "volsync-system",
	})

	// call[2]: helm upgrade --install dr2
	assertCall(t, "helm-install-dr2", f, 2, "helm", []string{
		"upgrade", "--install",
		"volsync", "backube/volsync",
		"--kube-context", "dr2",
		"--create-namespace",
		"--namespace", "volsync-system",
	})

	// call[3]: rollout status dr1
	assertCallContains(t, "rollout-dr1", f, 3,
		"--context", "dr1", "-n", "volsync-system",
		"rollout", "status", "deploy/volsync",
	)

	// call[4]: rollout status dr2
	assertCallContains(t, "rollout-dr2", f, 4,
		"--context", "dr2", "-n", "volsync-system",
		"rollout", "status", "deploy/volsync",
	)
}

// TestVolsyncSingleCluster verifies volsync with a single cluster arg.
func TestVolsyncSingleCluster(t *testing.T) {
	f := &cli.FakeRunner{}
	runStepFull(t, f, "/fake/addons", "testenv", "volsync", "", []string{"hub"})

	// 1 repo-add + 1 install + 1 rollout = 3
	if len(f.Calls) != 3 {
		t.Fatalf("expected 3 calls, got %d:\n%s", len(f.Calls), dumpCalls(f))
	}
	assertCallContains(t, "helm-install-hub", f, 1,
		"--kube-context", "hub",
	)
}

// ---- ocm/hub ----

// TestOCMHubArgv verifies the ocm/hub builder:
//   - clusteradm init with ManagedClusterAutoApproval=true --wait
//   - clusteradm install hub-addon for application-manager
//   - clusteradm install hub-addon for governance-policy-framework
//   - For each deployment in open-cluster-management (7) + open-cluster-management-hub (4):
//     wait --for=create + rollout status (2 calls each = 22 calls)
//
// Total: 1 + 2 + 22 = 25 calls.
func TestOCMHubArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}

	runStepFull(t, f, addonsDir, "testenv", "ocm/hub", "hub", nil)

	// 1 init + 2 installs + (7+4)*2 wait/rollout = 25
	expected := 1 + 2 + (7+4)*2
	if len(f.Calls) != expected {
		t.Fatalf("expected %d calls, got %d:\n%s", expected, len(f.Calls), dumpCalls(f))
	}

	// call[0]: clusteradm init
	assertCall(t, "clusteradm-init", f, 0, "clusteradm", []string{
		"init",
		"--feature-gates=ManagedClusterAutoApproval=true",
		"--wait",
		"--context", "hub",
	})

	// call[1]: clusteradm install hub-addon application-manager
	assertCall(t, "install-application-manager", f, 1, "clusteradm", []string{
		"install", "hub-addon",
		"--names=application-manager",
		"--context", "hub",
	})

	// call[2]: clusteradm install hub-addon governance-policy-framework
	assertCall(t, "install-governance-policy-framework", f, 2, "clusteradm", []string{
		"install", "hub-addon",
		"--names=governance-policy-framework",
		"--context", "hub",
	})

	// call[3]: wait --for=create deploy/cluster-manager in open-cluster-management
	assertCallContains(t, "wait-create-cluster-manager", f, 3,
		"--context", "hub", "-n", "open-cluster-management",
		"wait", "deploy/cluster-manager", "--for=create",
	)

	// call[4]: rollout status deploy/cluster-manager
	assertCallContains(t, "rollout-cluster-manager", f, 4,
		"--context", "hub", "-n", "open-cluster-management",
		"rollout", "status", "deploy/cluster-manager",
	)

	// The last deployment in open-cluster-management-hub is cluster-manager-work-webhook.
	// call[(1+2 + (7+4-1)*2)] and call[(1+2 + (7+4-1)*2)+1]
	lastWaitIdx := 1 + 2 + (11-1)*2
	assertCallContains(t, "wait-create-work-webhook", f, lastWaitIdx,
		"--context", "hub", "-n", "open-cluster-management-hub",
		"wait", "deploy/cluster-manager-work-webhook", "--for=create",
	)
	assertCallContains(t, "rollout-work-webhook", f, lastWaitIdx+1,
		"--context", "hub", "-n", "open-cluster-management-hub",
		"rollout", "status", "deploy/cluster-manager-work-webhook",
	)
}

// ---- ocm/cluster ----

// TestOCMClusterArgv verifies the ocm/cluster builder (cross-cluster).
// cluster = "dr1", hub = args[0] = "hub".
//
// Call count:
//   - wait_for_hub: (7+4) deployments * 2 = 22
//   - join: 1 clusteradm get token + 1 clusteradm join = 2
//   - wait_for_managed_cluster: 1 wait-create + 1 wait-hubAcceptsClient + 3 conditions = 5
//   - label: 1
//   - enable_addons: 1
//   - wait addon deployments: 3 addons * 2 = 6
//
// Total: 22 + 2 + 5 + 1 + 1 + 6 = 37
func TestOCMClusterArgv(t *testing.T) {
	f := &cli.FakeRunner{}

	// Script the clusteradm get token call to return parseable JSON.
	// Call order: 22 kubectl waits/rollouts for hub, then get token at call[22].
	hubDeploymentCalls := (7 + 4) * 2 // 22
	for i := 0; i < hubDeploymentCalls; i++ {
		f.Script(cli.FakeResult{}) // kubectl wait/rollout — success
	}
	// call[22]: clusteradm get token
	f.Script(cli.FakeResult{Out: `{"hub-token":"tok123","hub-apiserver":"https://192.168.1.1:6443"}`})
	// Remaining calls: default (nil error, empty output)

	runStepFull(t, f, "/fake/addons", "testenv", "ocm/cluster", "dr1", []string{"hub"})

	expected := 22 + 2 + 5 + 1 + 1 + 6
	if len(f.Calls) != expected {
		t.Fatalf("expected %d calls, got %d:\n%s", expected, len(f.Calls), dumpCalls(f))
	}

	// call[0..21]: hub deployment wait/rollout on hub context
	assertCallContains(t, "first-hub-wait", f, 0,
		"--context", "hub", "-n", "open-cluster-management",
		"wait", "deploy/cluster-manager", "--for=create",
	)

	// call[22]: clusteradm get token
	assertCall(t, "get-token", f, hubDeploymentCalls, "clusteradm", []string{
		"get", "token", "--output=json", "--context", "hub",
	})

	// call[23]: clusteradm join
	assertCall(t, "join", f, hubDeploymentCalls+1, "clusteradm", []string{
		"join",
		"--hub-token=tok123",
		"--hub-apiserver=https://192.168.1.1:6443",
		"--cluster-name=dr1",
		"--context", "dr1",
	})

	// call[24]: wait managedcluster/dr1 --for=create (180s) on hub
	assertCallContains(t, "wait-mc-create", f, hubDeploymentCalls+2,
		"--context", "hub",
		"wait", "managedcluster/dr1", "--for=create",
	)

	// call[25]: wait managedcluster/dr1 --for=jsonpath={.spec.hubAcceptsClient}=true on hub
	assertCallContains(t, "wait-mc-hubAcceptsClient", f, hubDeploymentCalls+3,
		"--context", "hub",
		"wait", "managedcluster/dr1",
		"--for=jsonpath={.spec.hubAcceptsClient}=true",
	)

	// call[26]: wait --for=condition=HubAcceptedManagedCluster
	assertCallContains(t, "wait-HubAccepted", f, hubDeploymentCalls+4,
		"--context", "hub",
		"wait", "managedcluster/dr1",
		"--for=condition=HubAcceptedManagedCluster",
	)

	// call[27]: wait --for=condition=ManagedClusterJoined
	assertCallContains(t, "wait-Joined", f, hubDeploymentCalls+5,
		"--context", "hub",
		"wait", "managedcluster/dr1",
		"--for=condition=ManagedClusterJoined",
	)

	// call[28]: wait --for=condition=ManagedClusterConditionAvailable
	assertCallContains(t, "wait-Available", f, hubDeploymentCalls+6,
		"--context", "hub",
		"wait", "managedcluster/dr1",
		"--for=condition=ManagedClusterConditionAvailable",
	)

	// call[29]: kubectl label managedclusters/dr1 name=dr1 --overwrite on hub
	assertCall(t, "label-cluster", f, hubDeploymentCalls+7, "kubectl", []string{
		"--context", "hub",
		"label", "managedclusters/dr1", "name=dr1",
		"--overwrite",
	})

	// call[30]: clusteradm addon enable
	assertCall(t, "enable-addons", f, hubDeploymentCalls+8, "clusteradm", []string{
		"addon", "enable",
		"--names=application-manager,governance-policy-framework,config-policy-controller",
		"--clusters=dr1",
		"--context", "hub",
	})

	// call[31]: wait deploy/application-manager create in open-cluster-management-agent-addon on dr1
	assertCallContains(t, "wait-addon-create-application-manager", f, hubDeploymentCalls+9,
		"--context", "dr1",
		"-n", "open-cluster-management-agent-addon",
		"wait", "deploy/application-manager", "--for=create",
	)

	// call[32]: rollout status deploy/application-manager on dr1
	assertCallContains(t, "rollout-application-manager", f, hubDeploymentCalls+10,
		"--context", "dr1",
		"-n", "open-cluster-management-agent-addon",
		"rollout", "status", "deploy/application-manager",
	)
}

// ---- submariner ----

// TestSubmarinerArgv verifies the submariner builder (cross-cluster).
// broker = "hub", members = ["dr1", "dr2"].
//
// Call count:
//   - deploy_broker: subctl deploy-broker (1) + wait broker deploy (2) = 3
//   - for dr1: kubectl get nodes (1) + kubectl annotate per node + subctl join (1) + wait cluster (6)
//   - for dr2: same
//
// For the unit test, kubectl get node returns a single-node JSON,
// so each cluster contributes: 1 get + 1 annotate + 1 join + 6 wait/rollout = 9.
// Total: 3 + 2*9 = 21.
func TestSubmarinerArgv(t *testing.T) {
	f := &cli.FakeRunner{}

	nodeJSON := `{"items":[{"metadata":{"name":"node1"},"status":{"addresses":[{"type":"InternalIP","address":"10.0.0.2"}]}}]}`

	// deploy-broker (1 call, no output needed)
	f.Script(cli.FakeResult{}) // subctl deploy-broker
	// broker wait: 2 calls (wait-create + rollout)
	f.Script(cli.FakeResult{}) // wait
	f.Script(cli.FakeResult{}) // rollout

	// dr1: get nodes, annotate, join, 3 wait+rollout pairs
	f.Script(cli.FakeResult{Out: nodeJSON}) // get nodes
	f.Script(cli.FakeResult{})              // annotate node1
	f.Script(cli.FakeResult{})              // subctl join
	for i := 0; i < 6; i++ {
		f.Script(cli.FakeResult{}) // wait + rollout * 3 deployments
	}

	// dr2: same
	f.Script(cli.FakeResult{Out: nodeJSON}) // get nodes
	f.Script(cli.FakeResult{})              // annotate
	f.Script(cli.FakeResult{})              // subctl join
	for i := 0; i < 6; i++ {
		f.Script(cli.FakeResult{})
	}

	runStepFull(t, f, "/fake/addons", "myenv", "submariner", "", []string{"hub", "dr1", "dr2"})

	expected := 3 + 2*9
	if len(f.Calls) != expected {
		t.Fatalf("expected %d calls, got %d:\n%s", expected, len(f.Calls), dumpCalls(f))
	}

	// call[0]: subctl deploy-broker hub --globalnet --version 0.21.2
	assertCall(t, "deploy-broker", f, 0, "subctl", []string{
		"deploy-broker",
		"--context", "hub",
		"--globalnet",
		"--version", "0.21.2",
	})

	// call[1]: kubectl wait deploy/submariner-operator --for=create in submariner-operator on hub
	assertCallContains(t, "wait-broker-create", f, 1,
		"--context", "hub", "-n", "submariner-operator",
		"wait", "deploy/submariner-operator", "--for=create",
	)

	// call[2]: rollout status deploy/submariner-operator on hub
	assertCallContains(t, "rollout-broker", f, 2,
		"--context", "hub", "-n", "submariner-operator",
		"rollout", "status", "deploy/submariner-operator",
	)

	// call[3]: kubectl get node --output=json on dr1
	assertCall(t, "get-nodes-dr1", f, 3, "kubectl", []string{
		"--context", "dr1",
		"-n", "",
		"get", "node", "--output=json",
	})

	// call[4]: kubectl annotate node/node1 gateway.submariner.io/public-ip=ipv4:10.0.0.2 --overwrite on dr1
	assertCall(t, "annotate-dr1", f, 4, "kubectl", []string{
		"--context", "dr1",
		"annotate", "node/node1",
		"gateway.submariner.io/public-ip=ipv4:10.0.0.2",
		"--overwrite",
	})

	// call[5]: subctl join <brokerInfo> --context dr1 --clusterid dr1 --cable-driver vxlan --version 0.21.2
	brokerInfo := brokerInfoPathFor("myenv")
	assertCall(t, "subctl-join-dr1", f, 5, "subctl", []string{
		"join", brokerInfo,
		"--context", "dr1",
		"--clusterid", "dr1",
		"--cable-driver", "vxlan",
		"--version", "0.21.2",
		"--check-broker-certificate=false",
	})

	// call[6]: wait create submariner-operator on dr1
	assertCallContains(t, "wait-cluster-create-dr1", f, 6,
		"--context", "dr1", "-n", "submariner-operator",
		"wait", "deploy/submariner-operator", "--for=create",
	)

	// calls 7-11: rollout + wait/rollout for lighthouse-agent + lighthouse-coredns on dr1
	assertCallContains(t, "rollout-operator-dr1", f, 7,
		"--context", "dr1", "-n", "submariner-operator",
		"rollout", "status", "deploy/submariner-operator",
	)

	// dr2 starts at call[12]
	assertCall(t, "get-nodes-dr2", f, 12, "kubectl", []string{
		"--context", "dr2",
		"-n", "",
		"get", "node", "--output=json",
	})
	// call[14]: subctl join dr2
	assertCall(t, "subctl-join-dr2", f, 14, "subctl", []string{
		"join", brokerInfo,
		"--context", "dr2",
		"--clusterid", "dr2",
		"--cable-driver", "vxlan",
		"--version", "0.21.2",
		"--check-broker-certificate=false",
	})
}

// brokerInfoPathFor returns the expected broker-info path for a given envName,
// mirroring brokerInfoPath() in submariner_addon.go.
func brokerInfoPathFor(envName string) string {
	// Use os.UserHomeDir equivalent by calling addon's exposed path function
	// indirectly — we test via a helper to avoid importing os in tests.
	// The formula is: home + "/.config/drenv/" + envName + "/submariner/broker-info.subm"
	// For tests we just rebuild the path the same way.
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "drenv", envName, "submariner", "broker-info.subm")
}

// ---- argocd ----

// TestArgocdArgv verifies the argocd builder (cross-cluster).
// hub = args[0] = "hub", members = ["dr1", "dr2"].
//
// Call count:
//   - apply: 1 kubectl apply
//   - wait: 1 kubectl wait
//   - add-cluster dr1: kubectl config view (1) + config use-context (1) + config set-context (1)
//   - argocd login (1) + argocd cluster add (1) = 5
//   - add-cluster dr2: same = 5
//
// Total: 2 + 2*5 = 12.
func TestArgocdArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}

	// Script the kubectl config view calls to return non-empty yaml.
	f.Script(cli.FakeResult{})                        // apply
	f.Script(cli.FakeResult{})                        // wait
	f.Script(cli.FakeResult{Out: "apiVersion: v1\n"}) // config view (dr1)
	f.Script(cli.FakeResult{})                        // config use-context (dr1)
	f.Script(cli.FakeResult{})                        // config set-context (dr1)
	// argocd login and cluster add (dr1) consume from queue (nil error)
	f.Script(cli.FakeResult{Out: "apiVersion: v1\n"}) // config view (dr2)
	f.Script(cli.FakeResult{})                        // config use-context (dr2)
	f.Script(cli.FakeResult{})                        // config set-context (dr2)

	runStepFull(t, f, addonsDir, "myenv", "argocd", "", []string{"hub", "dr1", "dr2"})

	expected := 12
	if len(f.Calls) != expected {
		t.Fatalf("expected %d calls, got %d:\n%s", expected, len(f.Calls), dumpCalls(f))
	}

	startDataDir := filepath.Join(addonsDir, "argocd", "start-data")

	// call[0]: kubectl apply --kustomize <start-data> --namespace argocd on hub
	assertCall(t, "apply-argocd", f, 0, "kubectl", []string{
		"--context", "hub",
		"apply",
		"--kustomize", startDataDir,
		"--namespace", "argocd",
	})

	// call[1]: kubectl wait deploy --all --for=condition=Available --namespace=argocd on hub
	assertCallContains(t, "wait-argocd", f, 1,
		"--context", "hub",
		"-n", "argocd",
		"wait", "deploy", "--all",
		"--for=condition=Available",
	)

	// call[2]: kubectl config view --flatten --output=yaml (for dr1)
	assertCall(t, "config-view-dr1", f, 2, "kubectl", []string{
		"config", "view", "--flatten", "--output=yaml",
	})

	// call[3]: kubectl config use-context hub --kubeconfig <kc>
	assertCallContains(t, "config-use-context-dr1", f, 3,
		"config", "use-context", "hub", "--kubeconfig",
	)

	// call[4]: kubectl config set-context --current --namespace=argocd --kubeconfig=<kc>
	assertCallContains(t, "config-set-context-dr1", f, 4,
		"config", "set-context", "--current", "--namespace=argocd",
	)

	// call[5]: argocd login --core (RunEnv)
	if f.Calls[5].Name != "argocd" {
		t.Errorf("call[5] name=%q, want argocd", f.Calls[5].Name)
	}
	if !reflect.DeepEqual(f.Calls[5].Args, []string{"login", "--core"}) {
		t.Errorf("call[5] argocd login args=%v, want [login --core]", f.Calls[5].Args)
	}

	// call[6]: argocd cluster add dr1 -y (RunEnv)
	if f.Calls[6].Name != "argocd" {
		t.Errorf("call[6] name=%q, want argocd", f.Calls[6].Name)
	}
	if !reflect.DeepEqual(f.Calls[6].Args, []string{"cluster", "add", "dr1", "-y"}) {
		t.Errorf("call[6] argocd cluster add args=%v, want [cluster add dr1 -y]", f.Calls[6].Args)
	}

	// call[7]: kubectl config view (for dr2)
	assertCall(t, "config-view-dr2", f, 7, "kubectl", []string{
		"config", "view", "--flatten", "--output=yaml",
	})

	// call[11]: argocd cluster add dr2 -y
	if !reflect.DeepEqual(f.Calls[11].Args, []string{"cluster", "add", "dr2", "-y"}) {
		t.Errorf("call[11] args=%v, want [cluster add dr2 -y]", f.Calls[11].Args)
	}
}
