// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rook_test.go exercises the rook-operator, rook-cluster, rook-toolbox,
// rook-pool, rook-cephfs, and rbd-mirror addon builders via FakeRunner,
// asserting argv-level correctness against the Python source. No real
// clusters are used.

package addon_test

import (
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

// ---- rook-operator ----

// TestRookOperatorArgv verifies the rook-operator builder:
//  1. kubectl apply -k <AddonsDir>/rook/operator
//  2. kubectl rollout status rook-ceph deploy/rook-ceph-operator (600s)
//  3. kubectl wait pod --selector=app=rook-ceph-operator
//     --for=jsonpath={.status.phase}=Running -n rook-ceph (300s)
func TestRookOperatorArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}
	gateNotReady(f)

	runStep(t, f, addonsDir, "rook-operator", "dr1", nil)
	stripGateCall(f)

	if len(f.Calls) != 3 {
		t.Fatalf("expected 3 calls, got %d:\n%s", len(f.Calls), strings.Join(callNames(f), "\n"))
	}

	operatorDir := filepath.Join(addonsDir, "rook", "operator")

	// call[0]: kubectl apply -k <operator-dir>
	assertArgsEqual(t, "apply-operator", callArgs(t, f, 0), []string{
		"--context", "dr1", "apply", "--kustomize", operatorDir,
	})

	// call[1]: kubectl rollout status deploy/rook-ceph-operator (600s)
	assertArgsContain(t, "rollout-operator", callArgs(t, f, 1),
		"--context", "dr1", "-n", "rook-ceph",
		"rollout", "status", "deploy/rook-ceph-operator",
		"--timeout", "600s",
	)

	// call[2]: kubectl wait pod --selector=... --for=jsonpath=...=Running (300s)
	assertArgsContain(t, "wait-running", callArgs(t, f, 2),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "pod", "--selector=app=rook-ceph-operator",
		"--for=jsonpath={.status.phase}=Running",
		"--timeout", "300s",
	)
}

// ---- rook-cluster ----

// TestRookClusterArgv verifies the rook-cluster builder.
// Expected calls:
//  1. apply -k rook/cluster
//  2. wait cephcluster/my-cluster --for=create -n rook-ceph (300s)
//  3. wait cephcluster/my-cluster --for=jsonpath={.status.phase}=Ready -n rook-ceph (600s)
//     4-7. rollout status for 4 CSI components (daemonsets + deployments)
//     8-13. for 3 CSIAddonsNodes: wait --for=create + wait --for=jsonpath=Connected (each pair)
//
// Total: 1 + 1 + 1 + 4 + (3*2) = 13 calls.
func TestRookClusterArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}

	runStep(t, f, addonsDir, "rook-cluster", "dr1", nil)

	expected := 1 + 1 + 1 + 4 + (3 * 2) // 13
	if len(f.Calls) != expected {
		t.Fatalf("expected %d calls, got %d:\n%s", expected, len(f.Calls), strings.Join(callNames(f), "\n"))
	}

	clusterDir := filepath.Join(addonsDir, "rook", "cluster")

	// call[0]: apply -k rook/cluster
	assertArgsEqual(t, "apply-cluster", callArgs(t, f, 0), []string{
		"--context", "dr1", "apply", "--kustomize", clusterDir,
	})

	// call[1]: wait cephcluster/my-cluster --for=create (300s)
	assertArgsContain(t, "wait-create", callArgs(t, f, 1),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephcluster/my-cluster", "--for=create", "--timeout", "300s",
	)

	// call[2]: wait cephcluster/my-cluster --for=jsonpath=Ready (600s)
	assertArgsContain(t, "wait-ready", callArgs(t, f, 2),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephcluster/my-cluster",
		"--for=jsonpath={.status.phase}=Ready",
		"--timeout", "600s",
	)

	// call[3]: rollout daemonset/csi-rbdplugin
	assertArgsContain(t, "rollout-csi-rbdplugin", callArgs(t, f, 3),
		"--context", "dr1", "-n", "rook-ceph",
		"rollout", "status", "daemonset/csi-rbdplugin",
	)

	// call[4]: rollout daemonset/csi-cephfsplugin
	assertArgsContain(t, "rollout-csi-cephfsplugin", callArgs(t, f, 4),
		"--context", "dr1", "-n", "rook-ceph",
		"rollout", "status", "daemonset/csi-cephfsplugin",
	)

	// call[5]: rollout deployment/csi-rbdplugin-provisioner
	assertArgsContain(t, "rollout-csi-rbdplugin-provisioner", callArgs(t, f, 5),
		"--context", "dr1", "-n", "rook-ceph",
		"rollout", "status", "deployment/csi-rbdplugin-provisioner",
	)

	// call[6]: rollout deployment/csi-cephfsplugin-provisioner
	assertArgsContain(t, "rollout-csi-cephfsplugin-provisioner", callArgs(t, f, 6),
		"--context", "dr1", "-n", "rook-ceph",
		"rollout", "status", "deployment/csi-cephfsplugin-provisioner",
	)

	// call[7]: wait csiaddonsnodes.../dr1-rook-ceph-daemonset-csi-rbdplugin --for=create
	node0 := "csiaddonsnodes.csiaddons.openshift.io/dr1-rook-ceph-daemonset-csi-rbdplugin"
	assertArgsContain(t, "wait-csiaddon0-create", callArgs(t, f, 7),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", node0, "--for=create",
	)

	// call[8]: wait csiaddonsnodes.../dr1-rook-ceph-daemonset-csi-rbdplugin --for=jsonpath=Connected
	assertArgsContain(t, "wait-csiaddon0-connected", callArgs(t, f, 8),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", node0, "--for=jsonpath={.status.state}=Connected",
	)

	// call[9]: wait csiaddonsnodes.../dr1-rook-ceph-deployment-csi-rbdplugin-provisioner --for=create
	node1 := "csiaddonsnodes.csiaddons.openshift.io/dr1-rook-ceph-deployment-csi-rbdplugin-provisioner"
	assertArgsContain(t, "wait-csiaddon1-create", callArgs(t, f, 9),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", node1, "--for=create",
	)

	// call[10]: wait node1 --for=jsonpath=Connected
	assertArgsContain(t, "wait-csiaddon1-connected", callArgs(t, f, 10),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", node1, "--for=jsonpath={.status.state}=Connected",
	)

	// call[11]: wait csiaddonsnodes.../dr1-rook-ceph-deployment-csi-cephfsplugin-provisioner --for=create
	node2 := "csiaddonsnodes.csiaddons.openshift.io/dr1-rook-ceph-deployment-csi-cephfsplugin-provisioner"
	assertArgsContain(t, "wait-csiaddon2-create", callArgs(t, f, 11),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", node2, "--for=create",
	)

	// call[12]: wait node2 --for=jsonpath=Connected
	assertArgsContain(t, "wait-csiaddon2-connected", callArgs(t, f, 12),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", node2, "--for=jsonpath={.status.state}=Connected",
	)
}

// ---- rook-toolbox ----

// TestRookToolboxArgv verifies the rook-toolbox builder:
//  1. kubectl apply -k <AddonsDir>/rook/toolbox
//  2. kubectl rollout status rook-ceph deploy/rook-ceph-tools (300s)
//  3. kubectl exec deploy/rook-ceph-tools -n rook-ceph -- ceph status
func TestRookToolboxArgv(t *testing.T) {
	addonsDir := "/fake/addons"
	f := &cli.FakeRunner{}

	// Script exec to return something
	gateNotReady(f)                              // readiness gate: deploy/rook-ceph-tools not ready → run
	f.Script(cli.FakeResult{})                   // apply
	f.Script(cli.FakeResult{})                   // rollout
	f.Script(cli.FakeResult{Out: "HEALTH_OK\n"}) // exec ceph status

	runStep(t, f, addonsDir, "rook-toolbox", "dr1", nil)
	stripGateCall(f)

	if len(f.Calls) != 3 {
		t.Fatalf("expected 3 calls, got %d:\n%s", len(f.Calls), strings.Join(callNames(f), "\n"))
	}

	toolboxDir := filepath.Join(addonsDir, "rook", "toolbox")

	// call[0]: apply -k rook/toolbox
	assertArgsEqual(t, "apply-toolbox", callArgs(t, f, 0), []string{
		"--context", "dr1", "apply", "--kustomize", toolboxDir,
	})

	// call[1]: rollout status deploy/rook-ceph-tools
	assertArgsContain(t, "rollout-toolbox", callArgs(t, f, 1),
		"--context", "dr1", "-n", "rook-ceph",
		"rollout", "status", "deploy/rook-ceph-tools",
	)

	// call[2]: exec deploy/rook-ceph-tools -- ceph status
	assertArgsEqual(t, "ceph-status", callArgs(t, f, 2), []string{
		"--context", "dr1", "-n", "rook-ceph",
		"exec", "deploy/rook-ceph-tools", "--", "ceph", "status",
	})
}

// ---- rook-pool ----

// TestRookPoolArgv verifies the rook-pool builder against the real template files.
// We use the actual addons directory so ApplyTemplate can read the .yaml files.
// Call sequence:
//  1. apply stdin (storageclass rook-ceph-block)
//  2. apply stdin (storageclass rook-ceph-block-2)
//  3. apply stdin (pool replicapool)
//  4. apply stdin (pool replicapool-2)
//  5. apply stdin (snapshot-class)
//  6. wait cephblockpool/replicapool --for=create (300s)
//  7. wait cephblockpool/replicapool --for=jsonpath=Ready (300s)
//  8. wait cephblockpool/replicapool --for=jsonpath=peerSecretName=pool-peer-token-replicapool (300s)
func TestRookPoolArgv(t *testing.T) {
	// Use the real addons directory to test actual template rendering.
	addonsDir := rookAddonsDir(t)
	f := &cli.FakeRunner{}

	runStep(t, f, addonsDir, "rook-pool", "dr1", nil)

	expected := 5 + 3 // 8 calls
	if len(f.Calls) != expected {
		t.Fatalf("expected %d calls, got %d:\n%s", expected, len(f.Calls), strings.Join(callNames(f), "\n"))
	}

	// call[0]: apply stdin for storageclass rook-ceph-block
	c := f.Calls[0]
	if c.Name != "kubectl" {
		t.Errorf("call[0] name=%q, want kubectl", c.Name)
	}
	assertArgsContain(t, "apply-sc1", c.Args,
		"--context", "dr1", "apply", "--filename", "-",
	)
	if !strings.Contains(c.Stdin, "rook-ceph-block") {
		t.Errorf("apply-sc1 stdin should contain 'rook-ceph-block'")
	}
	if !strings.Contains(c.Stdin, "replicapool") {
		t.Errorf("apply-sc1 stdin should reference pool 'replicapool', got: %s", c.Stdin[:min(200, len(c.Stdin))])
	}
	if !strings.Contains(c.Stdin, "dr1") {
		t.Errorf("apply-sc1 stdin should contain cluster 'dr1'")
	}

	// call[1]: apply stdin for storageclass rook-ceph-block-2
	c = f.Calls[1]
	assertArgsContain(t, "apply-sc2", c.Args,
		"--context", "dr1", "apply", "--filename", "-",
	)
	if !strings.Contains(c.Stdin, "rook-ceph-block-2") {
		t.Errorf("apply-sc2 stdin should contain 'rook-ceph-block-2'")
	}

	// call[2]: apply stdin for pool replicapool
	c = f.Calls[2]
	if !strings.Contains(c.Stdin, "replicapool") {
		t.Errorf("apply-pool1 stdin should contain 'replicapool'")
	}
	if strings.Contains(c.Stdin, "replicapool-2") {
		t.Errorf("apply-pool1 stdin should NOT contain 'replicapool-2', got first pool only")
	}

	// call[3]: apply stdin for pool replicapool-2
	c = f.Calls[3]
	if !strings.Contains(c.Stdin, "replicapool-2") {
		t.Errorf("apply-pool2 stdin should contain 'replicapool-2'")
	}

	// call[4]: apply stdin for snapshot class
	c = f.Calls[4]
	if !strings.Contains(c.Stdin, "VolumeSnapshotClass") {
		t.Errorf("apply-snapclass stdin should contain 'VolumeSnapshotClass'")
	}
	if !strings.Contains(c.Stdin, "rook-ceph-block-dr1-1") {
		t.Errorf("apply-snapclass stdin should contain storageID 'rook-ceph-block-dr1-1'")
	}

	// call[5]: wait cephblockpool/replicapool --for=create
	assertArgsContain(t, "wait-create", callArgs(t, f, 5),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephblockpool/replicapool", "--for=create",
	)

	// call[6]: wait --for=jsonpath=Ready
	assertArgsContain(t, "wait-ready", callArgs(t, f, 6),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephblockpool/replicapool",
		"--for=jsonpath={.status.phase}=Ready",
	)

	// call[7]: wait --for=jsonpath=peer token
	assertArgsContain(t, "wait-peer-token", callArgs(t, f, 7),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephblockpool/replicapool",
		"--for=jsonpath={.status.info.rbdMirrorBootstrapPeerSecretName}=pool-peer-token-replicapool",
	)
}

// ---- rook-cephfs ----

// TestRookCephFSArgv verifies the rook-cephfs builder against the real templates.
// Call sequence:
//  1. apply stdin (filesystem fs1)
//  2. apply stdin (storageclass rook-cephfs-fs1)
//  3. apply stdin (filesystem fs2)
//  4. apply stdin (storageclass rook-cephfs-fs2)
//  5. apply stdin (snapshot-class, scname=rook-cephfs-fs1)
//  6. wait cephfilesystem/fs1 --for=create (300s)
//  7. wait cephfilesystem/fs1 --for=jsonpath=Ready (300s)
//  8. wait cephfilesystem/fs2 --for=create (300s)
//  9. wait cephfilesystem/fs2 --for=jsonpath=Ready (300s)
func TestRookCephFSArgv(t *testing.T) {
	addonsDir := rookAddonsDir(t)
	f := &cli.FakeRunner{}

	runStep(t, f, addonsDir, "rook-cephfs", "dr1", nil)

	expected := 5 + 4 // 9 calls
	if len(f.Calls) != expected {
		t.Fatalf("expected %d calls, got %d:\n%s", expected, len(f.Calls), strings.Join(callNames(f), "\n"))
	}

	// call[0]: apply filesystem fs1
	c := f.Calls[0]
	assertArgsContain(t, "apply-fs1", c.Args, "--context", "dr1", "apply", "--filename", "-")
	if !strings.Contains(c.Stdin, "name: fs1") {
		t.Errorf("apply-fs1 stdin should contain 'name: fs1'")
	}

	// call[1]: apply storageclass rook-cephfs-fs1
	c = f.Calls[1]
	assertArgsContain(t, "apply-sc-fs1", c.Args, "--context", "dr1", "apply", "--filename", "-")
	if !strings.Contains(c.Stdin, "rook-cephfs-fs1") {
		t.Errorf("apply-sc-fs1 stdin should contain 'rook-cephfs-fs1'")
	}
	if !strings.Contains(c.Stdin, "fsName: fs1") {
		t.Errorf("apply-sc-fs1 stdin should contain 'fsName: fs1'")
	}

	// call[2]: apply filesystem fs2
	c = f.Calls[2]
	if !strings.Contains(c.Stdin, "name: fs2") {
		t.Errorf("apply-fs2 stdin should contain 'name: fs2'")
	}

	// call[3]: apply storageclass rook-cephfs-fs2
	c = f.Calls[3]
	if !strings.Contains(c.Stdin, "rook-cephfs-fs2") {
		t.Errorf("apply-sc-fs2 stdin should contain 'rook-cephfs-fs2'")
	}

	// call[4]: apply snapshot class (scname=rook-cephfs-fs1)
	c = f.Calls[4]
	if !strings.Contains(c.Stdin, "VolumeSnapshotClass") {
		t.Errorf("apply-snapclass stdin should contain 'VolumeSnapshotClass'")
	}
	if !strings.Contains(c.Stdin, "rook-cephfs-fs1") {
		t.Errorf("apply-snapclass stdin should contain 'rook-cephfs-fs1'")
	}

	// call[5]: wait cephfilesystem/fs1 --for=create
	assertArgsContain(t, "wait-fs1-create", callArgs(t, f, 5),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephfilesystem/fs1", "--for=create",
	)

	// call[6]: wait cephfilesystem/fs1 --for=jsonpath=Ready
	assertArgsContain(t, "wait-fs1-ready", callArgs(t, f, 6),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephfilesystem/fs1", "--for=jsonpath={.status.phase}=Ready",
	)

	// call[7]: wait cephfilesystem/fs2 --for=create
	assertArgsContain(t, "wait-fs2-create", callArgs(t, f, 7),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephfilesystem/fs2", "--for=create",
	)

	// call[8]: wait cephfilesystem/fs2 --for=jsonpath=Ready
	assertArgsContain(t, "wait-fs2-ready", callArgs(t, f, 8),
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephfilesystem/fs2", "--for=jsonpath={.status.phase}=Ready",
	)
}

// ---- rbd-mirror ----

// TestRBDMirrorArgv verifies the rbd-mirror builder (cross-cluster).
// cluster="" (global addon), args=["dr1", "dr2"].
//
// Call sequence (using real addons templates for stdin assertions):
//
//	For each cluster (fetch phase — sequential):
//	 [0]  wait cephblockpools.ceph.rook.io/replicapool --for=jsonpath=site_name (dr1)
//	 [1]  get cephblockpools.ceph.rook.io replicapool --output=jsonpath=site_name (dr1) → "site-dr1"
//	 [2]  get cephblockpools.ceph.rook.io replicapool --output=jsonpath=peerSecretName (dr1) → "token-secret-dr1"
//	 [3]  get secret token-secret-dr1 --output=jsonpath={.data.token} (dr1) → "token-dr1-b64"
//	 [4]  wait ... site_name (dr2) → "site-dr2"
//	 [5]  get ... site_name (dr2) → "site-dr2"
//	 [6]  get ... peerSecretName (dr2) → "token-secret-dr2"
//	 [7]  get secret token-secret-dr2 --output=jsonpath={.data.token} (dr2) → "token-dr2-b64"
//	configure_mirroring(dr1, c2Info):
//	 [8]  apply stdin rbd-mirror-secret (--namespace=rook-ceph) on dr1
//	 [9]  patch cephblockpool/replicapool --type=merge --patch=... on dr1
//	 [10] apply stdin vrc-1m on dr1
//	 [11] apply stdin vrc-5m on dr1
//	 [12] apply -k rbd_mirror/start-data on dr1
//	configure_mirroring(dr2, c1Info):
//	 [13] apply stdin rbd-mirror-secret on dr2
//	 [14] patch on dr2
//	 [15] apply stdin vrc-1m on dr2
//	 [16] apply stdin vrc-5m on dr2
//	 [17] apply -k on dr2
//	wait_until_ready(dr1):
//	 [18] wait cephrbdmirror/my-rbd-mirror --for=create (dr1)
//	 [19] wait cephrbdmirror/my-rbd-mirror --for=jsonpath=Ready (dr1)
//	wait_until_ready(dr2):
//	 [20] wait cephrbdmirror/my-rbd-mirror --for=create (dr2)
//	 [21] wait cephrbdmirror/my-rbd-mirror --for=jsonpath=Ready (dr2)
//	wait_until_pool_mirroring_is_healthy(dr1):
//	 [22] get cephblockpool/replicapool --output=jsonpath=mirroringStatus.summary (dr1)
//	wait_until_pool_mirroring_is_healthy(dr2):
//	 [23] get cephblockpool/replicapool --output=jsonpath=mirroringStatus.summary (dr2)
//
// Total: 24 calls.
func TestRBDMirrorArgv(t *testing.T) {
	addonsDir := rookAddonsDir(t)
	f := &cli.FakeRunner{}

	// Script fetch_secret_info for dr1 (calls [0..3])
	f.Script(cli.FakeResult{})                        // [0] wait site_name (dr1)
	f.Script(cli.FakeResult{Out: "site-dr1"})         // [1] get site_name (dr1)
	f.Script(cli.FakeResult{Out: "token-secret-dr1"}) // [2] get peer secret name (dr1)
	f.Script(cli.FakeResult{Out: "tokendr1b64=="})    // [3] get token from secret (dr1)

	// Script fetch_secret_info for dr2 (calls [4..7])
	f.Script(cli.FakeResult{})                        // [4] wait site_name (dr2)
	f.Script(cli.FakeResult{Out: "site-dr2"})         // [5] get site_name (dr2)
	f.Script(cli.FakeResult{Out: "token-secret-dr2"}) // [6] get peer secret name (dr2)
	f.Script(cli.FakeResult{Out: "tokendr2b64=="})    // [7] get token from secret (dr2)

	// configure_mirroring×2 (10 calls) + wait_ready×2 (4 calls): empty results.
	for i := 0; i < 14; i++ {
		f.Script(cli.FakeResult{})
	}
	// wait_healthy×2: each polls 3 summary fields, all must report OK so the
	// health step's Done is satisfied on the first check.
	for i := 0; i < 6; i++ {
		f.Script(cli.FakeResult{Out: "OK"})
	}

	runStepFull(t, f, addonsDir, "testenv", "rbd-mirror", "", []string{"dr1", "dr2"})

	expected := 28
	if len(f.Calls) != expected {
		t.Fatalf("expected %d calls, got %d:\n%s", expected, len(f.Calls), dumpCalls(f))
	}

	// ---- fetch_secret_info(dr1) ----

	// [0]: wait cephblockpools.ceph.rook.io/replicapool --for=jsonpath=site_name on dr1
	assertCallContains(t, "wait-site-name-dr1", f, 0,
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephblockpools.ceph.rook.io/replicapool",
		"--for=jsonpath={.status.mirroringInfo.site_name}",
		"--timeout", "300s",
	)

	// [1]: kubectl get cephblockpools.ceph.rook.io replicapool --output=... (dr1)
	assertCall(t, "get-sitename-dr1", f, 1, "kubectl", []string{
		"--context", "dr1", "-n", "rook-ceph",
		"get", "cephblockpools.ceph.rook.io", "replicapool",
		"--output=jsonpath={.status.mirroringInfo.site_name}",
	})

	// [2]: kubectl get cephblockpools.ceph.rook.io replicapool --output=...peerSecretName (dr1)
	assertCall(t, "get-peer-secret-name-dr1", f, 2, "kubectl", []string{
		"--context", "dr1", "-n", "rook-ceph",
		"get", "cephblockpools.ceph.rook.io", "replicapool",
		"--output=jsonpath={.status.info.rbdMirrorBootstrapPeerSecretName}",
	})

	// [3]: kubectl get secret token-secret-dr1 --output=jsonpath={.data.token} (dr1)
	assertCall(t, "get-token-dr1", f, 3, "kubectl", []string{
		"--context", "dr1", "-n", "rook-ceph",
		"get", "secret", "token-secret-dr1",
		"--output=jsonpath={.data.token}",
	})

	// ---- fetch_secret_info(dr2) ----

	// [4]: wait site_name on dr2
	assertCallContains(t, "wait-site-name-dr2", f, 4,
		"--context", "dr2", "-n", "rook-ceph",
		"wait", "cephblockpools.ceph.rook.io/replicapool",
		"--for=jsonpath={.status.mirroringInfo.site_name}",
	)

	// [5-7]: get calls for dr2 (mirror of [1-3] with dr2)
	assertCall(t, "get-sitename-dr2", f, 5, "kubectl", []string{
		"--context", "dr2", "-n", "rook-ceph",
		"get", "cephblockpools.ceph.rook.io", "replicapool",
		"--output=jsonpath={.status.mirroringInfo.site_name}",
	})
	assertCall(t, "get-peer-secret-name-dr2", f, 6, "kubectl", []string{
		"--context", "dr2", "-n", "rook-ceph",
		"get", "cephblockpools.ceph.rook.io", "replicapool",
		"--output=jsonpath={.status.info.rbdMirrorBootstrapPeerSecretName}",
	})
	assertCall(t, "get-token-dr2", f, 7, "kubectl", []string{
		"--context", "dr2", "-n", "rook-ceph",
		"get", "secret", "token-secret-dr2",
		"--output=jsonpath={.data.token}",
	})

	// ---- configure_mirroring(dr1, c2Info) ----
	// c2Info: name="site-dr2", token="tokendr2b64==", pool=base64("replicapool")

	// [8]: apply stdin rbd-mirror-secret with --namespace=rook-ceph on dr1
	c8 := f.Calls[8]
	if c8.Name != "kubectl" {
		t.Errorf("call[8] name=%q, want kubectl", c8.Name)
	}
	assertArgsContain(t, "apply-secret-dr1", c8.Args,
		"--context", "dr1", "apply", "--filename", "-", "--namespace=rook-ceph",
	)
	// Verify stdin contains the substituted secret content
	if !strings.Contains(c8.Stdin, "site-dr2") {
		t.Errorf("apply-secret-dr1 stdin should contain peer name 'site-dr2'; got:\n%s", c8.Stdin)
	}
	if !strings.Contains(c8.Stdin, "tokendr2b64==") {
		t.Errorf("apply-secret-dr1 stdin should contain token 'tokendr2b64=='")
	}
	poolB64 := base64.StdEncoding.EncodeToString([]byte("replicapool"))
	if !strings.Contains(c8.Stdin, poolB64) {
		t.Errorf("apply-secret-dr1 stdin should contain pool b64 %q", poolB64)
	}

	// [9]: patch cephblockpool/replicapool --type=merge on dr1
	assertCallContains(t, "patch-pool-dr1", f, 9,
		"--context", "dr1", "-n", "rook-ceph",
		"patch", "cephblockpool/replicapool", "--type=merge",
	)
	if !strings.Contains(f.Calls[9].Args[len(f.Calls[9].Args)-1], "site-dr2") {
		t.Errorf("patch call[9] should include peer name 'site-dr2' in patch JSON")
	}

	// [10]: apply stdin vrc-1m on dr1
	c10 := f.Calls[10]
	assertArgsContain(t, "apply-vrc-1m-dr1", c10.Args, "--context", "dr1", "apply", "--filename", "-")
	if !strings.Contains(c10.Stdin, "vrc-1m") {
		t.Errorf("apply-vrc-1m stdin should contain 'vrc-1m'")
	}
	if !strings.Contains(c10.Stdin, "schedulingInterval: 1m") {
		t.Errorf("apply-vrc-1m stdin should contain 'schedulingInterval: 1m'")
	}

	// [11]: apply stdin vrc-5m on dr1
	c11 := f.Calls[11]
	assertArgsContain(t, "apply-vrc-5m-dr1", c11.Args, "--context", "dr1", "apply", "--filename", "-")
	if !strings.Contains(c11.Stdin, "vrc-5m") {
		t.Errorf("apply-vrc-5m stdin should contain 'vrc-5m'")
	}

	// [12]: apply -k rbd_mirror/start-data on dr1
	rbdMirrorDir := filepath.Join(addonsDir, "rook", "rbd_mirror", "start-data")
	assertCall(t, "apply-kustomize-dr1", f, 12, "kubectl", []string{
		"--context", "dr1", "apply", "--kustomize", rbdMirrorDir,
	})

	// ---- configure_mirroring(dr2, c1Info) ----
	// c1Info: name="site-dr1", token="tokendr1b64==", pool=base64("replicapool")

	// [13]: apply stdin rbd-mirror-secret on dr2 (peer info from dr1)
	c13 := f.Calls[13]
	assertArgsContain(t, "apply-secret-dr2", c13.Args,
		"--context", "dr2", "apply", "--filename", "-", "--namespace=rook-ceph",
	)
	if !strings.Contains(c13.Stdin, "site-dr1") {
		t.Errorf("apply-secret-dr2 stdin should contain peer name 'site-dr1'")
	}

	// [14]: patch on dr2
	assertCallContains(t, "patch-pool-dr2", f, 14,
		"--context", "dr2", "-n", "rook-ceph",
		"patch", "cephblockpool/replicapool", "--type=merge",
	)

	// [15]: vrc-1m on dr2
	assertArgsContain(t, "apply-vrc-1m-dr2", f.Calls[15].Args, "--context", "dr2")

	// [16]: vrc-5m on dr2
	assertArgsContain(t, "apply-vrc-5m-dr2", f.Calls[16].Args, "--context", "dr2")

	// [17]: apply -k on dr2
	assertCall(t, "apply-kustomize-dr2", f, 17, "kubectl", []string{
		"--context", "dr2", "apply", "--kustomize", rbdMirrorDir,
	})

	// ---- wait_until_ready(dr1) ----
	// [18]: wait cephrbdmirror/my-rbd-mirror --for=create on dr1
	assertCallContains(t, "wait-rbd-mirror-create-dr1", f, 18,
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephrbdmirror/my-rbd-mirror", "--for=create",
	)

	// [19]: wait cephrbdmirror/my-rbd-mirror --for=jsonpath=Ready on dr1
	assertCallContains(t, "wait-rbd-mirror-ready-dr1", f, 19,
		"--context", "dr1", "-n", "rook-ceph",
		"wait", "cephrbdmirror/my-rbd-mirror", "--for=jsonpath={.status.phase}=Ready",
	)

	// ---- wait_until_ready(dr2) ----
	// [20]: wait create on dr2
	assertCallContains(t, "wait-rbd-mirror-create-dr2", f, 20,
		"--context", "dr2", "-n", "rook-ceph",
		"wait", "cephrbdmirror/my-rbd-mirror", "--for=create",
	)

	// [21]: wait ready on dr2
	assertCallContains(t, "wait-rbd-mirror-ready-dr2", f, 21,
		"--context", "dr2",
	)

	// ---- wait_until_pool_mirroring_is_healthy ----
	// Each cluster polls the three summary health fields; all must be OK.
	for i, field := range []string{"daemon_health", "health", "image_health"} {
		assertCall(t, "get-mirroring-"+field+"-dr1", f, 22+i, "kubectl", []string{
			"--context", "dr1", "-n", "rook-ceph",
			"get", "cephblockpools.ceph.rook.io/replicapool",
			"--output=jsonpath={.status.mirroringStatus.summary." + field + "}",
		})
		assertCall(t, "get-mirroring-"+field+"-dr2", f, 25+i, "kubectl", []string{
			"--context", "dr2", "-n", "rook-ceph",
			"get", "cephblockpools.ceph.rook.io/replicapool",
			"--output=jsonpath={.status.mirroringStatus.summary." + field + "}",
		})
	}
}

// rookAddonsDir returns the real test/drenv/addons directory for template-based
// tests. Go tests run with cwd = the package directory (internal/addon), so
// walking 3 levels up reaches test/ and then into drenv/addons.
func rookAddonsDir(t *testing.T) string {
	t.Helper()
	// internal/addon → ../../.. → test/ → drenv/addons
	return "../../../drenv/addons"
}
