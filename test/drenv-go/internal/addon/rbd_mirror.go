// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rbd-mirror sets up RBD mirroring between two clusters, mirroring the Python
// addons/rook/rbd_mirror/start.py start() function.
//
// This is a GLOBAL addon (cluster="" in the registry call) that takes two
// cluster names as args: args[0]=cluster1, args[1]=cluster2.
//
// Template variable names (from start-data/*.yaml files):
//   rbd-mirror-secret.yaml: $name, $token, $pool
//   vrc.yaml              : $cluster, $scname, $interval
//
// High-level flow (serial steps):
//  1. fetch_secret_info(cluster1): wait for site_name; get site_name; get peer secret name; get token
//  2. fetch_secret_info(cluster2): same
//  3. configure_mirroring(cluster1, cluster2_info):
//     a. ApplyTemplate rbd-mirror-secret.yaml + ApplyStdinNamespace (ns rook-ceph)
//     b. Patch cephblockpool/replicapool (merge) with peer secretNames
//     c. For each VRC interval (1m, 5m): ApplyTemplate vrc.yaml + ApplyStdin
//     d. apply -k <AddonsDir>/rook/rbd_mirror/start-data
//  4. configure_mirroring(cluster2, cluster1_info): same as above
//  5. wait_until_ready(cluster1): wait --for=create; wait --for=jsonpath=phase=Ready (cephrbdmirror)
//  6. wait_until_ready(cluster2): same
//  7. wait_until_pool_mirroring_is_healthy(cluster1): get pool mirroring status
//  8. wait_until_pool_mirroring_is_healthy(cluster2): same
//
// Simplifications vs Python:
//   - fetch_secret_info: The Python calls kubectl.wait then kubectl.get to fetch
//     site_name/secret_name/token. We combine into a wait + three Get calls,
//     matching argv exactly.
//   - wait_until_pool_mirroring_is_healthy: The Python watches the pool status
//     with a streaming kubectl watch (kubectl.watch) and retries up to 3 times,
//     restarting the rbd-mirror daemon on timeout. Here we use a single kubectl
//     get (non-streaming) for argv faithfulness; the streaming watch retry loop
//     needs real-cluster validation.
//
// NEEDS REAL-CLUSTER VALIDATION: cross-cluster secret exchange and mirroring
// health check.

package addon

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"time"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const (
	rbdMirrorPoolName = "replicapool"
	// rbdMirrorPoolNameB64 is the base64-encoded pool name stored in secrets.
	// Python: base64.b64encode(POOL_NAME.encode()).decode()
	rbdMirrorPoolNameB64 = "" // computed at runtime — see fetchRBDSecretInfo

	rbdMirrorReadyTimeout = rookDefaultWaitTimeout // 300s
)

// vrcIntervals lists the VolumeReplicationClass scheduling intervals to create,
// matching VRC_INTERVALS in start.py.
var vrcIntervals = []string{"1m", "5m"}

// rbdPeerInfo holds the information fetched from a cluster needed to configure
// its peer on the other cluster.
type rbdPeerInfo struct {
	// name is the peer site name (from .status.mirroringInfo.site_name).
	name string
	// token is the base64-encoded rbd mirror bootstrap peer token (from secret .data.token).
	token string
	// pool is the base64-encoded pool name ("replicapool").
	pool string
}

func init() {
	Register("rbd-mirror", buildRBDMirror)
}

func buildRBDMirror(d Deps, _ string, args []string) ensure.Step {
	// rbd-mirror is a global addon: cluster="" and args=[cluster1, cluster2].
	cluster1 := args[0]
	cluster2 := args[1]

	rbdMirrorStartData := filepath.Join(d.AddonsDir, "rook", "rbd_mirror", "start-data")

	// Mutable peer info filled in during Do() of fetch steps.
	var c1Info, c2Info rbdPeerInfo

	// ---- fetch_secret_info(cluster1) ----
	fetchC1 := newApplyStep("fetch-secret-info/"+cluster1, func(ctx context.Context) error {
		info, err := fetchRBDSecretInfo(ctx, d, cluster1)
		if err != nil {
			return err
		}
		c1Info = info
		return nil
	})

	// ---- fetch_secret_info(cluster2) ----
	fetchC2 := newApplyStep("fetch-secret-info/"+cluster2, func(ctx context.Context) error {
		info, err := fetchRBDSecretInfo(ctx, d, cluster2)
		if err != nil {
			return err
		}
		c2Info = info
		return nil
	})

	// ---- configure_mirroring(cluster1, cluster2_info) ----
	// c2Info is used here (peer info for cluster1 comes from cluster2).
	configureC1 := newApplyStep("configure-mirroring/"+cluster1, func(ctx context.Context) error {
		return configureMirroring(ctx, d, cluster1, c2Info, rbdMirrorStartData)
	})

	// ---- configure_mirroring(cluster2, cluster1_info) ----
	configureC2 := newApplyStep("configure-mirroring/"+cluster2, func(ctx context.Context) error {
		return configureMirroring(ctx, d, cluster2, c1Info, rbdMirrorStartData)
	})

	// ---- wait_until_ready(cluster1) ----
	waitReadyC1 := newApplyStep("wait-rbd-mirror-ready/"+cluster1, func(ctx context.Context) error {
		return waitRBDMirrorReady(ctx, d, cluster1)
	})

	// ---- wait_until_ready(cluster2) ----
	waitReadyC2 := newApplyStep("wait-rbd-mirror-ready/"+cluster2, func(ctx context.Context) error {
		return waitRBDMirrorReady(ctx, d, cluster2)
	})

	// ---- wait_until_pool_mirroring_is_healthy(cluster1/2) ----
	// Do polls the pool's mirroringStatus.summary until daemon_health, health,
	// and image_health are all OK (or the verify timeout elapses), giving the
	// same "block until mirroring is healthy" guarantee as the Python addon.
	// Using newApplyStep means the step latches Done after success, so the
	// group's post-Do verification does not re-query. The Python extra of
	// restarting the rbd-mirror daemon on timeout is not replicated and needs
	// real-cluster validation.
	waitHealthyC1 := newApplyStep("wait-pool-mirroring-healthy/"+cluster1, func(ctx context.Context) error {
		return waitRBDMirroringHealthy(ctx, d, cluster1)
	})
	waitHealthyC2 := newApplyStep("wait-pool-mirroring-healthy/"+cluster2, func(ctx context.Context) error {
		return waitRBDMirroringHealthy(ctx, d, cluster2)
	})

	return Serial("addon/rbd-mirror", d.Opts,
		fetchC1, fetchC2,
		configureC1, configureC2,
		waitReadyC1, waitReadyC2,
		waitHealthyC1, waitHealthyC2,
	)
}

// fetchRBDSecretInfo fetches the peer secret information from a cluster,
// matching fetch_secret_info() in start.py. The Python calls:
//
//	kubectl wait cephblockpools.ceph.rook.io/replicapool
//	  --for=jsonpath={.status.mirroringInfo.site_name} -n rook-ceph (300s)
//	kubectl get cephblockpools.ceph.rook.io replicapool
//	  --output=jsonpath={.status.mirroringInfo.site_name} -n rook-ceph
//	kubectl get cephblockpools.ceph.rook.io replicapool
//	  --output=jsonpath={.status.info.rbdMirrorBootstrapPeerSecretName} -n rook-ceph
//	kubectl get secret <secretName>
//	  --output=jsonpath={.data.token} -n rook-ceph
func fetchRBDSecretInfo(ctx context.Context, d Deps, cluster string) (rbdPeerInfo, error) {
	// Wait for the site_name to appear (non-empty).
	if err := d.K.WaitFor(ctx, cluster, "rook-ceph",
		"jsonpath={.status.mirroringInfo.site_name}",
		rookDefaultWaitTimeout,
		"cephblockpools.ceph.rook.io/"+rbdMirrorPoolName,
	); err != nil {
		return rbdPeerInfo{}, fmt.Errorf("wait mirroringInfo.site_name on %s: %w", cluster, err)
	}

	// Get site_name.
	siteName, err := d.K.Get(ctx, cluster, "rook-ceph",
		"cephblockpools.ceph.rook.io",
		rbdMirrorPoolName,
		"--output=jsonpath={.status.mirroringInfo.site_name}",
	)
	if err != nil {
		return rbdPeerInfo{}, fmt.Errorf("get site_name on %s: %w", cluster, err)
	}

	// Get the bootstrap peer secret name.
	secretName, err := d.K.Get(ctx, cluster, "rook-ceph",
		"cephblockpools.ceph.rook.io",
		rbdMirrorPoolName,
		"--output=jsonpath={.status.info.rbdMirrorBootstrapPeerSecretName}",
	)
	if err != nil {
		return rbdPeerInfo{}, fmt.Errorf("get peer secret name on %s: %w", cluster, err)
	}

	// Get the token from the secret.
	token, err := d.K.Get(ctx, cluster, "rook-ceph",
		"secret",
		secretName,
		"--output=jsonpath={.data.token}",
	)
	if err != nil {
		return rbdPeerInfo{}, fmt.Errorf("get token from secret %s on %s: %w", secretName, cluster, err)
	}

	// Pool name must be base64-encoded for the secret .data section,
	// matching: base64.b64encode(POOL_NAME.encode()).decode() in Python.
	poolB64 := base64.StdEncoding.EncodeToString([]byte(rbdMirrorPoolName))

	return rbdPeerInfo{
		name:  siteName,
		token: token,
		pool:  poolB64,
	}, nil
}

// configureMirroring sets up mirroring on cluster using the peer's info,
// matching configure_mirroring() in start.py.
func configureMirroring(ctx context.Context, d Deps, cluster string, peer rbdPeerInfo, startDataDir string) error {
	// 1. Apply rbd-mirror-secret (with --namespace=rook-ceph flag as in Python).
	secretManifest, err := ApplyTemplate(d, "rook/rbd_mirror/start-data/rbd-mirror-secret.yaml", map[string]string{
		"name":  peer.name,
		"token": peer.token,
		"pool":  peer.pool,
	})
	if err != nil {
		return fmt.Errorf("template rbd-mirror-secret: %w", err)
	}
	if err := d.K.ApplyStdinNamespace(ctx, cluster, "rook-ceph", secretManifest); err != nil {
		return fmt.Errorf("apply rbd-mirror-secret on %s: %w", cluster, err)
	}

	// 2. Patch cephblockpool/replicapool to configure peers.
	patch := map[string]interface{}{
		"spec": map[string]interface{}{
			"mirroring": map[string]interface{}{
				"peers": map[string]interface{}{
					"secretNames": []string{peer.name},
				},
			},
		},
	}
	patchJSON, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal peers patch: %w", err)
	}
	if err := d.K.Patch(ctx, cluster, "rook-ceph",
		"cephblockpool/"+rbdMirrorPoolName,
		"merge",
		string(patchJSON),
	); err != nil {
		return fmt.Errorf("patch cephblockpool on %s: %w", cluster, err)
	}

	// 3. Create VolumeReplicationClass for each interval.
	for _, interval := range vrcIntervals {
		vrcManifest, err := ApplyTemplate(d, "rook/rbd_mirror/start-data/vrc.yaml", map[string]string{
			"cluster":  cluster,
			"scname":   "rook-ceph-block",
			"interval": interval,
		})
		if err != nil {
			return fmt.Errorf("template vrc.yaml interval=%s: %w", interval, err)
		}
		if err := d.K.ApplyStdin(ctx, cluster, vrcManifest); err != nil {
			return fmt.Errorf("apply vrc-%s on %s: %w", interval, cluster, err)
		}
	}

	// 4. Apply the rbd-mirror kustomize dir (creates CephRBDMirror).
	if err := d.K.ApplyKustomizeDir(ctx, cluster, startDataDir); err != nil {
		return fmt.Errorf("apply rbd-mirror kustomize on %s: %w", cluster, err)
	}

	return nil
}

// waitRBDMirrorReady waits for cephrbdmirror/my-rbd-mirror to exist then be Ready,
// matching wait_until_ready() in start.py.
func waitRBDMirrorReady(ctx context.Context, d Deps, cluster string) error {
	if err := d.K.WaitFor(ctx, cluster, "rook-ceph", "create",
		rbdMirrorReadyTimeout, "cephrbdmirror/my-rbd-mirror",
	); err != nil {
		return fmt.Errorf("wait cephrbdmirror create on %s: %w", cluster, err)
	}
	if err := d.K.WaitFor(ctx, cluster, "rook-ceph",
		"jsonpath={.status.phase}=Ready",
		rbdMirrorReadyTimeout, "cephrbdmirror/my-rbd-mirror",
	); err != nil {
		return fmt.Errorf("wait cephrbdmirror ready on %s: %w", cluster, err)
	}
	return nil
}

// waitRBDMirroringHealthy polls rbdMirroringHealthy until it reports healthy,
// the context is cancelled, or the verify timeout elapses. This mirrors the
// Python wait_until_pool_mirroring_is_healthy loop (minus the daemon restart).
func waitRBDMirroringHealthy(ctx context.Context, d Deps, cluster string) error {
	interval := d.Opts.VerifyInterval
	if interval <= 0 {
		interval = 2 * time.Second
	}
	timeout := d.Opts.VerifyTimeout
	if timeout <= 0 {
		timeout = 5 * time.Minute
	}
	deadline := time.Now().Add(timeout)
	for {
		ok, err := rbdMirroringHealthy(ctx, d, cluster)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for healthy pool mirroring on %s", cluster)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(interval):
		}
	}
}

// rbdMirroringHealthy reports whether the pool's mirroring is healthy on a
// cluster: daemon_health, health, and image_health in
// .status.mirroringStatus.summary must all be "OK". An empty field (status not
// yet populated) reports not-healthy (no error) so the caller keeps polling.
func rbdMirroringHealthy(ctx context.Context, d Deps, cluster string) (bool, error) {
	resource := "cephblockpools.ceph.rook.io/" + rbdMirrorPoolName
	for _, field := range []string{"daemon_health", "health", "image_health"} {
		v, err := d.K.GetJSONPath(ctx, cluster, "rook-ceph", resource,
			"{.status.mirroringStatus.summary."+field+"}")
		if err != nil {
			return false, fmt.Errorf("get pool mirroring %s on %s: %w", field, cluster, err)
		}
		if strings.TrimSpace(v) != "OK" {
			return false, nil
		}
	}
	return true, nil
}
