// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rook-pool creates RBD pools, storage classes, and snapshot class, then
// waits until the primary pool is ready and has mirroring peer token,
// mirroring the Python addons/rook/pool/__init__.py start() function.
//
// Template variable names (from the .yaml files):
//   storage-class.yaml : $name, $cluster, $pool
//   replica-pool.yaml  : $name  (also has namespace hardcoded)
//   snapshot-class.yaml: $scname, $cluster
//
// Steps (serial):
//  1. For each storage class (rook-ceph-block/replicapool, rook-ceph-block-2/replicapool-2):
//     ApplyTemplate storage-class.yaml + ApplyStdin
//  2. For each pool (replicapool, replicapool-2):
//     ApplyTemplate replica-pool.yaml + ApplyStdin
//  3. ApplyTemplate snapshot-class.yaml + ApplyStdin
//  4. kubectl wait cephblockpool/replicapool --for=create -n rook-ceph (300s)
//  5. kubectl wait cephblockpool/replicapool --for=jsonpath={.status.phase}=Ready -n rook-ceph (300s)
//  6. kubectl wait cephblockpool/replicapool --for=jsonpath=...peerSecretName=... -n rook-ceph (300s)

package addon

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// rookPoolStorageClasses defines the (storageClass, pool) pairs, matching
// the storage_classes list in addons/rook/pool/__init__.py.
var rookPoolStorageClasses = []struct {
	name string
	pool string
}{
	{"rook-ceph-block", "replicapool"},
	{"rook-ceph-block-2", "replicapool-2"},
}

// rookPoolNames mirrors POOL_NAMES in addons/rook/pool/__init__.py.
var rookPoolNames = []string{"replicapool", "replicapool-2"}

func init() {
	Register("rook-pool", buildRookPool)
}

func buildRookPool(d Deps, cluster string, _ []string) ensure.Step {
	poolDir := "rook/pool"

	var steps []ensure.Step

	// Step 1: create storage classes (2 total)
	for _, sc := range rookPoolStorageClasses {
		sc := sc // capture
		steps = append(steps,
			newApplyStep(fmt.Sprintf("apply-storageclass-%s", sc.name), func(ctx context.Context) error {
				manifest, err := ApplyTemplate(d, filepath.Join(poolDir, "storage-class.yaml"), map[string]string{
					"cluster": cluster,
					"name":    sc.name,
					"pool":    sc.pool,
				})
				if err != nil {
					return err
				}
				return d.K.ApplyStdin(ctx, cluster, manifest)
			}),
		)
	}

	// Step 2: create RBD pools (2 total)
	for _, pool := range rookPoolNames {
		pool := pool // capture
		steps = append(steps,
			newApplyStep(fmt.Sprintf("apply-pool-%s", pool), func(ctx context.Context) error {
				manifest, err := ApplyTemplate(d, filepath.Join(poolDir, "replica-pool.yaml"), map[string]string{
					"cluster": cluster,
					"name":    pool,
				})
				if err != nil {
					return err
				}
				return d.K.ApplyStdin(ctx, cluster, manifest)
			}),
		)
	}

	// Step 3: create snapshot class
	steps = append(steps,
		newApplyStep("apply-snapshot-class", func(ctx context.Context) error {
			manifest, err := ApplyTemplate(d, filepath.Join(poolDir, "snapshot-class.yaml"), map[string]string{
				"cluster": cluster,
				"scname":  "rook-ceph-block",
			})
			if err != nil {
				return err
			}
			return d.K.ApplyStdin(ctx, cluster, manifest)
		}),
	)

	// Step 4: wait for cephblockpool/replicapool to be created
	steps = append(steps,
		newApplyStep("wait-replicapool-create", func(ctx context.Context) error {
			return d.K.WaitFor(ctx, cluster, "rook-ceph", "create",
				rookDefaultWaitTimeout, "cephblockpool/replicapool")
		}),
	)

	// Step 5: wait for cephblockpool/replicapool to be Ready
	steps = append(steps,
		newApplyStep("wait-replicapool-ready", func(ctx context.Context) error {
			return d.K.WaitFor(ctx, cluster, "rook-ceph",
				"jsonpath={.status.phase}=Ready",
				rookDefaultWaitTimeout, "cephblockpool/replicapool")
		}),
	)

	// Step 6: wait for peer token
	steps = append(steps,
		newApplyStep("wait-replicapool-peer-token", func(ctx context.Context) error {
			return d.K.WaitFor(ctx, cluster, "rook-ceph",
				"jsonpath={.status.info.rbdMirrorBootstrapPeerSecretName}=pool-peer-token-replicapool",
				rookDefaultWaitTimeout, "cephblockpool/replicapool")
		}),
	)

	return Serial("addon/rook-pool", d.Opts, steps...)
}
