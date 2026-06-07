// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// rook-cephfs creates CephFS filesystems, storage classes, and snapshot class,
// then waits until the filesystems are ready, mirroring the Python
// addons/rook/cephfs/start.py start() function.
//
// Template variable names (from the start-data/*.yaml files):
//   filesystem.yaml   : $name  (cluster also passed but not referenced)
//   storage-class.yaml: $name, $cluster, $fsname
//   snapshot-class.yaml: $scname, $cluster
//
// Steps (serial):
//  1. For each filesystem (fs1, fs2):
//     a. ApplyTemplate filesystem.yaml ($name=<fs>, $cluster=<cluster>) + ApplyStdin
//     b. ApplyTemplate storage-class.yaml ($name=rook-cephfs-<fs>, $cluster=<cluster>,
//        $fsname=<fs>) + ApplyStdin
//  2. ApplyTemplate snapshot-class.yaml ($scname=rook-cephfs-fs1, $cluster=<cluster>) + ApplyStdin
//  3. For each filesystem:
//     a. kubectl wait cephfilesystem/<fs> --for=create -n rook-ceph (300s)
//     b. kubectl wait cephfilesystem/<fs> --for=jsonpath={.status.phase}=Ready -n rook-ceph (300s)

package addon

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

const cephfsStorageClassNamePrefix = "rook-cephfs-"

// cephfsFileSystems mirrors FILE_SYSTEMS in addons/rook/cephfs/start.py.
var cephfsFileSystems = []string{"fs1", "fs2"}

func init() {
	Register("rook-cephfs", buildRookCephFS)
}

func buildRookCephFS(d Deps, cluster string, _ []string) ensure.Step {
	cephfsStartData := "rook/cephfs/start-data"

	var steps []ensure.Step

	// Step 1: for each filesystem, create the filesystem + storage class
	for _, fs := range cephfsFileSystems {
		fs := fs // capture
		scName := cephfsStorageClassNamePrefix + fs

		steps = append(steps,
			// 1a. apply CephFilesystem
			newApplyStep(fmt.Sprintf("apply-filesystem-%s", fs), func(ctx context.Context) error {
				manifest, err := ApplyTemplate(d, filepath.Join(cephfsStartData, "filesystem.yaml"), map[string]string{
					"cluster": cluster,
					"name":    fs,
				})
				if err != nil {
					return err
				}
				return d.K.ApplyStdin(ctx, cluster, manifest)
			}),
			// 1b. apply StorageClass
			newApplyStep(fmt.Sprintf("apply-storageclass-%s", scName), func(ctx context.Context) error {
				manifest, err := ApplyTemplate(d, filepath.Join(cephfsStartData, "storage-class.yaml"), map[string]string{
					"cluster": cluster,
					"name":    scName,
					"fsname":  fs,
				})
				if err != nil {
					return err
				}
				return d.K.ApplyStdin(ctx, cluster, manifest)
			}),
		)
	}

	// Step 2: apply snapshot class (uses the first filesystem's SC name)
	firstSCName := cephfsStorageClassNamePrefix + cephfsFileSystems[0]
	steps = append(steps,
		newApplyStep("apply-snapshot-class", func(ctx context.Context) error {
			manifest, err := ApplyTemplate(d, filepath.Join(cephfsStartData, "snapshot-class.yaml"), map[string]string{
				"cluster": cluster,
				"scname":  firstSCName,
			})
			if err != nil {
				return err
			}
			return d.K.ApplyStdin(ctx, cluster, manifest)
		}),
	)

	// Step 3: wait for each filesystem to be created then Ready
	for _, fs := range cephfsFileSystems {
		fs := fs // capture
		resource := "cephfilesystem/" + fs

		steps = append(steps,
			newApplyStep(fmt.Sprintf("wait-%s-create", resource), func(ctx context.Context) error {
				return d.K.WaitFor(ctx, cluster, "rook-ceph", "create",
					rookDefaultWaitTimeout, resource)
			}),
			newApplyStep(fmt.Sprintf("wait-%s-ready", resource), func(ctx context.Context) error {
				return d.K.WaitFor(ctx, cluster, "rook-ceph",
					"jsonpath={.status.phase}=Ready",
					rookDefaultWaitTimeout, resource)
			}),
		)
	}

	return Serial("addon/rook-cephfs", d.Opts, steps...)
}
