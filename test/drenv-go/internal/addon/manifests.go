// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

import (
	"context"
	"embed"

	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// manifestsFS holds the pre-rendered addon manifests. They are produced at
// build/maintenance time by `make render` (see magefile.go Render), which runs
// `kustomize build` against the addon kustomizations — fetching their upstream
// bases and flattening them into standalone YAML. Embedding the rendered output
// makes runtime apply hermetic: no network access and no addons directory are
// needed for the addons that previously fetched remote bases.
//
//go:embed manifests
var manifestsFS embed.FS

// embeddedManifest returns the bytes of a pre-rendered manifest by file name
// (e.g. "argocd.yaml"), as embedded from the manifests directory.
func embeddedManifest(name string) ([]byte, error) {
	return manifestsFS.ReadFile("manifests/" + name)
}

// applyEmbedded returns a step that applies an embedded manifest to cluster via
// `kubectl apply <args...> --filename -` (manifest on stdin). It replaces the
// runtime `kubectl apply --kustomize <dir>` calls for the addons whose bases are
// rendered at build time. args carries any extra apply flags (e.g. --namespace,
// --server-side=true).
func applyEmbedded(name string, d Deps, cluster, manifestName string, args ...string) ensure.Step {
	return newApplyStep(name, func(ctx context.Context) error {
		manifest, err := embeddedManifest(manifestName)
		if err != nil {
			return err
		}
		return d.K.ApplyStdinArgs(ctx, cluster, manifest, args...)
	})
}
