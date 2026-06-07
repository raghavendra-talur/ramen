// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package addon provides the addon execution framework for drenv-go. An addon
// is a named, idempotent unit of cluster configuration that produces an
// ensure.Step when instantiated for a specific cluster and argument list.
//
// Addons register themselves with Register (typically via init() in their own
// file). The build layer looks them up via Lookup to compose per-worker step
// trees.
package addon

import (
	"fmt"
	"sync"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

// Deps holds the runtime dependencies available to all addon builders.
type Deps struct {
	// K is the kubectl wrapper for issuing Kubernetes commands.
	K *cli.Kubectl
	// MK is the minikube wrapper (rarely needed by addons).
	MK *cli.Minikube
	// Helm is the helm wrapper for chart installation.
	Helm *cli.Helm
	// Clusteradm is the clusteradm wrapper for OCM operations.
	Clusteradm *cli.Clusteradm
	// Subctl is the subctl wrapper for Submariner operations.
	Subctl *cli.Subctl
	// MC is the mc wrapper for MinIO client operations.
	MC *cli.MC
	// Velero is the velero wrapper for Velero installation.
	Velero *cli.Velero
	// Argocd is the argocd wrapper for ArgoCD CLI operations.
	Argocd *cli.Argocd
	// AddonsDir is the absolute path to the test/drenv/addons directory
	// containing the addon manifests and kustomization files.
	AddonsDir string
	// EnvName is the environment name (from the envfile). Used by addons that
	// need to persist files under ~/.config/drenv/<EnvName>/ (e.g. submariner
	// broker-info, argocd kubeconfig).
	EnvName string
	// Opts is the ensure.Options to use for all addon steps.
	Opts ensure.Options
}

// Builder is a factory function that returns the ensure.Step for an addon
// on a specific cluster with the given args. Each registered addon provides
// one Builder.
type Builder func(d Deps, cluster string, args []string) ensure.Step

// registry is the global addon registry.
var registry struct {
	mu       sync.RWMutex
	builders map[string]Builder
}

// Register registers a Builder under the given name. It panics if the same
// name is registered more than once (indicating a programming error).
func Register(name string, b Builder) {
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if registry.builders == nil {
		registry.builders = make(map[string]Builder)
	}
	if _, exists := registry.builders[name]; exists {
		panic(fmt.Sprintf("addon: duplicate registration for %q", name))
	}
	registry.builders[name] = b
}

// Lookup returns the Builder registered under name and whether it was found.
func Lookup(name string) (Builder, bool) {
	registry.mu.RLock()
	defer registry.mu.RUnlock()
	b, ok := registry.builders[name]
	return b, ok
}
