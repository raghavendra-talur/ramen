// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

type Cluster struct {
	Name           string
	Env            *envtest.Environment
	Cfg            *rest.Config
	Client         client.Client
	KubeconfigPath string
}

// StartCluster boots one envtest control plane with all ramen + third-party
// CRDs and writes an admin kubeconfig into dir.
func StartCluster(name, dir string) (*Cluster, error) {
	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join(RepoRoot(), "config", "crd", "bases"),
			filepath.Join(RepoRoot(), "hack", "test"),
		},
		ErrorIfCRDPathMissing: true,
	}

	cfg, err := env.Start()
	if err != nil {
		return nil, fmt.Errorf("cluster %s: envtest start: %w", name, err)
	}

	user, err := env.AddUser(envtest.User{Name: "simtest-admin", Groups: []string{"system:masters"}}, nil)
	if err != nil {
		_ = env.Stop()

		return nil, fmt.Errorf("cluster %s: add user: %w", name, err)
	}

	kc, err := user.KubeConfig()
	if err != nil {
		_ = env.Stop()

		return nil, fmt.Errorf("cluster %s: kubeconfig: %w", name, err)
	}

	kcPath := filepath.Join(dir, name+".kubeconfig")
	if err := os.WriteFile(kcPath, kc, 0o600); err != nil {
		_ = env.Stop()

		return nil, err
	}

	cl, err := client.New(cfg, client.Options{Scheme: NewScheme()})
	if err != nil {
		_ = env.Stop()

		return nil, err
	}

	return &Cluster{Name: name, Env: env, Cfg: cfg, Client: cl, KubeconfigPath: kcPath}, nil
}

func (c *Cluster) Stop() error { return c.Env.Stop() }

func isNotFound(err error) bool { return errors.IsNotFound(err) }
