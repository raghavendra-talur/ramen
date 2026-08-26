// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// kindClusterName namespaces the world's cluster names on the shared
// container host. kind names are host-global, so only one kind-backed world
// can run at a time; startKindCluster deletes leftovers from a previous
// (crashed) run before creating.
func kindClusterName(name string) string { return "simtest-" + name }

// kindEnv returns the environment for kind invocations, selecting the
// podman provider when docker is absent (kind's docker detection is not
// automatic for podman).
func kindEnv() []string {
	env := os.Environ()

	if _, err := exec.LookPath("docker"); err != nil {
		if _, err := exec.LookPath("podman"); err == nil {
			env = append(env, "KIND_EXPERIMENTAL_PROVIDER=podman")
		}
	}

	return env
}

func kindRun(args ...string) error {
	cmd := exec.Command("kind", args...)
	cmd.Env = kindEnv()

	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("kind %v: %w\n%s", args, err, out)
	}

	return nil
}

// startKindCluster boots one kind cluster and installs the same CRD set the
// envtest backend uses. The kubeconfig is written next to the envtest ones,
// so managers, actors, and artifacts work identically.
func startKindCluster(name, dir string) (*Cluster, error) {
	if _, err := exec.LookPath("kind"); err != nil {
		return nil, fmt.Errorf("SIMTEST_BACKEND=kind requires the kind binary on PATH "+
			"(and a running docker or podman runtime): %w", err)
	}

	kindName := kindClusterName(name)
	kcPath := filepath.Join(dir, name+".kubeconfig")

	// A leftover cluster from a crashed run would make create fail; delete
	// is a no-op when none exists.
	_ = kindRun("delete", "cluster", "--name", kindName)

	if err := kindRun("create", "cluster", "--name", kindName,
		"--kubeconfig", kcPath, "--wait", "120s"); err != nil {
		return nil, fmt.Errorf("cluster %s: %w (is the container runtime running? e.g. podman machine start)", name, err)
	}

	cfg, err := clientcmd.BuildConfigFromFlags("", kcPath)
	if err != nil {
		_ = kindRun("delete", "cluster", "--name", kindName)

		return nil, fmt.Errorf("cluster %s: load kubeconfig: %w", name, err)
	}

	crdPaths, err := hackTestCRDPaths()
	if err != nil {
		_ = kindRun("delete", "cluster", "--name", kindName)

		return nil, fmt.Errorf("cluster %s: %w", name, err)
	}

	_, err = envtest.InstallCRDs(cfg, envtest.CRDInstallOptions{
		Paths: append([]string{filepath.Join(RepoRoot(), "config", "crd", "bases")}, crdPaths...),
	})
	if err != nil {
		_ = kindRun("delete", "cluster", "--name", kindName)

		return nil, fmt.Errorf("cluster %s: install CRDs: %w", name, err)
	}

	cl, err := client.New(cfg, client.Options{Scheme: NewScheme()})
	if err != nil {
		_ = kindRun("delete", "cluster", "--name", kindName)

		return nil, err
	}

	return &Cluster{Name: name, Cfg: cfg, Client: cl, KubeconfigPath: kcPath, kindName: kindName}, nil
}
