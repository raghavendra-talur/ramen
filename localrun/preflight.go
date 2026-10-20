// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
)

// The CRD each kind of cluster must have before its managers can start; the
// rest of the CRDs are installed with it.
const (
	hubCRD       = "customresourcedefinitions/drplacementcontrols.ramendr.openshift.io"
	drClusterCRD = "customresourcedefinitions/volumereplicationgroups.ramendr.openshift.io"
)

// getter runs `kubectl get <resource>` against a kubeconfig and returns its
// stderr when it fails.
type getter func(kubeconfig, resource string) (string, error)

func kubectlGet(kubeconfig, resource string) (string, error) {
	var stderr bytes.Buffer

	ctx, cancel := context.WithTimeout(context.Background(), kubectlProbeTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "kubectl", "--kubeconfig="+kubeconfig, //nolint:gosec // our envfile's kubeconfigs
		"get", resource, "--output=name")
	cmd.Stderr = &stderr

	err := cmd.Run()

	if ctx.Err() != nil {
		return "", fmt.Errorf("no answer in %v", kubectlProbeTimeout)
	}

	return stderr.String(), err
}

// clusterCheck is what preflight found on one cluster.
type clusterCheck struct {
	missing     []string
	unreachable string
}

// preflight checks that every cluster has what configure creates and the
// managers need: the ramen namespace and the CRDs. Without them a manager
// starts and dies on its first API call, with an error that does not say
// configure was never run. Clusters are checked in parallel, and every
// problem is reported at once.
func preflight(clusters []cluster, get getter) error {
	checks := make([]clusterCheck, len(clusters))

	var wg sync.WaitGroup

	for i, c := range clusters {
		wg.Add(1)

		go func() {
			defer wg.Done()

			checks[i] = checkCluster(c, get)
		}()
	}

	wg.Wait()

	var missing, unreachable []string

	for i, c := range clusters {
		for _, res := range checks[i].missing {
			missing = append(missing, fmt.Sprintf("[%s] %s", c.name, res))
		}

		if checks[i].unreachable != "" {
			unreachable = append(unreachable, fmt.Sprintf("[%s] %s", c.name, checks[i].unreachable))
		}
	}

	var errs []error

	if len(missing) > 0 {
		errs = append(errs, fmt.Errorf(
			"the clusters are not configured for localrun; run `localrun configure` first. Missing:\n  %s",
			strings.Join(missing, "\n  ")))
	}

	if len(unreachable) > 0 {
		errs = append(errs, fmt.Errorf("cannot check the clusters; are they running?\n  %s",
			strings.Join(unreachable, "\n  ")))
	}

	return errors.Join(errs...)
}

// checkCluster looks for the namespace and the CRD. It stops at the first
// check that cannot be answered: the next would wait as long and say no more.
func checkCluster(c cluster, get getter) clusterCheck {
	var check clusterCheck

	crd := drClusterCRD
	if c.hub {
		crd = hubCRD
	}

	for _, res := range []string{"namespaces/" + podNamespace(c), crd} {
		stderr, err := get(c.kubeconfig, res)

		switch {
		case err == nil:
		case strings.Contains(stderr, "NotFound"):
			check.missing = append(check.missing, res)
		default:
			check.unreachable = fmt.Sprintf("cannot check %s: %s", res, firstLine(stderr, err))

			return check
		}
	}

	return check
}

func firstLine(stderr string, err error) string {
	if line, _, _ := strings.Cut(strings.TrimSpace(stderr), "\n"); line != "" {
		return line
	}

	return err.Error()
}
