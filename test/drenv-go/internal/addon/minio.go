// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon

import (
	"context"
	"fmt"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

// MinioServiceURL returns the HTTP URL for the MinIO service running on a
// given cluster. It resolves the URL by querying:
//  1. The pod hostIP via `-n minio --selector=component=minio jsonpath={.items[0].status.hostIP}`
//  2. The service nodePort via `service/minio jsonpath={.spec.ports[0].nodePort}`
//
// The returned URL is of the form `http://<hostIP>:<nodePort>`.
func MinioServiceURL(ctx context.Context, k *cli.Kubectl, kubeContext string) (string, error) {
	hostIP, err := k.Get(ctx, kubeContext, "minio",
		"pod", "--selector=component=minio",
		"--output=jsonpath={.items[0].status.hostIP}",
	)
	if err != nil {
		return "", fmt.Errorf("minio: get pod hostIP on %s: %w", kubeContext, err)
	}
	if hostIP == "" {
		return "", fmt.Errorf("minio: pod hostIP is empty on %s (pod not ready?)", kubeContext)
	}

	nodePort, err := k.GetJSONPath(ctx, kubeContext, "minio",
		"service/minio",
		"{.spec.ports[0].nodePort}",
	)
	if err != nil {
		return "", fmt.Errorf("minio: get service nodePort on %s: %w", kubeContext, err)
	}
	if nodePort == "" {
		return "", fmt.Errorf("minio: service nodePort is empty on %s", kubeContext)
	}

	return fmt.Sprintf("http://%s:%s", hostIP, nodePort), nil
}
