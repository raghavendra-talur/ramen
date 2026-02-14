// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package util

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const (
	// KubeVirt CRD names
	VirtualMachineCRDName         = "virtualmachines.kubevirt.io"
	VirtualMachineInstanceCRDName = "virtualmachineinstances.kubevirt.io"

	// Velero deployment name
	VeleroDeploymentName = "velero"

	// KubeVirt Velero plugin image identifier
	KubeVirtVeleroPluginImageSubstring = "kubevirt-velero-plugin"
)

// KubeVirtValidationResult contains the result of KubeVirt environment validation
type KubeVirtValidationResult struct {
	// KubeVirtInstalled indicates if KubeVirt CRDs are present in the cluster
	KubeVirtInstalled bool
	// VeleroPluginInstalled indicates if the kubevirt-velero-plugin is configured in Velero
	VeleroPluginInstalled bool
	// ValidationError contains any error encountered during validation
	ValidationError error
	// Message provides additional context about the validation result
	Message string
}

// IsValid returns true if KubeVirt is not installed (no validation needed) or
// if KubeVirt is installed and the Velero plugin is properly configured
func (r *KubeVirtValidationResult) IsValid() bool {
	if r.ValidationError != nil {
		return false
	}

	// If KubeVirt is not installed, no validation needed
	if !r.KubeVirtInstalled {
		return true
	}

	// If KubeVirt is installed, the plugin must also be installed
	return r.VeleroPluginInstalled
}

// ValidateKubeVirtVeleroPlugin checks if KubeVirt CRDs are installed and if so,
// validates that the kubevirt-velero-plugin is properly configured in Velero.
// This validation ensures that VM resources can be properly protected during
// backup and restore operations.
func ValidateKubeVirtVeleroPlugin(
	ctx context.Context,
	k8sClient client.Client,
	apiReader client.Reader,
	veleroNamespace string,
	log logr.Logger,
) KubeVirtValidationResult {
	result := KubeVirtValidationResult{}

	// Step 1: Check if KubeVirt CRDs are installed
	kubeVirtInstalled, err := isKubeVirtCRDInstalled(ctx, apiReader, log)
	if err != nil {
		result.ValidationError = fmt.Errorf("failed to check KubeVirt CRD installation: %w", err)
		result.Message = "Unable to determine if KubeVirt is installed"
		log.Error(err, "Failed to check KubeVirt CRD installation")

		return result
	}

	result.KubeVirtInstalled = kubeVirtInstalled

	// If KubeVirt is not installed, no further validation needed
	if !kubeVirtInstalled {
		result.Message = "KubeVirt CRDs not found - no VM protection validation required"
		log.Info("KubeVirt CRDs not installed, skipping velero plugin validation")

		return result
	}

	log.Info("KubeVirt CRDs detected, validating kubevirt-velero-plugin configuration")

	// Step 2: Check if kubevirt-velero-plugin is installed in Velero
	pluginInstalled, err := isKubeVirtVeleroPluginInstalled(ctx, apiReader, veleroNamespace, log)
	if err != nil {
		result.ValidationError = fmt.Errorf("failed to check kubevirt-velero-plugin installation: %w", err)
		result.Message = "Unable to determine if kubevirt-velero-plugin is installed"
		log.Error(err, "Failed to check kubevirt-velero-plugin installation")

		return result
	}

	result.VeleroPluginInstalled = pluginInstalled

	if pluginInstalled {
		result.Message = "KubeVirt and kubevirt-velero-plugin are properly configured for VM protection"
		log.Info("kubevirt-velero-plugin validation successful")
	} else {
		result.Message = "KubeVirt is installed but kubevirt-velero-plugin is not configured in Velero. " +
			"VM backup and restore operations may fail. " +
			"Please install the kubevirt-velero-plugin to enable VM resource protection."
		log.Info("WARNING: KubeVirt detected but kubevirt-velero-plugin not found in Velero deployment",
			"veleroNamespace", veleroNamespace)
	}

	return result
}

// isKubeVirtCRDInstalled checks if the VirtualMachine CRD is installed in the cluster
func isKubeVirtCRDInstalled(ctx context.Context, apiReader client.Reader, log logr.Logger) (bool, error) {
	crd := &apiextensionsv1.CustomResourceDefinition{}

	err := apiReader.Get(ctx, types.NamespacedName{Name: VirtualMachineCRDName}, crd)
	if err != nil {
		if errors.IsNotFound(err) {
			log.V(1).Info("VirtualMachine CRD not found", "crd", VirtualMachineCRDName)

			return false, nil
		}

		return false, fmt.Errorf("error checking VirtualMachine CRD: %w", err)
	}

	log.V(1).Info("VirtualMachine CRD found", "crd", VirtualMachineCRDName)

	return true, nil
}

// isKubeVirtVeleroPluginInstalled checks if the kubevirt-velero-plugin is configured
// in the Velero deployment by examining the init containers for the plugin image
func isKubeVirtVeleroPluginInstalled(
	ctx context.Context,
	apiReader client.Reader,
	veleroNamespace string,
	log logr.Logger,
) (bool, error) {
	deployment := &appsv1.Deployment{}

	err := apiReader.Get(ctx, types.NamespacedName{
		Namespace: veleroNamespace,
		Name:      VeleroDeploymentName,
	}, deployment)
	if err != nil {
		if errors.IsNotFound(err) {
			log.Info("Velero deployment not found", "namespace", veleroNamespace)

			return false, nil
		}

		return false, fmt.Errorf("error getting Velero deployment: %w", err)
	}

	// Check init containers for kubevirt-velero-plugin
	for _, initContainer := range deployment.Spec.Template.Spec.InitContainers {
		if strings.Contains(initContainer.Image, KubeVirtVeleroPluginImageSubstring) {
			log.V(1).Info("Found kubevirt-velero-plugin in Velero init containers",
				"container", initContainer.Name,
				"image", initContainer.Image)

			return true, nil
		}
	}

	// Also check regular containers (some deployments might use sidecar pattern)
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if strings.Contains(container.Image, KubeVirtVeleroPluginImageSubstring) {
			log.V(1).Info("Found kubevirt-velero-plugin in Velero containers",
				"container", container.Name,
				"image", container.Image)

			return true, nil
		}
	}

	log.V(1).Info("kubevirt-velero-plugin not found in Velero deployment",
		"namespace", veleroNamespace,
		"initContainers", len(deployment.Spec.Template.Spec.InitContainers))

	return false, nil
}
