// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"fmt"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	cfgv1alpha1 "k8s.io/component-base/config/v1alpha1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"
)

// RamenConfigYAML renders the operator config stored in the operator
// ConfigMap. Both operator types get both clusters' S3 profiles: the
// dr-cluster operator uploads to all profiles in VRG.spec.s3Profiles and the
// hub validates DRCluster.spec.s3ProfileName against its own copy.
func RamenConfigYAML(controllerType, s3URL string) (string, error) {
	resourceName := "hub.ramendr.openshift.io"
	if controllerType == "dr-cluster" {
		resourceName = "dr-cluster.ramendr.openshift.io"
	}

	cfg := rmn.RamenConfig{
		TypeMeta: metav1.TypeMeta{APIVersion: "ramendr.openshift.io/v1alpha1", Kind: "RamenConfig"},
		LeaderElection: &cfgv1alpha1.LeaderElectionConfiguration{
			LeaderElect:  ptr.To(false),
			ResourceName: resourceName,
		},
		Metrics:                 rmn.ControllerMetrics{BindAddress: "0"},
		Health:                  rmn.ControllerHealth{HealthProbeBindAddress: "0"},
		RamenControllerType:     rmn.ControllerType(controllerType),
		MaxConcurrentReconciles: 50,
		RamenOpsNamespace:       RamenOpsNS,
		S3StoreProfiles: []rmn.S3StoreProfile{
			s3Profile(DR1Name, s3URL),
			s3Profile(DR2Name, s3URL),
		},
	}
	cfg.VolSync.Disabled = true
	cfg.KubeObjectProtection.Disabled = true
	cfg.DrClusterOperator.DeploymentAutomationEnabled = false
	cfg.DrClusterOperator.S3SecretDistributionEnabled = false

	b, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal ramen config: %w", err)
	}

	return string(b), nil
}

func s3Profile(cluster, s3URL string) rmn.S3StoreProfile {
	return rmn.S3StoreProfile{
		S3ProfileName:        S3Profile(cluster),
		S3Bucket:             S3Bucket(cluster),
		S3CompatibleEndpoint: s3URL,
		S3Region:             "us-east-1",
		S3SecretRef:          corev1.SecretReference{Name: S3SecretName}, // resolved in POD_NAMESPACE
	}
}

// OperatorConfigMap builds the operator ConfigMap (name per controllerType,
// namespace RamenSystemNS) containing the rendered RamenConfig YAML under
// ConfigMapKey, matching what the real ramen binary reads from its
// operator ConfigMap.
func OperatorConfigMap(controllerType, s3URL string) (*corev1.ConfigMap, error) {
	name := HubConfigMapName
	if controllerType == "dr-cluster" {
		name = DRConfigMapName
	}

	y, err := RamenConfigYAML(controllerType, s3URL)
	if err != nil {
		return nil, err
	}

	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: RamenSystemNS},
		Data:       map[string]string{ConfigMapKey: y},
	}, nil
}
