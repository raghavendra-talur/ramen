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
	// KubeObjectProtection must stay ENABLED on dr-cluster operators: the VRG
	// reconciler only sets up its velero/recipe watches (veleroCRsAreWatched)
	// when the config enables kube object protection, and every VRG living in
	// an admin namespace (all discovered apps, which is what simtest drives)
	// hard-fails reconciliation when those watches are missing. The velero and
	// recipe CRDs are installed from hack/test, so the watch setup succeeds;
	// no velero controller is needed because per-VRG protection stays off
	// (VRG.Spec.KubeObjectProtection is nil — the DRPCs never set it).
	cfg.KubeObjectProtection.Disabled = controllerType != "dr-cluster"
	cfg.DrClusterOperator.DeploymentAutomationEnabled = false
	cfg.DrClusterOperator.S3SecretDistributionEnabled = false

	b, err := yaml.Marshal(cfg)
	if err != nil {
		return "", fmt.Errorf("marshal ramen config: %w", err)
	}

	// The ramen binary merges this YAML onto its built-in defaults, which set
	// the DrClusterOperator automation flags to true. Those fields carry
	// `omitempty`, so their false values vanish from the marshaled YAML and
	// the defaults would win — the hub would then push an OLM
	// OperatorGroup/Subscription ManifestWork that can never apply on an
	// envtest managed cluster. Re-add them explicitly.
	var m map[string]interface{}
	if err := yaml.Unmarshal(b, &m); err != nil {
		return "", fmt.Errorf("unmarshal ramen config: %w", err)
	}

	m["drClusterOperator"] = map[string]interface{}{
		"deploymentAutomationEnabled": false,
		"s3SecretDistributionEnabled": false,
	}

	b, err = yaml.Marshal(m)
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
