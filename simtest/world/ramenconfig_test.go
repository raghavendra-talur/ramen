// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import (
	"testing"

	rmn "github.com/ramendr/ramen/api/v1alpha1"
	"sigs.k8s.io/yaml"
)

func TestRamenConfigYAML(t *testing.T) {
	y, err := RamenConfigYAML("dr-hub", "http://127.0.0.1:9999")
	if err != nil {
		t.Fatal(err)
	}

	cfg := rmn.RamenConfig{}
	if err := yaml.Unmarshal([]byte(y), &cfg); err != nil {
		t.Fatalf("generated config does not unmarshal into RamenConfig: %v", err)
	}

	if cfg.LeaderElection == nil || cfg.LeaderElection.LeaderElect == nil || *cfg.LeaderElection.LeaderElect {
		t.Fatal("leader election must be disabled")
	}
	if cfg.Metrics.BindAddress != "0" || cfg.Health.HealthProbeBindAddress != "0" {
		t.Fatal("metrics and health must be disabled")
	}
	if len(cfg.S3StoreProfiles) != 2 {
		t.Fatalf("want 2 s3 profiles, got %d", len(cfg.S3StoreProfiles))
	}
	if cfg.VolSync.Disabled || !cfg.KubeObjectProtection.Disabled {
		t.Fatal("volsync must be enabled (cephfs pvcspec) and kubeObjectProtection disabled on the hub")
	}
	if cfg.DrClusterOperator.DeploymentAutomationEnabled || cfg.DrClusterOperator.S3SecretDistributionEnabled {
		t.Fatal("drClusterOperator automation and secret-distribution must be off")
	}

	dy, err := RamenConfigYAML("dr-cluster", "http://127.0.0.1:9999")
	if err != nil {
		t.Fatal(err)
	}

	dcfg := rmn.RamenConfig{}
	if err := yaml.Unmarshal([]byte(dy), &dcfg); err != nil {
		t.Fatalf("generated dr-cluster config does not unmarshal into RamenConfig: %v", err)
	}
	if dcfg.KubeObjectProtection.Disabled {
		t.Fatal("kubeObjectProtection must stay enabled on dr-cluster so admin-namespace VRGs reconcile")
	}

	cm, err := OperatorConfigMap("dr-cluster", "http://127.0.0.1:9999")
	if err != nil {
		t.Fatal(err)
	}
	if cm.Name != DRConfigMapName || cm.Namespace != RamenSystemNS || cm.Data[ConfigMapKey] == "" {
		t.Fatalf("bad configmap: %+v", cm.ObjectMeta)
	}
}
