// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package controllers

import (
	"testing"
	"time"

	"github.com/ramendr/ramen/api/v1alpha1"
	"github.com/ramendr/ramen/controllers/kubeobjects"
	Recipe "github.com/ramendr/recipe/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/google/go-cmp/cmp"
)

const namespaceName = "my-ns"

func setup() (*Recipe.Hook, *Recipe.Group, error) {
	duration, err := time.ParseDuration("30s")
	if err != nil {
		return nil, nil, err
	}

	hook := &Recipe.Hook{
		Namespace: namespaceName,
		Name:      "hook-single",
		Type:      "exec",
		LabelSelector: &metav1.LabelSelector{
			MatchLabels: map[string]string{
				"myapp": "testapp",
			},
		},
		SinglePodOnly: false,
		Ops: []*Recipe.Operation{
			{
				Name:      "checkpoint",
				Container: "main",
				Timeout:   &metav1.Duration{Duration: duration},
				Command:   []string{"bash", "/scripts/checkpoint.sh"},
			},
		},
		Chks:      []*Recipe.Check{},
		Essential: new(bool),
	}

	group := &Recipe.Group{
		Name:                  "test-group",
		BackupRef:             "test-backup-ref",
		Type:                  "resource",
		IncludedNamespaces:    []string{namespaceName},
		IncludedResourceTypes: []string{"deployment", "replicaset"},
		ExcludedResourceTypes: nil,
		LabelSelector: &metav1.LabelSelector{
			MatchExpressions: []metav1.LabelSelectorRequirement{
				{
					Key:      "test",
					Operator: metav1.LabelSelectorOpNotIn,
					Values:   []string{"empty-on-backup notin", "ignore-on-backup"},
				},
			},
		},
	}

	return hook, group, nil
}

func TestVRG_KubeObjectProtection(t *testing.T) {
	hook, group, err := setup()
	if err != nil {
		t.Fatalf("Setup failed: %v", err)
	}

	t.Run("Conversion", func(t *testing.T) {
		t.Run("Hook to CaptureSpec", func(t *testing.T) {
			targetCaptureSpec := &kubeobjects.CaptureSpec{
				Name: hook.Name + "-" + hook.Ops[0].Name,
				Spec: kubeobjects.Spec{
					KubeResourcesSpec: kubeobjects.KubeResourcesSpec{
						IncludedNamespaces: []string{namespaceName},
						IncludedResources:  []string{"pod"},
						ExcludedResources:  []string{},
						Hooks: []kubeobjects.HookSpec{
							{
								Name:          hook.Ops[0].Name,
								Type:          hook.Type,
								Command:       hook.Ops[0].Command,
								Timeout:       hook.Ops[0].Timeout,
								Container:     &hook.Ops[0].Container,
								LabelSelector: hook.LabelSelector,
							},
						},
					},
					LabelSelector:           hook.LabelSelector,
					IncludeClusterResources: new(bool),
				},
			}
			converted, err := convertRecipeHookToCaptureSpec(*hook, *hook.Ops[0])
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if diff := cmp.Diff(targetCaptureSpec, converted); diff != "" {
				t.Errorf("Hook to CaptureSpec mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("Hook to RecoverSpec", func(t *testing.T) {
			targetRecoverSpec := &kubeobjects.RecoverSpec{
				BackupName: v1alpha1.ReservedBackupName,
				Spec: kubeobjects.Spec{
					KubeResourcesSpec: kubeobjects.KubeResourcesSpec{
						IncludedNamespaces: []string{namespaceName},
						IncludedResources:  []string{"pod"},
						ExcludedResources:  []string{},
						Hooks: []kubeobjects.HookSpec{
							{
								Name:          hook.Ops[0].Name,
								Type:          hook.Type,
								Command:       hook.Ops[0].Command,
								Timeout:       hook.Ops[0].Timeout,
								Container:     &hook.Ops[0].Container,
								LabelSelector: hook.LabelSelector,
							},
						},
					},
					LabelSelector:           hook.LabelSelector,
					IncludeClusterResources: new(bool),
				},
			}
			converted, err := convertRecipeHookToRecoverSpec(*hook, *hook.Ops[0])
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if diff := cmp.Diff(targetRecoverSpec, converted); diff != "" {
				t.Errorf("Hook to RecoverSpec mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("Group to CaptureSpec", func(t *testing.T) {
			targetCaptureSpec := &kubeobjects.CaptureSpec{
				Name: group.Name,
				Spec: kubeobjects.Spec{
					KubeResourcesSpec: kubeobjects.KubeResourcesSpec{
						IncludedNamespaces: group.IncludedNamespaces,
						IncludedResources:  group.IncludedResourceTypes,
						ExcludedResources:  group.ExcludedResourceTypes,
					},
					LabelSelector:           group.LabelSelector,
					IncludeClusterResources: group.IncludeClusterResources,
					OrLabelSelectors:        []*metav1.LabelSelector{},
				},
			}
			converted, err := convertRecipeGroupToCaptureSpec(*group)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if diff := cmp.Diff(targetCaptureSpec, converted); diff != "" {
				t.Errorf("Group to CaptureSpec mismatch (-want +got):\n%s", diff)
			}
		})

		t.Run("Group to RecoverSpec", func(t *testing.T) {
			targetRecoverSpec := &kubeobjects.RecoverSpec{
				BackupName: group.BackupRef,
				Spec: kubeobjects.Spec{
					KubeResourcesSpec: kubeobjects.KubeResourcesSpec{
						IncludedNamespaces: group.IncludedNamespaces,
						IncludedResources:  group.IncludedResourceTypes,
						ExcludedResources:  group.ExcludedResourceTypes,
					},
					LabelSelector:           group.LabelSelector,
					IncludeClusterResources: group.IncludeClusterResources,
					OrLabelSelectors:        []*metav1.LabelSelector{},
				},
			}
			converted, err := convertRecipeGroupToRecoverSpec(*group)
			if err != nil {
				t.Fatalf("Expected no error, got %v", err)
			}
			if diff := cmp.Diff(targetRecoverSpec, converted); diff != "" {
				t.Errorf("Group to RecoverSpec mismatch (-want +got):\n%s", diff)
			}
		})
	})
}