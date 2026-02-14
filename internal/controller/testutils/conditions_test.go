// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package testutils

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ramen "github.com/ramendr/ramen/api/v1alpha1"
)

func TestMatchCondition(t *testing.T) {
	cond := metav1.Condition{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "AllGood",
		Message: "Everything is fine",
	}

	// Test exact match
	err := MatchCondition(cond, ConditionMatcher{
		Type:    "Ready",
		Status:  metav1.ConditionTrue,
		Reason:  "AllGood",
		Message: "Everything is fine",
	})
	if err != nil {
		t.Errorf("expected match, got error: %v", err)
	}

	// Test partial match (wildcards)
	err = MatchCondition(cond, ConditionMatcher{
		Type:   "Ready",
		Status: metav1.ConditionTrue,
	})
	if err != nil {
		t.Errorf("expected partial match, got error: %v", err)
	}

	// Test type mismatch
	err = MatchCondition(cond, ConditionMatcher{
		Type: "NotReady",
	})
	if err == nil {
		t.Error("expected type mismatch error")
	}

	// Test status mismatch
	err = MatchCondition(cond, ConditionMatcher{
		Type:   "Ready",
		Status: metav1.ConditionFalse,
	})
	if err == nil {
		t.Error("expected status mismatch error")
	}
}

func TestFindCondition(t *testing.T) {
	conditions := []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionTrue},
		{Type: "Available", Status: metav1.ConditionFalse},
	}

	// Test finding existing condition
	cond := FindCondition(conditions, "Ready")
	if cond == nil {
		t.Error("expected to find 'Ready' condition")
	}

	if cond.Status != metav1.ConditionTrue {
		t.Errorf("expected status True, got %s", cond.Status)
	}

	// Test finding non-existing condition
	cond = FindCondition(conditions, "Missing")
	if cond != nil {
		t.Error("expected nil for missing condition")
	}
}

func TestCheckDRClusterCondition(t *testing.T) {
	drcluster := &ramen.DRCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "test"},
		Status: ramen.DRClusterStatus{
			Conditions: []metav1.Condition{
				{Type: ramen.DRClusterValidated, Status: metav1.ConditionTrue, Reason: "Valid"},
			},
		},
	}

	// Test match
	err := CheckDRClusterCondition(drcluster, ConditionMatcher{
		Type:   ramen.DRClusterValidated,
		Status: metav1.ConditionTrue,
	})
	if err != nil {
		t.Errorf("expected match, got error: %v", err)
	}

	// Test condition not found
	err = CheckDRClusterCondition(drcluster, ConditionMatcher{
		Type: "NonExistent",
	})
	if err == nil {
		t.Error("expected error for missing condition")
	}
}

func TestEqualStringSlices(t *testing.T) {
	// Test equal slices
	err := EqualStringSlices([]string{"a", "b", "c"}, []string{"a", "b", "c"})
	if err != nil {
		t.Errorf("expected equal, got error: %v", err)
	}

	// Test equal slices (different order)
	err = EqualStringSlices([]string{"a", "b", "c"}, []string{"c", "a", "b"})
	if err != nil {
		t.Errorf("expected equal (different order), got error: %v", err)
	}

	// Test missing element in actual
	err = EqualStringSlices([]string{"a", "b", "c"}, []string{"a", "b"})
	if err == nil {
		t.Error("expected error for missing element")
	}

	// Test extra element in actual
	err = EqualStringSlices([]string{"a", "b"}, []string{"a", "b", "c"})
	if err == nil {
		t.Error("expected error for extra element")
	}

	// Test duplicate in actual
	err = EqualStringSlices([]string{"a", "b"}, []string{"a", "a"})
	if err == nil {
		t.Error("expected error for duplicate")
	}
}
