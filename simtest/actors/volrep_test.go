// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"testing"

	volrep "github.com/csi-addons/kubernetes-csi-addons/api/replication.storage/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func newVR(state volrep.ReplicationState) *volrep.VolumeReplication {
	vr := &volrep.VolumeReplication{}
	vr.Name, vr.Namespace, vr.Generation = "pvc-a", "app-ns", 3
	vr.Spec.ReplicationState = state

	return vr
}

func findCond(conds []metav1.Condition, t string) *metav1.Condition {
	for i := range conds {
		if conds[i].Type == t {
			return &conds[i]
		}
	}

	return nil
}

func TestFulfillVolumeReplicationPrimary(t *testing.T) {
	st := fulfillVolumeReplication(newVR(volrep.Primary), Normal{})

	if st.State != volrep.PrimaryState || st.ObservedGeneration != 3 {
		t.Fatalf("bad state: %+v", st)
	}
	if st.LastSyncTime == nil || st.DestinationVolumeID == "" {
		t.Fatal("primary needs LastSyncTime and DestinationVolumeID")
	}
	for typ, want := range map[string]metav1.ConditionStatus{
		volrep.ConditionValidated: metav1.ConditionTrue,
		volrep.ConditionCompleted: metav1.ConditionTrue,
		volrep.ConditionDegraded:  metav1.ConditionFalse,
		volrep.ConditionResyncing: metav1.ConditionFalse,
	} {
		c := findCond(st.Conditions, typ)
		if c == nil || c.Status != want || c.ObservedGeneration != 3 {
			t.Fatalf("condition %s: got %+v want %s", typ, c, want)
		}
	}
}

func TestFulfillVolumeReplicationSecondary(t *testing.T) {
	st := fulfillVolumeReplication(newVR(volrep.Secondary), Normal{})

	if st.State != volrep.SecondaryState || st.DestinationVolumeID != "" {
		t.Fatalf("bad secondary status: %+v", st)
	}
	// VRG requires Completed=True, Degraded=False, Resyncing=False, State=Secondary
	// to consider a secondary protected (vrg_volrep.go checkResyncCompletionAsSecondary).
	if c := findCond(st.Conditions, volrep.ConditionCompleted); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatal("secondary Completed must be True")
	}
}

func TestFulfillVolumeReplicationFailureModes(t *testing.T) {
	st := fulfillVolumeReplication(newVR(volrep.Primary), FailWith{Mode: "validated-false"})
	if c := findCond(st.Conditions, volrep.ConditionValidated); c == nil || c.Status != metav1.ConditionFalse {
		t.Fatal("validated-false mode must set Validated=False")
	}

	st = fulfillVolumeReplication(newVR(volrep.Secondary), FailWith{Mode: "degraded"})
	if c := findCond(st.Conditions, volrep.ConditionDegraded); c == nil || c.Status != metav1.ConditionTrue {
		t.Fatal("degraded mode must set Degraded=True")
	}
}
