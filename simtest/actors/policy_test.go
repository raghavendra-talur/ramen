// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package actors

import (
	"testing"
	"time"
)

func TestPolicyStore(t *testing.T) {
	s := NewStore()
	k := VolRep("dr1")

	if d := s.Decide(k, "pvc-a"); !d.Proceed {
		t.Fatal("default policy must be Normal/proceed")
	}

	s.Set(k, Silent{})
	if d := s.Decide(k, "pvc-a"); d.Proceed || d.RequeueAfter <= 0 {
		t.Fatalf("silent must not proceed and must requeue, got %+v", d)
	}

	s.Set(k, Delayed{After: 80 * time.Millisecond})
	if d := s.Decide(k, "pvc-b"); d.Proceed {
		t.Fatal("delayed must hold before deadline")
	}
	time.Sleep(120 * time.Millisecond)
	if d := s.Decide(k, "pvc-b"); !d.Proceed {
		t.Fatal("delayed must proceed after deadline")
	}

	s.Set(k, FailWith{Mode: "degraded"})
	d := s.Decide(k, "pvc-a")
	if !d.Proceed {
		t.Fatal("failWith proceeds (with failure payload)")
	}
	if fw, ok := d.Policy.(FailWith); !ok || fw.Mode != "degraded" {
		t.Fatalf("decision must carry the policy, got %+v", d.Policy)
	}

	s.Set(k, Normal{})
	if d := s.Decide(k, "pvc-b"); !d.Proceed {
		t.Fatal("reset to normal must proceed")
	}
}

func TestPolicyStoreKeyIsolation(t *testing.T) {
	s := NewStore()
	keyA := VolRep("dr1")
	keyB := Work("dr2")

	// Set Delayed policy on key A with 200ms deadline
	s.Set(keyA, Delayed{After: 200 * time.Millisecond})

	// Start the clock on key A
	if d := s.Decide(keyA, "pvc-1"); d.Proceed {
		t.Fatal("delayed must hold before deadline")
	}

	// Set key B to Normal (non-Delayed) — must NOT reset key A's clock
	s.Set(keyB, Normal{})

	// Sleep past key A's deadline
	time.Sleep(250 * time.Millisecond)

	// Key A's clock should NOT have been reset; it must Proceed
	if d := s.Decide(keyA, "pvc-1"); !d.Proceed {
		t.Fatal("key A's clock must NOT be reset by setting key B to Normal")
	}

	// Verify that setting key A itself to Normal resets its clock
	s.Set(keyA, Delayed{After: 200 * time.Millisecond})
	if d := s.Decide(keyA, "pvc-1"); d.Proceed {
		t.Fatal("after reset, delayed must hold before deadline")
	}
	time.Sleep(250 * time.Millisecond)
	if d := s.Decide(keyA, "pvc-1"); !d.Proceed {
		t.Fatal("after reset and wait, delayed must proceed")
	}
}
