// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func TestParseReconcilers(t *testing.T) {
	if got := parseReconcilers(""); got != nil {
		t.Fatalf("empty list: got %v, want nil", got)
	}

	got := parseReconcilers("vrg, pvrgl")
	if len(got) != 2 || !got["vrg"] || !got["pvrgl"] {
		t.Fatalf("got %v, want vrg and pvrgl", got)
	}

	if got["drpc"] {
		t.Fatal("drpc enabled, want disabled")
	}
}
