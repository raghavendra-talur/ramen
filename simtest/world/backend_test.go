// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package world

import "testing"

// The world backend is selected by SIMTEST_BACKEND: envtest (default,
// fast, no controllers beyond the apiserver) or kind (real
// kube-controller-manager — no janitor crutch).
func TestBackendSelection(t *testing.T) {
	cases := []struct {
		env     string
		want    BackendKind
		wantErr bool
	}{
		{env: "", want: BackendEnvtest},
		{env: "envtest", want: BackendEnvtest},
		{env: "kind", want: BackendKind_},
		{env: "minikube", wantErr: true},
	}

	for _, c := range cases {
		t.Setenv("SIMTEST_BACKEND", c.env)

		got, err := Backend()
		if c.wantErr {
			if err == nil {
				t.Fatalf("env %q: expected error", c.env)
			}

			continue
		}

		if err != nil || got != c.want {
			t.Fatalf("env %q: got %v, %v", c.env, got, err)
		}
	}
}
