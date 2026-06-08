// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

// Unit tests for the pure parts of the containerd-config step: the deep merge,
// the TOML normalization, and the "already satisfied" decision. The full step
// (minikube cp/ssh + file I/O) needs a real cluster and is covered by manual
// validation, not here.

import (
	"reflect"
	"testing"
)

func TestMergeContainerdConfig(t *testing.T) {
	base := map[string]any{
		"version": int64(2),
		"plugins": map[string]any{
			"io.containerd.cri.v1.images": map[string]any{
				"max_concurrent_downloads": int64(3),
			},
		},
	}
	overlay := map[string]any{
		"plugins": map[string]any{
			"io.containerd.cri.v1.runtime": map[string]any{
				"device_ownership_from_security_context": true,
			},
		},
	}
	got := mergeContainerdConfig(base, overlay)

	want := map[string]any{
		"version": int64(2),
		"plugins": map[string]any{
			"io.containerd.cri.v1.images": map[string]any{
				"max_concurrent_downloads": int64(3),
			},
			"io.containerd.cri.v1.runtime": map[string]any{
				"device_ownership_from_security_context": true,
			},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("merge =\n  %#v\nwant\n  %#v", got, want)
	}

	// The merge must not mutate base.
	plugins := base["plugins"].(map[string]any)
	if _, leaked := plugins["io.containerd.cri.v1.runtime"]; leaked {
		t.Error("merge mutated the base map")
	}
}

func TestMergeContainerdConfigOverlayWins(t *testing.T) {
	base := map[string]any{"a": map[string]any{"x": int64(1)}}
	overlay := map[string]any{"a": map[string]any{"x": int64(2)}}
	got := mergeContainerdConfig(base, overlay)
	if got["a"].(map[string]any)["x"] != int64(2) {
		t.Errorf("overlay did not win: %#v", got)
	}
}

func TestContainerdConfigSatisfied(t *testing.T) {
	existing := map[string]any{
		"plugins": map[string]any{
			"io.containerd.cri.v1.runtime": map[string]any{
				"device_ownership_from_security_context": true,
			},
		},
	}
	// Overlay already present → satisfied.
	overlayPresent := map[string]any{
		"plugins": map[string]any{
			"io.containerd.cri.v1.runtime": map[string]any{
				"device_ownership_from_security_context": true,
			},
		},
	}
	if !containerdConfigSatisfied(existing, overlayPresent) {
		t.Error("expected satisfied when overlay already present")
	}
	// Overlay adds a new key → not satisfied.
	overlayNew := map[string]any{
		"plugins": map[string]any{
			"io.containerd.cri.v1.runtime": map[string]any{
				"other_setting": true,
			},
		},
	}
	if containerdConfigSatisfied(existing, overlayNew) {
		t.Error("expected not satisfied when overlay adds a key")
	}
}

func TestNormalizeViaTOMLMatchesParsedTypes(t *testing.T) {
	// A YAML-decoded overlay uses int; after normalization it must compare equal
	// to a TOML-parsed map (int64), so the idempotency check is reliable.
	overlay := map[string]any{
		"plugins": map[string]any{
			"x": map[string]any{"n": 3, "b": true},
		},
	}
	norm, err := normalizeViaTOML(overlay)
	if err != nil {
		t.Fatalf("normalize: %v", err)
	}
	got := norm["plugins"].(map[string]any)["x"].(map[string]any)
	if got["n"] != int64(3) {
		t.Errorf("n = %#v (%T), want int64(3)", got["n"], got["n"])
	}
	if got["b"] != true {
		t.Errorf("b = %#v, want true", got["b"])
	}
}
