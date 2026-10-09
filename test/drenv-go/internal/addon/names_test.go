// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon_test

import (
	"slices"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

func TestNamesSortedAndIncludesRegistered(t *testing.T) {
	name := "zz-test-names-" + t.Name()
	addon.Register(name, func(addon.Deps, string, []string) ensure.Step { return nil })

	names := addon.Names()
	if !slices.IsSorted(names) {
		t.Fatalf("Names() not sorted: %v", names)
	}
	for _, want := range []string{"rook-operator", "rbd-mirror", "volsync", name} {
		if !slices.Contains(names, want) {
			t.Errorf("Names() missing %q", want)
		}
	}
}
