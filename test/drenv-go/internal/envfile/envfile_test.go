// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package envfile

import (
	"strings"
	"testing"
)

func TestLoadParsesEnv(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if env.Name != "sample" {
		t.Fatalf("Name = %q, want sample", env.Name)
	}
	if len(env.Profiles) != 4 {
		t.Fatalf("got %d profiles, want 4", len(env.Profiles))
	}
	if env.Ramen == nil || env.Ramen.Hub != "hub" {
		t.Fatalf("ramen.hub not parsed: %+v", env.Ramen)
	}
}

func TestLoadAppliesTemplate(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	dr1 := env.Profiles[0]
	if dr1.Name != "dr1" {
		t.Fatalf("profile[0] = %q, want dr1", dr1.Name)
	}
	// Driver is inherited from the template ("$vm") and then resolved to the
	// host's platform default, so it must no longer be the raw placeholder.
	if dr1.Driver == "$vm" {
		t.Fatalf("dr1.Driver = %q, want resolved platform driver, not the placeholder", dr1.Driver)
	}
	if dr1.CPUs != 4 {
		t.Fatalf("dr1.CPUs = %d, want 4 (from template)", dr1.CPUs)
	}
	if len(dr1.Workers) != 1 || len(dr1.Workers[0].Addons) != 2 {
		t.Fatalf("dr1 workers/addons not inherited: %+v", dr1.Workers)
	}
}

func TestLoadExpandsNameArg(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// dr1's ocm-cluster addon args ["$name","hub"] should become ["dr1","hub"].
	addon := env.Profiles[0].Workers[0].Addons[1]
	if addon.Name != "ocm-cluster" {
		t.Fatalf("addon = %q, want ocm-cluster", addon.Name)
	}
	if len(addon.Args) != 2 || addon.Args[0] != "dr1" || addon.Args[1] != "hub" {
		t.Fatalf("args = %v, want [dr1 hub]", addon.Args)
	}
	// dr2 should independently expand to dr2 (no slice aliasing).
	addon2 := env.Profiles[1].Workers[0].Addons[1]
	if addon2.Args[0] != "dr2" {
		t.Fatalf("dr2 args = %v, want first elem dr2", addon2.Args)
	}
}

func TestLoadParsesGlobalWorkers(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(env.Workers) != 1 || env.Workers[0].Addons[0].Name != "rbd-mirror" {
		t.Fatalf("global workers not parsed: %+v", env.Workers)
	}
}

func TestLoadUnknownTemplateErrors(t *testing.T) {
	// A profile referencing a template that does not exist is an error.
	e := &Env{
		Profiles: []Profile{{Name: "x", Template: "nope"}},
	}
	if err := e.expand(); err == nil || !strings.Contains(err.Error(), "unknown template") {
		t.Fatalf("expected unknown template error, got %v", err)
	}
}

func TestLoadParsesExternalProfile(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// The ext-cluster profile is the fourth (index 3) in the fixture.
	var ext *Profile
	for i := range env.Profiles {
		if env.Profiles[i].Name == "ext-cluster" {
			ext = &env.Profiles[i]
			break
		}
	}
	if ext == nil {
		t.Fatal("ext-cluster profile not found")
	}
	if !ext.External {
		t.Errorf("ext-cluster.External = false, want true")
	}
	// Regular profiles must not be flagged as external.
	for _, p := range env.Profiles {
		if p.Name != "ext-cluster" && p.External {
			t.Errorf("profile %q.External = true, want false", p.Name)
		}
	}
}

func TestApplyTemplateProfileOverrides(t *testing.T) {
	// A field set on the profile wins over the template's value.
	e := &Env{
		Templates: []Template{{Name: "base", MinikubeSpec: MinikubeSpec{Driver: "$vm", CPUs: 4}}},
		Profiles:  []Profile{{Name: "p", Template: "base", MinikubeSpec: MinikubeSpec{Driver: "custom", CPUs: 2}}},
	}
	if err := e.expand(); err != nil {
		t.Fatalf("expand: %v", err)
	}
	p := e.Profiles[0]
	if p.Driver != "custom" {
		t.Fatalf("Driver = %q, want custom (profile overrides template)", p.Driver)
	}
	if p.CPUs != 2 {
		t.Fatalf("CPUs = %d, want 2 (profile overrides template)", p.CPUs)
	}
}

// TestApplyTemplateInheritsMinikubeFields covers the minikube-creation fields
// added for parity with Python drenv: a profile inherits unset values from its
// template (container_runtime, extra_disks, disk_size, cni) and overrides win.
func TestApplyTemplateInheritsMinikubeFields(t *testing.T) {
	e := &Env{
		Templates: []Template{{Name: "base", MinikubeSpec: MinikubeSpec{
			ContainerRuntime: "containerd",
			ExtraDisks:       1,
			DiskSize:         "50g",
			CNI:              "calico",
		}}},
		Profiles: []Profile{{Name: "p", Template: "base", MinikubeSpec: MinikubeSpec{
			DiskSize: "100g", // override
		}}},
	}
	if err := e.expand(); err != nil {
		t.Fatalf("expand: %v", err)
	}
	p := e.Profiles[0]
	if p.ContainerRuntime != "containerd" {
		t.Errorf("ContainerRuntime = %q, want containerd (inherited)", p.ContainerRuntime)
	}
	if p.ExtraDisks != 1 {
		t.Errorf("ExtraDisks = %d, want 1 (inherited)", p.ExtraDisks)
	}
	if p.CNI != "calico" {
		t.Errorf("CNI = %q, want calico (inherited)", p.CNI)
	}
	if p.DiskSize != "100g" {
		t.Errorf("DiskSize = %q, want 100g (profile overrides template)", p.DiskSize)
	}
}

// TestResolvePlatform covers the $vm/$container/$network placeholder resolution
// for the supported (os, arch) combinations, matching Python's _PLATFORM_DEFAULTS.
func TestResolvePlatform(t *testing.T) {
	cases := []struct {
		goos, goarch            string
		inDriver, inNetwork     string
		wantDriver, wantNetwork string
	}{
		{"darwin", "arm64", "$vm", "$network", "vfkit", "vmnet-shared"},
		{"darwin", "amd64", "$vm", "$network", "vfkit", "vmnet-shared"},
		{"linux", "amd64", "$vm", "$network", "kvm2", "default"},
		{"linux", "arm64", "$vm", "$network", "", ""},
		{"darwin", "arm64", "$container", "$network", "podman", "vmnet-shared"},
		{"linux", "amd64", "$container", "$network", "docker", "default"},
		{"windows", "amd64", "$vm", "$network", "", ""},          // unknown OS → empty
		{"linux", "amd64", "custom", "mynet", "custom", "mynet"}, // non-placeholders untouched
	}
	for _, c := range cases {
		p := &Profile{MinikubeSpec: MinikubeSpec{Driver: c.inDriver, Network: c.inNetwork}}
		resolvePlatform(p, c.goos, c.goarch)
		if p.Driver != c.wantDriver {
			t.Errorf("%s/%s driver(%q) = %q, want %q", c.goos, c.goarch, c.inDriver, p.Driver, c.wantDriver)
		}
		if p.Network != c.wantNetwork {
			t.Errorf("%s/%s network(%q) = %q, want %q", c.goos, c.goarch, c.inNetwork, p.Network, c.wantNetwork)
		}
	}
}

func TestTreeRendersStructure(t *testing.T) {
	env, err := Load("testdata/sample.yaml")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	out := Tree(env)
	if !strings.Contains(out, "profile dr1") {
		t.Fatalf("tree missing 'profile dr1':\n%s", out)
	}
	if !strings.Contains(out, "rook-operator") {
		t.Fatalf("tree missing 'rook-operator':\n%s", out)
	}
}
