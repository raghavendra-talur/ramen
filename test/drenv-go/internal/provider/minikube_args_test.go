// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

// Internal tests for buildStartArgs. These exercise the platform-dependent
// --rosetta rule and the full minikube-start flag set deterministically by
// passing fixed GOOS/GOARCH, which the external (provider_test) argv tests
// cannot do because they run through the real runtime values.

import (
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

func boolPtr(b bool) *bool { return &b }

func TestBuildStartArgsFullParity(t *testing.T) {
	prof := envfile.Profile{
		Name: "dr1",
		MinikubeSpec: envfile.MinikubeSpec{
			Driver:                "kvm2",
			ContainerRuntime:      "containerd",
			Network:               "mynet",
			CPUs:                  4,
			Memory:                "8g",
			ExtraDisks:            1,
			DiskSize:              "50g",
			Nodes:                 2,
			CNI:                   "calico",
			ServiceClusterIPRange: "10.96.0.0/12",
			ExtraConfig:           []string{"apiserver.foo=bar"},
			FeatureGates:          []string{"A=true", "B=false"},
		},
	}

	got := buildStartArgs(prof, "linux", "amd64")
	want := []string{
		"-p", "dr1",
		"--driver", "kvm2",
		"--container-runtime", "containerd",
		"--extra-disks", "1",
		"--disk-size", "50g",
		"--network", "mynet",
		"--nodes", "2",
		"--cni", "calico",
		"--cpus", "4",
		"--memory", "8g",
		"--service-cluster-ip-range", "10.96.0.0/12",
		"--extra-config", "kubelet.serialize-image-pulls=false",
		"--extra-config", "apiserver.foo=bar",
		"--feature-gates", "A=true,B=false",
		"--wait-timeout", "180s",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("args =\n  %v\nwant\n  %v", got, want)
	}
}

func TestBuildStartArgsRosettaOnAppleSilicon(t *testing.T) {
	prof := envfile.Profile{Name: "dr1"}
	got := buildStartArgs(prof, "darwin", "arm64")
	want := []string{
		"-p", "dr1",
		"--extra-config", "kubelet.serialize-image-pulls=false",
		"--rosetta",
		"--wait-timeout", "180s",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("args = %v, want %v", got, want)
	}
}

func TestBuildStartArgsRosettaDisabledExplicitly(t *testing.T) {
	prof := envfile.Profile{Name: "dr1", MinikubeSpec: envfile.MinikubeSpec{Rosetta: boolPtr(false)}}
	got := buildStartArgs(prof, "darwin", "arm64")
	for _, a := range got {
		if a == "--rosetta" {
			t.Fatalf("--rosetta present despite rosetta: false; args = %v", got)
		}
	}
}

func TestBuildStartArgsNoRosettaOnNonApple(t *testing.T) {
	prof := envfile.Profile{Name: "dr1"}
	for _, plat := range [][2]string{{"linux", "amd64"}, {"darwin", "amd64"}, {"linux", "arm64"}} {
		got := buildStartArgs(prof, plat[0], plat[1])
		for _, a := range got {
			if a == "--rosetta" {
				t.Errorf("%s/%s: unexpected --rosetta in %v", plat[0], plat[1], got)
			}
		}
	}
}
