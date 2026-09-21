// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider

// Internal tests for the DNS-bypass logic ported from #2786: dnsServers mode
// selection, systemextensionsctl output parsing, managed-Mac detection, and the
// --dns-servers placement in buildStartArgs.

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

func TestDNSServers(t *testing.T) {
	static := []string{"8.8.8.8", "1.1.1.1"}
	cases := []struct {
		name       string
		driver     string
		mode       string
		managedMac bool
		want       []string
		wantErr    bool
	}{
		{name: "auto vm managed → static", driver: "kvm2", mode: dnsModeAuto, managedMac: true, want: static},
		{name: "auto vm unmanaged → host", driver: "kvm2", mode: dnsModeAuto, managedMac: false, want: nil},
		{name: "auto docker managed → host", driver: "docker", mode: dnsModeAuto, managedMac: true, want: nil},
		{name: "auto vfkit managed → static", driver: "vfkit", mode: dnsModeAuto, managedMac: true, want: static},
		{name: "static vm → static", driver: "kvm2", mode: dnsModeStatic, want: static},
		{name: "static docker → host fallback", driver: "docker", mode: dnsModeStatic, want: nil},
		{name: "host vm → host", driver: "kvm2", mode: dnsModeHost, want: nil},
		{name: "invalid mode → error", driver: "kvm2", mode: "bogus", wantErr: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := dnsServers(tc.driver, tc.mode, tc.managedMac)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil (servers=%v)", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("dnsServers = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestBuildStartArgsWithDNSServers(t *testing.T) {
	prof := envfile.Profile{Name: "dr1", MinikubeSpec: envfile.MinikubeSpec{Driver: "kvm2"}}
	got := buildStartArgs(prof, "linux", "amd64", []string{"8.8.8.8", "1.1.1.1"})
	want := []string{
		"-p", "dr1",
		"--dns-servers", "8.8.8.8,1.1.1.1",
		"--driver", "kvm2",
		"--extra-config", "kubelet.serialize-image-pulls=false",
		"--wait-timeout", "180s",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("args =\n  %v\nwant\n  %v", got, want)
	}
}

// sampleExtensionsList mimics `systemextensionsctl list` output: a category
// header count line, the column header, then extension rows.
const sampleExtensionsList = `1 extension(s)
enabled	active	teamID	bundleID (version)	name	[state]
*	*	ABCDE12345	com.cisco.anyconnect.macos.acsockext (5.1)	Cisco AnyConnect	[activated enabled]
	*	FGHIJ67890	com.example.disabled (1.0)	Disabled Ext	[activated waiting for user]`

func TestParseNetworkExtensions(t *testing.T) {
	exts := parseNetworkExtensions(sampleExtensionsList)
	if len(exts) != 2 {
		t.Fatalf("expected 2 extensions, got %d: %+v", len(exts), exts)
	}
	if !exts[0].enabled || !exts[0].active {
		t.Errorf("ext[0] should be enabled+active, got %+v", exts[0])
	}
	if exts[0].name != "Cisco AnyConnect" {
		t.Errorf("ext[0].name = %q, want %q", exts[0].name, "Cisco AnyConnect")
	}
	if exts[0].state != "activated enabled" {
		t.Errorf("ext[0].state = %q, want %q", exts[0].state, "activated enabled")
	}
	if exts[1].enabled {
		t.Errorf("ext[1] should not be enabled, got %+v", exts[1])
	}
	if !exts[1].active {
		t.Errorf("ext[1] should be active, got %+v", exts[1])
	}
}

func TestParseNetworkExtensionsEmpty(t *testing.T) {
	if exts := parseNetworkExtensions("0 extension(s)\n"); len(exts) != 0 {
		t.Errorf("expected no extensions, got %+v", exts)
	}
}

func TestIsManagedMacNonDarwin(t *testing.T) {
	// On non-Darwin, isManagedMac must not shell out at all.
	called := false
	output := func(ctx context.Context, name string, args ...string) (string, error) {
		called = true
		return "", nil
	}
	got, err := isManagedMac(context.Background(), "linux", output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Errorf("isManagedMac(linux) = true, want false")
	}
	if called {
		t.Errorf("systemextensionsctl should not run on non-Darwin")
	}
}

func TestIsManagedMacDarwinActive(t *testing.T) {
	output := func(ctx context.Context, name string, args ...string) (string, error) {
		if name != "systemextensionsctl" {
			t.Errorf("ran %q, want systemextensionsctl", name)
		}
		return sampleExtensionsList, nil
	}
	got, err := isManagedMac(context.Background(), "darwin", output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !got {
		t.Errorf("isManagedMac with active extension = false, want true")
	}
}

func TestIsManagedMacDarwinNone(t *testing.T) {
	output := func(ctx context.Context, name string, args ...string) (string, error) {
		return "0 extension(s)\n", nil
	}
	got, err := isManagedMac(context.Background(), "darwin", output)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got {
		t.Errorf("isManagedMac with no extensions = true, want false")
	}
}

func TestIsManagedMacDarwinError(t *testing.T) {
	wantErr := errors.New("boom")
	output := func(ctx context.Context, name string, args ...string) (string, error) {
		return "", wantErr
	}
	if _, err := isManagedMac(context.Background(), "darwin", output); !errors.Is(err, wantErr) {
		t.Errorf("isManagedMac error = %v, want wrapped %v", err, wantErr)
	}
}
