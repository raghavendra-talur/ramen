// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package provider_test

import (
	"context"
	"errors"
	"reflect"
	"runtime"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
	"github.com/ramendr/ramen/test/drenv-go/internal/provider"
)

// commonStartTail returns the flags MinikubeProvider.Start always appends after
// the profile-derived flags: the mandatory --extra-config, the platform-gated
// --rosetta (darwin/arm64 only), and --wait-timeout. Tests that go through the
// real Start build their expected argv with this so they pass on any host.
func commonStartTail() []string {
	tail := []string{"--extra-config", "kubelet.serialize-image-pulls=false"}
	if runtime.GOOS == "darwin" && runtime.GOARCH == "arm64" {
		tail = append(tail, "--rosetta")
	}
	return append(tail, "--wait-timeout", "180s")
}

// statusJSON builds a minimal minikube status JSON payload.
func runningJSON(name string) string {
	return `{"Name":"` + name + `","Host":"Running","APIServer":"Running","Kubelet":"Running"}`
}

func stoppedJSON(name string) string {
	return `{"Name":"` + name + `","Host":"Stopped","APIServer":"Stopped","Kubelet":"Stopped"}`
}

func unknownJSON(name string) string {
	return `{"Name":"` + name + `","Host":"Starting","APIServer":"Paused","Kubelet":"Running"}`
}

const notFoundOutput = `❌  Profile "dr1" not found.`

// newProvider is a test helper that wires a FakeRunner into MinikubeProvider.
func newProvider(f *cli.FakeRunner) provider.MinikubeProvider {
	return provider.MinikubeProvider{MK: &cli.Minikube{R: f}}
}

// ---- Status mapping table ----

func TestMinikubeProviderStatusRunning(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: runningJSON("dr1")})
	p := newProvider(f)

	s, err := p.Status(context.Background(), "dr1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != provider.StatusRunning {
		t.Errorf("Status = %s, want running", s)
	}
}

func TestMinikubeProviderStatusStopped(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: stoppedJSON("dr1")})
	p := newProvider(f)

	s, err := p.Status(context.Background(), "dr1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != provider.StatusStopped {
		t.Errorf("Status = %s, want stopped", s)
	}
}

func TestMinikubeProviderStatusNotFound(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: notFoundOutput})
	p := newProvider(f)

	s, err := p.Status(context.Background(), "dr1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != provider.StatusNotFound {
		t.Errorf("Status = %s, want not-found", s)
	}
}

func TestMinikubeProviderStatusUnknown(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: unknownJSON("dr1")})
	p := newProvider(f)

	s, err := p.Status(context.Background(), "dr1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if s != provider.StatusUnknown {
		t.Errorf("Status = %s, want unknown", s)
	}
}

func TestMinikubeProviderStatusPropagatesError(t *testing.T) {
	// Script non-JSON output plus a non-nil error, simulating a genuine
	// mid-flight failure (e.g. minikube binary not found, network error).
	// In a real failure the output is not parseable JSON, so
	// cli.Minikube.Status must propagate the original command error.
	sentinelErr := errors.New("some connection error")
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: "Error: unable to connect to minikube", Err: sentinelErr})
	p := newProvider(f)

	_, err := p.Status(context.Background(), "dr1")
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

// ---- Start argv tests ----

func TestMinikubeProviderStartFullProfile(t *testing.T) {
	f := &cli.FakeRunner{}
	p := newProvider(f)

	prof := envfile.Profile{
		Name: "dr1",
		MinikubeSpec: envfile.MinikubeSpec{
			Driver:           "kvm2",
			ContainerRuntime: "containerd",
			Network:          "mynet",
			CPUs:             4,
			Memory:           "8192m",
			ExtraDisks:       1,
			DiskSize:         "50g",
		},
	}
	if err := p.Start(context.Background(), prof); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := append([]string{
		"start", "-p", "dr1",
		"--driver", "kvm2",
		"--container-runtime", "containerd",
		"--extra-disks", "1",
		"--disk-size", "50g",
		"--network", "mynet",
		"--cpus", "4",
		"--memory", "8192m",
	}, commonStartTail()...)
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeProviderStartOmitsPlaceholderDriver(t *testing.T) {
	f := &cli.FakeRunner{}
	p := newProvider(f)

	prof := envfile.Profile{
		Name: "dr1",
		MinikubeSpec: envfile.MinikubeSpec{
			Driver: "$vm", // unresolved placeholder — should be omitted
			CPUs:   2,
			Memory: "4096m",
		},
	}
	if err := p.Start(context.Background(), prof); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := append([]string{"start", "-p", "dr1", "--cpus", "2", "--memory", "4096m"}, commonStartTail()...)
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeProviderStartOmitsPlaceholderNetwork(t *testing.T) {
	f := &cli.FakeRunner{}
	p := newProvider(f)

	prof := envfile.Profile{
		Name: "dr1",
		MinikubeSpec: envfile.MinikubeSpec{
			Driver:  "kvm2",
			Network: "$network", // unresolved placeholder — should be omitted
			CPUs:    2,
			Memory:  "4096m",
		},
	}
	if err := p.Start(context.Background(), prof); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := append([]string{"start", "-p", "dr1", "--driver", "kvm2", "--cpus", "2", "--memory", "4096m"}, commonStartTail()...)
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeProviderStartMinimal(t *testing.T) {
	// Only name set; no driver/network/cpus/memory. The profile-derived flags
	// are all omitted, leaving just "-p <name>" plus the always-on tail.
	f := &cli.FakeRunner{}
	p := newProvider(f)

	prof := envfile.Profile{Name: "dr1"}
	if err := p.Start(context.Background(), prof); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := append([]string{"start", "-p", "dr1"}, commonStartTail()...)
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

// ---- Exists tests ----

func TestMinikubeProviderExistsTrue(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: runningJSON("dr1")})
	p := newProvider(f)

	ok, err := p.Exists(context.Background(), "dr1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !ok {
		t.Errorf("Exists = false, want true")
	}
}

func TestMinikubeProviderExistsFalse(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: notFoundOutput})
	p := newProvider(f)

	ok, err := p.Exists(context.Background(), "dr1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Errorf("Exists = true, want false")
	}
}

// ---- Suspend / Resume argv tests ----

func TestMinikubeProviderSuspendIssuesCorrectArgv(t *testing.T) {
	f := &cli.FakeRunner{}
	p := newProvider(f)

	if err := p.Suspend(context.Background(), "dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{"pause", "-p", "dr1"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeProviderResumeIssuesCorrectArgv(t *testing.T) {
	f := &cli.FakeRunner{}
	p := newProvider(f)

	if err := p.Resume(context.Background(), "dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{"unpause", "-p", "dr1"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

// ---- Status String() tests ----

func TestStatusString(t *testing.T) {
	cases := []struct {
		s    provider.Status
		want string
	}{
		{provider.StatusRunning, "running"},
		{provider.StatusStopped, "stopped"},
		{provider.StatusNotFound, "not-found"},
		{provider.StatusUnknown, "unknown"},
	}
	for _, tc := range cases {
		if got := tc.s.String(); got != tc.want {
			t.Errorf("Status(%d).String() = %q, want %q", tc.s, got, tc.want)
		}
	}
}
