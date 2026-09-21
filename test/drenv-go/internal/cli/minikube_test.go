// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

// statusJSON is a real-looking minikube status -o json payload.
const statusJSON = `{
  "Name": "dr1",
  "Host": "Running",
  "Kubelet": "Running",
  "APIServer": "Running",
  "Kubeconfig": "Configured"
}`

// notFoundOutput is typical output when minikube can't find the profile.
const notFoundOutput = `❌  Profile "dr1" not found.`

func TestMinikubeStatusParses(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: statusJSON})

	mk := cli.Minikube{R: f}
	st, err := mk.Status(ctx, "dr1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := cli.MinikubeStatus{Name: "dr1", Host: "Running", APIServer: "Running", Kubeconfig: "Configured"}
	if st != want {
		t.Errorf("Status = %+v, want %+v", st, want)
	}
}

func TestMinikubeStatusNotFound(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	// Minikube exits non-zero for a missing profile; we script the error too
	// but the content matters more — notFoundOutput triggers the not-found path.
	f.Script(cli.FakeResult{Out: notFoundOutput, Err: nil})

	mk := cli.Minikube{R: f}
	st, err := mk.Status(ctx, "dr1")
	if err != nil {
		t.Fatalf("unexpected error for not-found profile: %v", err)
	}
	if st.Host != "" {
		t.Errorf("expected empty Host for not-found profile, got %q", st.Host)
	}
}

func TestMinikubeStatusNotFoundWithError(t *testing.T) {
	// When Output returns an error AND the output contains the not-found marker,
	// Status should treat it as "not found" (zero value, nil error).
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: notFoundOutput, Err: &fakeExitError{}})

	mk := cli.Minikube{R: f}
	st, err := mk.Status(ctx, "dr1")
	if err != nil {
		t.Fatalf("unexpected error for not-found profile: %v", err)
	}
	if st.Host != "" {
		t.Errorf("expected empty Host for not-found profile, got %q", st.Host)
	}
}

// fakeExitError simulates an exec.ExitError for tests.
type fakeExitError struct{}

func (e *fakeExitError) Error() string { return "exit status 1" }

// stoppedJSON is what minikube emits for an existing-but-stopped cluster. Note
// that minikube exits non-zero (code 7) in this case while still printing valid
// JSON.
const stoppedJSON = `{"Name":"dr1","Host":"Stopped","Kubelet":"Stopped","APIServer":"Stopped"}`

// TestMinikubeStatusStoppedWithExitError pins the fix for minikube's exit-7 on
// stopped clusters: when the output is valid JSON, Status must parse it and
// discard the exit error rather than treating the cluster as failed.
func TestMinikubeStatusStoppedWithExitError(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: stoppedJSON, Err: &fakeExitError{}})

	mk := cli.Minikube{R: f}
	st, err := mk.Status(ctx, "dr1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if st.Host != "Stopped" {
		t.Errorf("Host = %q, want %q", st.Host, "Stopped")
	}
}

// misconfiguredOutput reproduces minikube's combined stdout+stderr for an
// existing-but-misconfigured cluster (interrupted mid-start): a stderr
// diagnostic line is interleaved before the JSON, and minikube exits 6.
const misconfiguredOutput = `E0916 18:28:03.535535   86523 status.go:457] kubeconfig endpoint: get endpoint: "dr2" does not appear in /Users/rtalur/.kube/config
{"Name":"dr2","Host":"Running","Kubelet":"Stopped","APIServer":"Stopped","Kubeconfig":"Misconfigured","Worker":false}`

// TestMinikubeStatusMisconfiguredWithStderrNoise pins the fix for minikube
// exit-6 on a misconfigured cluster whose stderr diagnostics are interleaved
// with the JSON payload in the combined output the runner captures. Status must
// isolate and parse the JSON object rather than treating the cluster as failed,
// so the start flow reconciles it via `minikube start`.
func TestMinikubeStatusMisconfiguredWithStderrNoise(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: misconfiguredOutput, Err: &fakeExitError{}})

	mk := cli.Minikube{R: f}
	st, err := mk.Status(ctx, "dr2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := cli.MinikubeStatus{Name: "dr2", Host: "Running", APIServer: "Stopped", Kubeconfig: "Misconfigured"}
	if st != want {
		t.Errorf("Status = %+v, want %+v", st, want)
	}
}

func TestMinikubeStartIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	mk := cli.Minikube{R: f}

	if err := mk.Start(ctx, "-p", "dr1", "--driver", "kvm2"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "minikube" {
		t.Errorf("name = %q, want %q", c.Name, "minikube")
	}
	want := []string{"start", "-p", "dr1", "--driver", "kvm2"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeStopIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	mk := cli.Minikube{R: f}

	if err := mk.Stop(ctx, "dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{"stop", "-p", "dr1"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeDeleteIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	mk := cli.Minikube{R: f}

	if err := mk.Delete(ctx, "dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"delete", "-p", "dr1"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeLoadImageIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	mk := cli.Minikube{R: f}

	if err := mk.LoadImage(ctx, "dr1", "docker.io/foo/bar:latest"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"image", "load", "-p", "dr1", "docker.io/foo/bar:latest"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeStatusIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: statusJSON})
	mk := cli.Minikube{R: f}

	_, _ = mk.Status(ctx, "dr1")
	c := f.Calls[0]
	want := []string{"status", "-p", "dr1", "-o", "json"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubePauseIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	mk := cli.Minikube{R: f}

	if err := mk.Pause(ctx, "dr1"); err != nil {
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

func TestMinikubeUnpauseIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	mk := cli.Minikube{R: f}

	if err := mk.Unpause(ctx, "dr1"); err != nil {
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

func TestMinikubeCpIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	mk := cli.Minikube{R: f}

	if err := mk.Cp(ctx, "dr1", "dr1:/etc/containerd/config.toml", "/tmp/config.toml"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{"cp", "--profile", "dr1", "dr1:/etc/containerd/config.toml", "/tmp/config.toml"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestMinikubeSSHIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	mk := cli.Minikube{R: f}

	if err := mk.SSH(ctx, "dr1", "sudo systemctl restart containerd"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	want := []string{"ssh", "--profile", "dr1", "sudo systemctl restart containerd"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}
