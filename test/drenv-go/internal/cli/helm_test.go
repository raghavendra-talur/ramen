// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"context"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
)

func TestHelmRepoAddIssuesCorrectArgv(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	h := cli.Helm{R: f}

	if err := h.RepoAdd(ctx, "volsync", "https://backube.github.io/helm-charts/"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(f.Calls))
	}
	c := f.Calls[0]
	if c.Name != "helm" {
		t.Errorf("Name = %q, want %q", c.Name, "helm")
	}
	want := []string{"repo", "add", "--force-update", "volsync", "https://backube.github.io/helm-charts/"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestHelmUpgradeInstallIssuesCorrectArgvNoExtra(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	h := cli.Helm{R: f}

	if err := h.UpgradeInstall(ctx, "volsync", "volsync/volsync", "dr1"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{"upgrade", "--install", "volsync", "volsync/volsync", "--kube-context", "dr1"}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}

func TestHelmUpgradeInstallIssuesCorrectArgvWithExtra(t *testing.T) {
	ctx := context.Background()
	f := &cli.FakeRunner{}
	h := cli.Helm{R: f}

	if err := h.UpgradeInstall(ctx, "volsync", "volsync/volsync", "dr1",
		"--namespace", "volsync-system",
		"--create-namespace",
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	c := f.Calls[0]
	want := []string{
		"upgrade", "--install", "volsync", "volsync/volsync",
		"--kube-context", "dr1",
		"--namespace", "volsync-system",
		"--create-namespace",
	}
	if !reflect.DeepEqual(c.Args, want) {
		t.Errorf("args = %v, want %v", c.Args, want)
	}
}
