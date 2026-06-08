// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package e2econfig_test

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/e2econfig"
	"github.com/ramendr/ramen/test/drenv-go/internal/envfile"
)

func TestDumpWritesKubeconfigsAndConfig(t *testing.T) {
	f := &cli.FakeRunner{}
	// One Config call per cluster, in [hub, clusters...] order.
	f.Script(cli.FakeResult{Out: "hub-kubeconfig-content"})
	f.Script(cli.FakeResult{Out: "dr1-kubeconfig-content"})
	f.Script(cli.FakeResult{Out: "dr2-kubeconfig-content"})

	k := &cli.Kubectl{R: f}
	baseDir := t.TempDir()
	ramen := envfile.Ramen{Hub: "hub", Clusters: []string{"dr1", "dr2"}}

	if err := e2econfig.Dump(context.Background(), k, baseDir, ramen); err != nil {
		t.Fatalf("Dump() error = %v", err)
	}

	// Each cluster gets a self-contained kubeconfig named by its real name.
	want := map[string]string{
		"hub": "hub-kubeconfig-content",
		"dr1": "dr1-kubeconfig-content",
		"dr2": "dr2-kubeconfig-content",
	}
	for cluster, content := range want {
		path := filepath.Join(baseDir, "kubeconfigs", cluster)

		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading kubeconfig %q: %v", path, err)
		}

		if string(got) != content {
			t.Errorf("kubeconfig %q = %q, want %q", cluster, got, content)
		}
	}

	// config.yaml maps the e2e names onto the per-cluster files.
	configPath := filepath.Join(baseDir, "config.yaml")

	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("reading config.yaml: %v", err)
	}

	var parsed struct {
		Clusters map[string]struct {
			Kubeconfig string `yaml:"kubeconfig"`
		} `yaml:"clusters"`
	}
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("parsing config.yaml: %v", err)
	}

	wantClusters := map[string]string{
		"hub": filepath.Join(baseDir, "kubeconfigs", "hub"),
		"c1":  filepath.Join(baseDir, "kubeconfigs", "dr1"),
		"c2":  filepath.Join(baseDir, "kubeconfigs", "dr2"),
	}
	for e2eName, wantPath := range wantClusters {
		got, ok := parsed.Clusters[e2eName]
		if !ok {
			t.Errorf("config.yaml missing cluster %q", e2eName)

			continue
		}

		if got.Kubeconfig != wantPath {
			t.Errorf("config.yaml cluster %q kubeconfig = %q, want %q", e2eName, got.Kubeconfig, wantPath)
		}
	}
}

// TestDumpUsesFlattenMinifyContext asserts the exact kubectl invocation, since
// e2e/localrun rely on each file being a self-contained, single-context config.
func TestDumpUsesFlattenMinifyContext(t *testing.T) {
	f := &cli.FakeRunner{}
	k := &cli.Kubectl{R: f}
	ramen := envfile.Ramen{Hub: "hub", Clusters: []string{"dr1"}}

	if err := e2econfig.Dump(context.Background(), k, t.TempDir(), ramen); err != nil {
		t.Fatalf("Dump() error = %v", err)
	}

	wantArgs := [][]string{
		{"config", "view", "--flatten", "--minify", "--context=hub"},
		{"config", "view", "--flatten", "--minify", "--context=dr1"},
	}
	if len(f.Calls) != len(wantArgs) {
		t.Fatalf("got %d kubectl calls, want %d: %+v", len(f.Calls), len(wantArgs), f.Calls)
	}

	for i, want := range wantArgs {
		if f.Calls[i].Name != "kubectl" {
			t.Errorf("call %d name = %q, want kubectl", i, f.Calls[i].Name)
		}

		if !reflect.DeepEqual(f.Calls[i].Args, want) {
			t.Errorf("call %d args = %v, want %v", i, f.Calls[i].Args, want)
		}
	}
}

// TestDumpTruncatesExtraClusters mirrors the Python zip() behavior: only hub,
// c1, c2 slots exist, so a fourth cluster is ignored.
func TestDumpTruncatesExtraClusters(t *testing.T) {
	f := &cli.FakeRunner{}
	k := &cli.Kubectl{R: f}
	ramen := envfile.Ramen{Hub: "hub", Clusters: []string{"dr1", "dr2", "dr3"}}

	if err := e2econfig.Dump(context.Background(), k, t.TempDir(), ramen); err != nil {
		t.Fatalf("Dump() error = %v", err)
	}

	// hub + dr1 + dr2 = 3 calls; dr3 has no e2e slot.
	if len(f.Calls) != 3 {
		t.Fatalf("got %d kubectl calls, want 3 (dr3 should be truncated): %+v", len(f.Calls), f.Calls)
	}
}

// TestDumpSingleManagedCluster covers a topology with only one managed cluster
// (hub + c1), as in a minimal env.
func TestDumpSingleManagedCluster(t *testing.T) {
	f := &cli.FakeRunner{}
	k := &cli.Kubectl{R: f}
	ramen := envfile.Ramen{Hub: "hub", Clusters: []string{"dr1"}}

	if err := e2econfig.Dump(context.Background(), k, t.TempDir(), ramen); err != nil {
		t.Fatalf("Dump() error = %v", err)
	}

	if len(f.Calls) != 2 {
		t.Fatalf("got %d kubectl calls, want 2 (hub + dr1): %+v", len(f.Calls), f.Calls)
	}
}

// TestDumpExportError surfaces a kubectl failure rather than writing a partial
// config.
func TestDumpExportError(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Err: os.ErrPermission})

	k := &cli.Kubectl{R: f}
	baseDir := t.TempDir()
	ramen := envfile.Ramen{Hub: "hub", Clusters: []string{"dr1"}}

	if err := e2econfig.Dump(context.Background(), k, baseDir, ramen); err == nil {
		t.Fatal("Dump() error = nil, want error from kubectl export")
	}

	if _, err := os.Stat(filepath.Join(baseDir, "config.yaml")); !os.IsNotExist(err) {
		t.Errorf("config.yaml should not be written when export fails (stat err = %v)", err)
	}
}
