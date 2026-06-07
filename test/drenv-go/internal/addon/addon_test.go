// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package addon_test

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"

	"github.com/ramendr/ramen/test/drenv-go/internal/addon"
	"github.com/ramendr/ramen/test/drenv-go/internal/cli"
	"github.com/ramendr/ramen/test/drenv-go/internal/ensure"
)

var errBoom = errors.New("boom")

// ---- Registry tests ----

// freshRegistry creates an isolated registry per test by calling the package-
// level functions. Because the global registry persists across tests, each
// test uses a unique name derived from t.Name().
func TestRegistryLookupAfterRegister(t *testing.T) {
	name := "test-addon-" + t.Name()
	called := false
	addon.Register(name, func(d addon.Deps, cluster string, args []string) ensure.Step {
		called = true
		return nil
	})

	b, ok := addon.Lookup(name)
	if !ok {
		t.Fatalf("Lookup(%q) = false, want true", name)
	}
	if b == nil {
		t.Fatal("Lookup returned nil builder")
	}
	// Call the builder to confirm it's the right one.
	b(addon.Deps{}, "dr1", nil)
	if !called {
		t.Error("builder was not called")
	}
}

func TestRegistryLookupUnregisteredReturnsFalse(t *testing.T) {
	_, ok := addon.Lookup("this-addon-does-not-exist")
	if ok {
		t.Fatal("Lookup for unregistered addon should return false")
	}
}

func TestRegistryDuplicateRegistrationPanics(t *testing.T) {
	name := "panic-test-addon-" + t.Name()
	addon.Register(name, func(d addon.Deps, cluster string, args []string) ensure.Step { return nil })

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on duplicate registration, got none")
		}
	}()
	addon.Register(name, func(d addon.Deps, cluster string, args []string) ensure.Step { return nil })
}

// ---- ApplyTemplateBytes tests ----

func TestApplyTemplateBytesSubstitutesDollarKey(t *testing.T) {
	tmpl := []byte("namespace: $NS\nname: $NAME\n")
	vars := map[string]string{"NS": "ramen-system", "NAME": "my-resource"}
	got := addon.ApplyTemplateBytes(tmpl, vars)
	want := []byte("namespace: ramen-system\nname: my-resource\n")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestApplyTemplateBytesSubstitutesBracedKey(t *testing.T) {
	tmpl := []byte("url: ${ENDPOINT}/bucket\n")
	vars := map[string]string{"ENDPOINT": "http://192.168.64.10:30000"}
	got := addon.ApplyTemplateBytes(tmpl, vars)
	want := []byte("url: http://192.168.64.10:30000/bucket\n")
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestApplyTemplateBytesUnknownVarPreserved(t *testing.T) {
	// Unlike os.Expand, an unrecognised "$" token is left untouched so we never
	// silently corrupt manifest content that legitimately contains "$".
	tmpl := []byte("value: $UNKNOWN\n")
	got := addon.ApplyTemplateBytes(tmpl, map[string]string{})
	if !reflect.DeepEqual(got, tmpl) {
		t.Errorf("got %q, want %q", got, tmpl)
	}
}

func TestApplyTemplateBytesMixed(t *testing.T) {
	got := addon.ApplyTemplateBytes([]byte("${A} and $B"), map[string]string{"A": "hello", "B": "world"})
	if want := []byte("hello and world"); !reflect.DeepEqual(got, want) {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestApplyTemplateBytesNoSubstitution(t *testing.T) {
	tmpl := []byte("apiVersion: v1\nkind: Namespace\n")
	got := addon.ApplyTemplateBytes(tmpl, nil)
	if !reflect.DeepEqual(got, tmpl) {
		t.Errorf("got %q, want %q", got, tmpl)
	}
}

// ---- ApplyTemplate (disk) test ----

func TestApplyTemplateReadsAndSubstitutes(t *testing.T) {
	// Write a temporary template file.
	dir := t.TempDir()
	content := "storageClass: $STORAGE_CLASS\npool: ${POOL}\n"
	if err := os.WriteFile(filepath.Join(dir, "pool.yaml"), []byte(content), 0o644); err != nil {
		t.Fatalf("write template: %v", err)
	}

	d := addon.Deps{AddonsDir: dir}
	vars := map[string]string{"STORAGE_CLASS": "ocs-storagecluster-ceph-rbd", "POOL": "replicapool"}
	got, err := addon.ApplyTemplate(d, "pool.yaml", vars)
	if err != nil {
		t.Fatalf("ApplyTemplate: %v", err)
	}
	want := "storageClass: ocs-storagecluster-ceph-rbd\npool: replicapool\n"
	if string(got) != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestApplyTemplateMissingFileReturnsError(t *testing.T) {
	d := addon.Deps{AddonsDir: t.TempDir()}
	_, err := addon.ApplyTemplate(d, "nonexistent.yaml", nil)
	if err == nil {
		t.Fatal("expected error for missing file, got nil")
	}
}

// ---- Primitive step Done/Do tests ----

// TestApplyStepDoneAlwaysFalse verifies that an apply-flavoured step always
// returns Done=false and that Do invokes the underlying kubectl command.
// We test the step primitives directly (not via ensure.Ensure) to avoid
// needing a real ensure.Options ticker.
func TestApplyStepDoneAlwaysFalse(t *testing.T) {
	f := &cli.FakeRunner{}
	k := &cli.Kubectl{R: f}
	ctx := context.Background()

	var doCallCount int
	step := applyOnlyStep{
		name: "apply/manifest",
		doFn: func(ctx context.Context) error {
			doCallCount++
			return k.ApplyFile(ctx, "dr1", "manifest.yaml")
		},
	}

	// Done should always return false for apply steps.
	ok, err := step.Done(ctx)
	if err != nil {
		t.Fatalf("Done returned error: %v", err)
	}
	if ok {
		t.Error("Done = true, want false (apply step is never done)")
	}

	// Calling Done a second time still returns false.
	ok, err = step.Done(ctx)
	if err != nil {
		t.Fatalf("Done (2nd) returned error: %v", err)
	}
	if ok {
		t.Error("Done (2nd) = true, want false")
	}

	// Do should invoke ApplyFile exactly once.
	if err := step.Do(ctx); err != nil {
		t.Fatalf("Do returned error: %v", err)
	}
	if doCallCount != 1 {
		t.Errorf("doCallCount = %d, want 1", doCallCount)
	}
	if len(f.Calls) != 1 {
		t.Fatalf("expected 1 kubectl call, got %d", len(f.Calls))
	}
	wantArgs := []string{"--context", "dr1", "apply", "--filename", "manifest.yaml"}
	if !reflect.DeepEqual(f.Calls[0].Args, wantArgs) {
		t.Errorf("kubectl args = %v, want %v", f.Calls[0].Args, wantArgs)
	}
}

// applyOnlyStep is an ensure.Step whose Done always returns false, for testing.
type applyOnlyStep struct {
	name string
	doFn func(ctx context.Context) error
}

func (s applyOnlyStep) Name() string                         { return s.name }
func (s applyOnlyStep) Done(_ context.Context) (bool, error) { return false, nil }
func (s applyOnlyStep) Do(ctx context.Context) error         { return s.doFn(ctx) }

// ---- MinioServiceURL tests ----

func TestMinioServiceURLComposesCorrectly(t *testing.T) {
	f := &cli.FakeRunner{}
	// First call: GetJSONPath for pod hostIP.
	f.Script(cli.FakeResult{Out: "192.168.64.10"})
	// Second call: GetJSONPath for service nodePort.
	f.Script(cli.FakeResult{Out: "30000"})

	k := &cli.Kubectl{R: f}
	ctx := context.Background()

	url, err := addon.MinioServiceURL(ctx, k, "dr1")
	if err != nil {
		t.Fatalf("MinioServiceURL: %v", err)
	}
	if url != "http://192.168.64.10:30000" {
		t.Errorf("url = %q, want %q", url, "http://192.168.64.10:30000")
	}

	// Verify the correct kubectl commands were issued.
	if len(f.Calls) != 2 {
		t.Fatalf("expected 2 kubectl calls, got %d", len(f.Calls))
	}
	// First call: get pod hostIP
	if f.Calls[0].Name != "kubectl" {
		t.Errorf("call[0].Name = %q, want kubectl", f.Calls[0].Name)
	}
	// Second call: get service nodePort
	if f.Calls[1].Name != "kubectl" {
		t.Errorf("call[1].Name = %q, want kubectl", f.Calls[1].Name)
	}
}

func TestMinioServiceURLErrorOnEmptyHostIP(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Out: ""}) // empty hostIP
	k := &cli.Kubectl{R: f}

	_, err := addon.MinioServiceURL(context.Background(), k, "dr1")
	if err == nil {
		t.Fatal("expected error when hostIP is empty, got nil")
	}
}

func TestMinioServiceURLPropagatesHostIPError(t *testing.T) {
	f := &cli.FakeRunner{}
	f.Script(cli.FakeResult{Err: errBoom})
	k := &cli.Kubectl{R: f}

	_, err := addon.MinioServiceURL(context.Background(), k, "dr1")
	if err == nil {
		t.Fatal("expected error when GetJSONPath fails, got nil")
	}
}
