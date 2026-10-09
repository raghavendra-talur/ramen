// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Disabling testpackage linter to test unexported helpers.
//
//nolint:testpackage
package stepper

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.uber.org/zap"

	"github.com/ramendr/ramen/e2e/app"
	"github.com/ramendr/ramen/e2e/config"
	"github.com/ramendr/ramen/e2e/test"
)

func newUnconnectedTestContext(t *testing.T, name, distro string) *test.Context {
	t.Helper()

	cfg, err := app.ReadConfig("testdata/config.yaml")
	if err != nil {
		t.Fatal(err)
	}

	if distro == "" {
		// Simulate a configuration without distro before detecting it.
		cfg.Distro = ""
		cfg.Namespaces = config.Namespaces{}
	}

	ctx, err := NewTestContext(app.NewContext(context.Background(), cfg, nil, zap.NewNop().Sugar()), name)
	if err != nil {
		t.Fatal(err)
	}

	return ctx
}

func TestManagementNamespace(t *testing.T) {
	cases := []struct {
		name     string
		distro   string
		expected string
	}{
		{"subscr-deploy-rbd", "k8s", "test-subscr-deploy-rbd"},
		{"subscr-deploy-rbd", "", "test-subscr-deploy-rbd"},
		{"second", "k8s", "ramen-ops"},
		// Depends on the distro, so it is unknown.
		{"second", "", ""},
	}

	for _, c := range cases {
		ctx := newUnconnectedTestContext(t, c.name, c.distro)

		if ns := resolvedManagementNamespace(ctx); ns != c.expected {
			t.Errorf("test %q distro %q: management namespace %q, expected %q", c.name, c.distro, ns, c.expected)
		}
	}
}

func TestUnobserved(t *testing.T) {
	ctx := newUnconnectedTestContext(t, "second", "")
	s := Unobserved(ctx, errors.New("cluster unreachable"))

	if s.Position != PositionUnknown {
		t.Errorf("position %q, expected %q", s.Position, PositionUnknown)
	}

	if s.PositionReason != "could not observe the clusters: cluster unreachable" {
		t.Errorf("unexpected reason %q", s.PositionReason)
	}

	if len(s.Errors) != 1 || s.Errors[0] != "cluster unreachable" {
		t.Errorf("unexpected errors %q", s.Errors)
	}

	if s.Test != "second" || s.Namespace != "test-second" || s.ManagementNamespace != "" {
		t.Errorf("unexpected names: %+v", s)
	}

	if s.Clusters == nil || s.DRPC.Conditions == nil {
		t.Error("nil slices in unobserved state")
	}
}

func TestCollectAbandonsSlowReaders(t *testing.T) {
	block := make(chan struct{})
	defer close(block)

	fast := reader{
		read: func() (func(*State), []string) {
			return func(s *State) { s.Test = "fast" }, []string{"fast error"}
		},
	}
	slow := reader{
		read: func() (func(*State), []string) {
			<-block

			return func(*State) {}, nil
		},
	}

	start := time.Now()
	results := collect([]reader{slow, fast}, 100*time.Millisecond)

	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("collect took %v", elapsed)
	}

	if results[0] != nil {
		t.Error("expected no result for the slow reader")
	}

	if results[1] == nil || len(results[1].errs) != 1 {
		t.Fatalf("unexpected result for the fast reader: %+v", results[1])
	}

	s := &State{}
	results[1].apply(s)

	if s.Test != "fast" {
		t.Errorf("fast reader not applied: %+v", s)
	}
}
