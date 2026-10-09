// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package stepper_test

import (
	"context"
	"encoding/json"
	"testing"

	"go.uber.org/zap"

	"github.com/ramendr/ramen/e2e/app"
	"github.com/ramendr/ramen/e2e/config"
	"github.com/ramendr/ramen/e2e/stepper"
	"github.com/ramendr/ramen/e2e/test"
)

func readTestConfig(t *testing.T) *config.Config {
	t.Helper()

	cfg, err := app.ReadConfig("testdata/config.yaml")
	if err != nil {
		t.Fatal(err)
	}

	return cfg
}

func TestListTestsJSON(t *testing.T) {
	cfg := readTestConfig(t)

	data, err := json.Marshal(stepper.ListTests(cfg))
	if err != nil {
		t.Fatal(err)
	}

	expected := `{"tests":[` +
		`{"name":"subscr-deploy-rbd","deployer":"subscr","deployerType":"subscr",` +
		`"workload":"deploy","pvcSpec":"rbd","namespace":"test-subscr-deploy-rbd"},` +
		`{"name":"second","deployer":"disapp-recipe","deployerType":"disapp",` +
		`"workload":"deploy","pvcSpec":"cephfs","namespace":"test-second"}]}`

	if string(data) != expected {
		t.Fatalf("unexpected list:\n got: %s\nwant: %s", data, expected)
	}
}

func TestListTestsEmpty(t *testing.T) {
	data, err := json.Marshal(stepper.ListTests(&config.Config{}))
	if err != nil {
		t.Fatal(err)
	}

	if string(data) != `{"tests":[]}` {
		t.Fatalf("expected empty list, got %s", data)
	}
}

func TestFindTestUnknown(t *testing.T) {
	cfg := readTestConfig(t)

	if _, err := stepper.FindTest(cfg, "no-such-test"); err == nil {
		t.Fatal("expected an error for an unknown test")
	}
}

type expectedContext struct {
	name                string
	appNamespace        string
	managementNamespace string
	discovered          bool
}

func TestNewTestContext(t *testing.T) {
	cfg := readTestConfig(t)
	parent := app.NewContext(context.Background(), cfg, nil, zap.NewNop().Sugar())

	cases := []expectedContext{
		{"subscr-deploy-rbd", "test-subscr-deploy-rbd", "test-subscr-deploy-rbd", false},
		{"second", "test-second", "ramen-ops", true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctx, err := stepper.NewTestContext(parent, c.name)
			if err != nil {
				t.Fatal(err)
			}

			checkContext(t, ctx, c)
		})
	}

	if _, err := stepper.NewTestContext(parent, "no-such-test"); err == nil {
		t.Fatal("expected an error for an unknown test")
	}
}

func checkContext(t *testing.T, ctx *test.Context, c expectedContext) {
	t.Helper()

	if ctx.Name() != c.name {
		t.Errorf("name %q, expected %q", ctx.Name(), c.name)
	}

	if ctx.AppNamespace() != c.appNamespace {
		t.Errorf("app namespace %q, expected %q", ctx.AppNamespace(), c.appNamespace)
	}

	if ctx.ManagementNamespace() != c.managementNamespace {
		t.Errorf("management namespace %q, expected %q", ctx.ManagementNamespace(), c.managementNamespace)
	}

	if ctx.Deployer().IsDiscovered() != c.discovered {
		t.Errorf("discovered %v, expected %v", ctx.Deployer().IsDiscovered(), c.discovered)
	}
}
