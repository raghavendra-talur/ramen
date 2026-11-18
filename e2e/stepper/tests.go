// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Package stepper drives the DR lifecycle of a single e2e test one action at a
// time, and observes where the test application is in the lifecycle between
// actions. It uses the same deployers, workloads and DR actions as the e2e
// suite.
package stepper

import (
	"fmt"

	"github.com/ramendr/ramen/e2e/config"
	"github.com/ramendr/ramen/e2e/deployers"
	"github.com/ramendr/ramen/e2e/test"
	"github.com/ramendr/ramen/e2e/types"
	"github.com/ramendr/ramen/e2e/workloads"
)

// TestInfo describes a configured test. The JSON field names are a contract
// with programs consuming `e2e-step list --json`.
type TestInfo struct {
	Name         string `json:"name"`
	Deployer     string `json:"deployer"`
	DeployerType string `json:"deployerType"`
	Workload     string `json:"workload"`
	PVCSpec      string `json:"pvcSpec"`
	Namespace    string `json:"namespace"`
}

// TestList is the document printed by `e2e-step list --json`.
type TestList struct {
	Tests []TestInfo `json:"tests"`
}

// ListTests returns the tests in the configuration, in configuration order.
// It reads only the configuration and does not access the clusters.
func ListTests(cfg *config.Config) TestList {
	deployerSpecs := config.DeployersMap(cfg)
	list := TestList{Tests: make([]TestInfo, 0, len(cfg.Tests))}

	for _, tc := range cfg.Tests {
		name := tc.ContextName()
		list.Tests = append(list.Tests, TestInfo{
			Name:         name,
			Deployer:     tc.Deployer,
			DeployerType: deployerSpecs[tc.Deployer].Type,
			Workload:     tc.Workload,
			PVCSpec:      tc.PVCSpec,
			Namespace:    cfg.NamespacePrefix + name,
		})
	}

	return list
}

// FindTest returns the configured test with the given context name.
func FindTest(cfg *config.Config, name string) (config.Test, error) {
	names := make([]string, 0, len(cfg.Tests))

	for _, tc := range cfg.Tests {
		if tc.ContextName() == name {
			return tc, nil
		}

		names = append(names, tc.ContextName())
	}

	return config.Test{}, fmt.Errorf("test %q not found in configuration (available %q)", name, names)
}

// NewTestContext creates the test context for the named test, in the same way
// TestDR creates it for every configured test.
func NewTestContext(parent types.Context, name string) (*test.Context, error) {
	cfg := parent.Config()

	tc, err := FindTest(cfg, name)
	if err != nil {
		return nil, err
	}

	pvcSpec, ok := config.PVCSpecsMap(cfg)[tc.PVCSpec]
	if !ok {
		return nil, fmt.Errorf("unknown pvcSpec %q for test %q", tc.PVCSpec, name)
	}

	workload, err := workloads.New(tc.Workload, cfg.Repo.Branch, pvcSpec)
	if err != nil {
		return nil, err
	}

	deployerSpec, ok := config.DeployersMap(cfg)[tc.Deployer]
	if !ok {
		return nil, fmt.Errorf("unknown deployer %q for test %q", tc.Deployer, name)
	}

	deployer, err := deployers.New(deployerSpec)
	if err != nil {
		return nil, err
	}

	ctx := test.NewContext(parent, tc.ContextName(), workload, deployer)

	return &ctx, nil
}
