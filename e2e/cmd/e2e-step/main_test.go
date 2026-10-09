// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

// Disabling testpackage linter to test the command flag parsing.
//
//nolint:testpackage
package main

import (
	"slices"
	"testing"
)

func TestParseFlagsDefaults(t *testing.T) {
	opts, positional, err := parseFlags("list", nil, commands["list"].flags)
	if err != nil {
		t.Fatal(err)
	}

	if opts.configFile != "config.yaml" || opts.logFile != "e2e-step.log" || opts.json {
		t.Errorf("unexpected defaults: %+v", opts)
	}

	if len(positional) != 0 {
		t.Errorf("unexpected positional arguments: %q", positional)
	}
}

func TestParseFlagsRun(t *testing.T) {
	args := []string{"--config", "c.yaml", "--test", "disapp-deploy-rbd", "--action", "failover", "--logfile", "l.log"}

	opts, _, err := parseFlags("run", args, commands["run"].flags)
	if err != nil {
		t.Fatal(err)
	}

	expected := options{configFile: "c.yaml", logFile: "l.log", test: "disapp-deploy-rbd", action: "failover"}
	if *opts != expected {
		t.Errorf("options %+v, expected %+v", *opts, expected)
	}
}

func TestParseFlagsAfterPositional(t *testing.T) {
	args := []string{"--logfile", "l.log", "delete", "--config", "c.yaml"}

	opts, positional, err := parseFlags("channel", args, commands["channel"].flags)
	if err != nil {
		t.Fatal(err)
	}

	if !slices.Equal(positional, []string{"delete"}) {
		t.Errorf("positional %q, expected [\"delete\"]", positional)
	}

	if opts.configFile != "c.yaml" || opts.logFile != "l.log" {
		t.Errorf("unexpected options: %+v", opts)
	}
}

func TestParseFlagsUnknownFlag(t *testing.T) {
	// The run command has no --json flag.
	if _, _, err := parseFlags("run", []string{"--json"}, commands["run"].flags); err == nil {
		t.Error("expected an error for an unknown flag")
	}
}
