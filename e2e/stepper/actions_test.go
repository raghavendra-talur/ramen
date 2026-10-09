// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package stepper_test

import (
	"testing"
	"time"

	"github.com/ramendr/ramen/e2e/stepper"
	"github.com/ramendr/ramen/e2e/util"
)

func TestParseAction(t *testing.T) {
	names := []string{"deploy", "enable", "failover", "relocate", "disable", "undeploy", "purge"}

	for _, name := range names {
		action, err := stepper.ParseAction(name)
		if err != nil {
			t.Fatalf("failed to parse %q: %s", name, err)
		}

		if string(action) != name {
			t.Errorf("parsed %q as %q", name, action)
		}
	}

	for _, name := range []string{"", "Deploy", "failback", "deploy "} {
		if _, err := stepper.ParseAction(name); err == nil {
			t.Errorf("expected an error parsing %q", name)
		}
	}
}

func TestActionsOrder(t *testing.T) {
	expected := []stepper.Action{
		stepper.ActionDeploy,
		stepper.ActionEnable,
		stepper.ActionFailover,
		stepper.ActionRelocate,
		stepper.ActionDisable,
		stepper.ActionUndeploy,
		stepper.ActionPurge,
	}

	actions := stepper.Actions()
	if len(actions) != len(expected) {
		t.Fatalf("actions %q, expected %q", actions, expected)
	}

	for i := range expected {
		if actions[i] != expected[i] {
			t.Fatalf("actions %q, expected %q", actions, expected)
		}
	}
}

func TestActionTimeouts(t *testing.T) {
	expected := map[stepper.Action]time.Duration{
		stepper.ActionDeploy:   util.DeployTimeout,
		stepper.ActionEnable:   util.EnableTimeout,
		stepper.ActionFailover: util.FailoverTimeout,
		stepper.ActionRelocate: util.RelocateTimeout,
		stepper.ActionDisable:  util.DisableTimeout,
		stepper.ActionUndeploy: util.UndeployTimeout,
		stepper.ActionPurge:    util.PurgeTimeout,
	}

	for action, timeout := range expected {
		if action.Timeout() != timeout {
			t.Errorf("action %q timeout %v, expected %v", action, action.Timeout(), timeout)
		}
	}
}
