// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package stepper

import (
	"fmt"
	"time"

	"github.com/ramendr/ramen/e2e/dractions"
	"github.com/ramendr/ramen/e2e/test"
	"github.com/ramendr/ramen/e2e/types"
	"github.com/ramendr/ramen/e2e/util"
	"github.com/ramendr/ramen/e2e/validate"
)

// Action is a single step in the DR lifecycle of a test.
type Action string

const (
	ActionDeploy   = Action("deploy")
	ActionEnable   = Action("enable")
	ActionFailover = Action("failover")
	ActionRelocate = Action("relocate")
	ActionDisable  = Action("disable")
	ActionUndeploy = Action("undeploy")
	ActionPurge    = Action("purge")
)

// actionSpec describes how to run an action. The functions and timeouts are
// the same ones used by the test.Context methods run by TestDR.
type actionSpec struct {
	description string
	timeout     time.Duration
	run         func(types.TestContext) error
}

var actionSpecs = map[Action]actionSpec{
	ActionDeploy: {
		description: "deploy workload",
		timeout:     util.DeployTimeout,
		run:         func(ctx types.TestContext) error { return ctx.Deployer().Deploy(ctx) },
	},
	ActionEnable: {
		description: "enable protection for workload",
		timeout:     util.EnableTimeout,
		run:         dractions.EnableProtection,
	},
	ActionFailover: {
		description: "failover workload",
		timeout:     util.FailoverTimeout,
		run:         dractions.Failover,
	},
	ActionRelocate: {
		description: "relocate workload",
		timeout:     util.RelocateTimeout,
		run:         dractions.Relocate,
	},
	ActionDisable: {
		description: "disable protection for workload",
		timeout:     util.DisableTimeout,
		run:         dractions.DisableProtection,
	},
	ActionUndeploy: {
		description: "undeploy workload",
		timeout:     util.UndeployTimeout,
		run:         func(ctx types.TestContext) error { return ctx.Deployer().Undeploy(ctx) },
	},
	ActionPurge: {
		description: "purge workload",
		timeout:     util.PurgeTimeout,
		run:         dractions.Purge,
	},
}

// Actions returns all actions in lifecycle order.
func Actions() []Action {
	return []Action{
		ActionDeploy,
		ActionEnable,
		ActionFailover,
		ActionRelocate,
		ActionDisable,
		ActionUndeploy,
		ActionPurge,
	}
}

// ParseAction returns the action named s.
func ParseAction(s string) (Action, error) {
	action := Action(s)
	if _, ok := actionSpecs[action]; !ok {
		return "", fmt.Errorf("unknown action %q (choose from %q)", s, Actions())
	}

	return action, nil
}

// Timeout returns the time limit for running the action.
func (a Action) Timeout() time.Duration {
	return actionSpecs[a].timeout
}

// RunAction runs a single action for the test, as TestDR would run it. Before
// the action it validates the test configuration and the test context, like
// TestDR and runTestFlow do. Before deploy it ensures that the channel exists.
// The channel is never deleted; use util.EnsureChannelDeleted when done.
func RunAction(ctx *test.Context, action Action) error {
	spec, ok := actionSpecs[action]
	if !ok {
		return fmt.Errorf("unknown action %q (choose from %q)", action, Actions())
	}

	if err := validate.TestConfig(ctx); err != nil {
		return err
	}

	// runTestFlow skips the test in this case; we cannot run the action.
	if err := ctx.Validate(); err != nil {
		return fmt.Errorf("test %q would be skipped by the suite: %w", ctx.Name(), err)
	}

	if action == ActionDeploy {
		if err := util.EnsureChannel(ctx); err != nil {
			return fmt.Errorf("failed to ensure channel: %w", err)
		}
	}

	timedCtx, cancel := ctx.WithTimeout(spec.timeout)
	defer cancel()

	if err := spec.run(timedCtx); err != nil {
		return fmt.Errorf("failed to %s: %w", spec.description, err)
	}

	return nil
}
