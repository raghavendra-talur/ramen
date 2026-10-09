// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package app

import (
	"github.com/ramendr/ramen/e2e/config"
	"github.com/ramendr/ramen/e2e/deployers"
	"github.com/ramendr/ramen/e2e/workloads"
)

// ReadConfig reads and validates the e2e configuration file, allowing all
// registered deployer types and workloads. This is how the e2e suite and the
// e2e-step command read their configuration.
func ReadConfig(configFile string) (*config.Config, error) {
	options := config.Options{
		Deployers: deployers.AvailableTypes(),
		Workloads: workloads.AvailableNames(),
	}

	return config.ReadConfig(configFile, options)
}
