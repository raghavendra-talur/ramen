// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"strings"
)

// enabledReconcilers is the set of reconcilers to run, from
// RAMEN_RECONCILERS. nil means every reconciler runs.
var enabledReconcilers = parseReconcilers(os.Getenv("RAMEN_RECONCILERS"))

// parseReconcilers parses a comma-separated reconciler list. An empty list
// returns nil, meaning no filtering.
func parseReconcilers(val string) map[string]bool {
	if val == "" {
		return nil
	}

	enabled := make(map[string]bool)

	for _, name := range strings.Split(val, ",") {
		enabled[strings.TrimSpace(name)] = true
	}

	return enabled
}

func reconcilerEnabled(name string) bool {
	if enabledReconcilers == nil {
		return true
	}

	return enabledReconcilers[name]
}
