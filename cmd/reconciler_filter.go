// SPDX-FileCopyrightText: The RamenDR authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"os"
	"strings"
)

var enabledReconcilers map[string]bool

func init() {
	val := os.Getenv("RAMEN_RECONCILERS")
	if val == "" {
		return
	}

	enabledReconcilers = make(map[string]bool)

	for _, name := range strings.Split(val, ",") {
		enabledReconcilers[strings.TrimSpace(name)] = true
	}
}

func reconcilerEnabled(name string) bool {
	if enabledReconcilers == nil {
		return true
	}

	return enabledReconcilers[name]
}
