# SPDX-FileCopyrightText: The RamenDR authors
# SPDX-License-Identifier: Apache-2.0

TOOLS_MOD := tools.mod
GO_TOOL := go tool -modfile=$(TOOLS_MOD)

.PHONY: build test lint clean render parity parity-update

build:
	$(GO_TOOL) mage build

# render pre-renders the addon kustomizations (fetching upstream bases) into
# internal/addon/manifests, which are embedded into the binary. Needs network.
render:
	$(GO_TOOL) mage render

# parity fails when the Python drenv project has drifted from the locked
# baseline (parity.lock), naming exactly which mirrored files changed. Pure file
# hashing — no network. See parity.go for the workflow.
parity:
	$(GO_TOOL) mage parity

# parity-update re-baselines parity.lock. Run it ONLY after reconciling drenv-go
# with the upstream change it reports.
parity-update:
	$(GO_TOOL) mage parityupdate

test:
	$(GO_TOOL) mage test

lint:
	$(GO_TOOL) mage lint

clean:
	$(GO_TOOL) mage clean
