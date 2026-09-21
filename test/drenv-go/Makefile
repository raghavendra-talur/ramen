# SPDX-FileCopyrightText: The RamenDR authors
# SPDX-License-Identifier: Apache-2.0

TOOLS_MOD := tools.mod
GO_TOOL := go tool -modfile=$(TOOLS_MOD)

.PHONY: build test lint clean render

build:
	$(GO_TOOL) mage build

# render pre-renders the addon kustomizations (fetching upstream bases) into
# internal/addon/manifests, which are embedded into the binary. Needs network.
render:
	$(GO_TOOL) mage render

test:
	$(GO_TOOL) mage test

lint:
	$(GO_TOOL) mage lint

clean:
	$(GO_TOOL) mage clean
