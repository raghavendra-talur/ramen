# SPDX-FileCopyrightText: The RamenDR authors
# SPDX-License-Identifier: Apache-2.0

TOOLS_MOD := tools.mod
GO_TOOL := go tool -modfile=$(TOOLS_MOD)

.PHONY: build test lint clean

build:
	$(GO_TOOL) mage build

test:
	$(GO_TOOL) mage test

lint:
	$(GO_TOOL) mage lint

clean:
	$(GO_TOOL) mage clean
