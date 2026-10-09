# SPDX-FileCopyrightText: The RamenDR authors
# SPDX-License-Identifier: Apache-2.0

.PHONY: test e2e-step

ramen-e2e:
	go test -c -o $@

e2e-step:
	go build -o e2e-step ./cmd/e2e-step

test:
	go test -v ./config ./recipes
