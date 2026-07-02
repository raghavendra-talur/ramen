ROOT := ..

.PHONY: simtest manager assets

simtest: manager assets
	go test ./tests/... -v -timeout 90m

manager:
	$(MAKE) -C $(ROOT) build

assets:
	cd $(ROOT) && hack/install-setup-envtest.sh
