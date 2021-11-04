GO      ?= go
GO_BUILD := GOBIN='$(abspath bin)' $(GO) build
GO_INSTALL := GOBIN='$(abspath bin)' $(GO) install
.PHONY: build install test acceptance goreman clean
build:
	@$(GO_BUILD) -v ./cmd/...

install:
	@$(GO_INSTALL) -v ./cmd/...

goreman:
	@$(GO) get -v github.com/mattn/goreman@v0.1.1

# Run these tests serially to avoid port conflicts.
acceptance: build goreman
	$(GO) test -p 1 -v ./acceptance/... --tags=acceptance --timeout 30m

test:
	$(GO) test -v ./...

clean:
	rm -f goreman kronos
