GO      ?= go
PATH	?= PATH
GO_BUILD := $(GO) build
GO_INSTALL := $(GO) install
.PHONY: build install test acceptance goreman clean
build:
	@$(GO_BUILD) -v ./cmd/...

install:
	@$(GO_INSTALL) -v ./cmd/...

goreman:
	@$(GO_INSTALL) github.com/mattn/goreman@v0.1.1

# Run these tests serially to avoid port conflicts.
acceptance: install goreman
	PATH=$(shell $(GO) env GOPATH)/bin:$(PATH) $(GO) test -p 1 -v ./acceptance/... --tags=acceptance --timeout 30m

test:
	$(GO) test -v ./...

clean:
	rm -f goreman kronos
