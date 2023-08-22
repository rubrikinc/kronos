GO      ?= go
PATH	?= PATH
GO_BUILD   := $(GO) build
GO_INSTALL := $(GO) install
IDL_DIR    := pb
.PHONY: build install test acceptance goreman clean

idl_clean:
	$(MAKE) -C $(IDL_DIR) clean

idl_generate:
	$(MAKE) -C $(IDL_DIR) generate

build: idl_generate
	@$(GO_BUILD) -v ./cmd/...

install: idl_generate
	@$(GO_INSTALL) -v ./cmd/...

goreman:
	@$(GO_INSTALL) github.com/mattn/goreman@v0.1.1

# Run these tests serially to avoid port conflicts.
acceptance: install goreman
	PATH=$(shell $(GO) env GOPATH)/bin:$(PATH) $(GO) test -p 1 -v ./acceptance/... --tags=acceptance --timeout 30m

test: idl_generate
	$(GO) test -v ./...

clean: idl_clean
	rm -f goreman kronos
