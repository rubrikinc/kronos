# Ensure we have an unambiguous GOPATH.
export GOPATH := $(realpath ../../../..)
#                           ^  ^  ^  ^~ GOPATH
#                           |  |  |~ GOPATH/src
#                           |  |~ GOPATH/src/github.com
#                           |~ GOPATH/src/github.com/rubrikinc

GO      ?= go
# We install our vendored tools to a directory within this repository to avoid
# overwriting any user-installed binaries of the same name in the default GOBIN.
#
GO_BUILD := GOBIN='$(abspath bin)' $(GO) build
GO_INSTALL := GOBIN='$(abspath bin)' $(GO) install
.PHONY: build install test acceptance goreman clean
build:
	@$(GO_BUILD) -v ./cmd/...

install:
	@$(GO_INSTALL) -v ./cmd/...

goreman:
	@$(GO_BUILD) -v ./vendor/github.com/mattn/goreman

# Run these tests serially to avoid port conflicts.
acceptance: build goreman
	$(GO) test -p 1 -v ./acceptance/... --tags=acceptance --timeout 30m

test:
	$(GO) test -v ./...

clean:
	rm -f goreman kronos
