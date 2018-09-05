# Ensure we have an unambiguous GOPATH.
export GOPATH := $(realpath ../../../..)
#                           ^  ^  ^  ^~ GOPATH
#                           |  |  |~ GOPATH/src
#                           |  |~ GOPATH/src/github.com
#                           |~ GOPATH/src/github.com/scaledata

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

acceptance: build goreman
	$(GO) test -v ./acceptance/... --tags=acceptance

test:
	$(GO) test -v ./...

clean:
	rm -f goreman kronos
