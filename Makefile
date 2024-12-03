GO      ?= go
GIT		?= git
PATH	?= PATH
PWD    ?= $(shell pwd)
GO_BUILD := $(GO) build
GO_INSTALL := $(GO) install
.PHONY: build install test acceptance goreman clean
build:
	@$(GO_BUILD) -v ./cmd/...

install_with_failpoints: gofail
	$(shell $(GO) env GOPATH)/bin/gofail enable ./oracle
	@$(GO_INSTALL) -v ./cmd/...

install:
	@$(GO_INSTALL) -v ./cmd/...

goreman:
	@$(GO_INSTALL) github.com/mohanr-rubrik/goreman@latest

getaddrinfo:
	gcc -D_GNU_SOURCE -shared -fPIC -o $(PWD)/getaddrinfo.so acceptance/cutils/getaddrinfo.c -ldl

gofail:
	@$(GO_INSTALL) go.etcd.io/gofail@v0.1.0

# Run these tests serially to avoid port conflicts.
acceptance: install_with_failpoints goreman getaddrinfo
	PATH=$(shell $(GO) env GOPATH)/bin:$(PATH) PROXY_AWARE_RESOLVER=$(PWD)/getaddrinfo.so $(GO) test -p 1 -v ./acceptance/... --tags=acceptance --timeout 60m
	$(shell $(GO) env GOPATH)/bin/gofail disable ./oracle
	./acceptance/run_upgrade_test.sh b9.1 $(shell $(GIT) rev-parse --abbrev-ref HEAD)

test:
	$(GO) test -v ./...

clean:
	rm -f goreman kronos
