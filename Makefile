GO      ?= go
PATH	?= PATH
PWD    ?= $(shell pwd)
GO_BUILD := $(GO) build
GO_INSTALL := $(GO) install
.PHONY: build install test acceptance goreman clean
build:
	@$(GO_BUILD) -v ./cmd/...

install:
	@$(GO_INSTALL) -v ./cmd/...

goreman:
	@$(GO_INSTALL) github.com/mattn/goreman@v0.3.15

getaddrinfo:
	gcc -D_GNU_SOURCE -shared -fPIC -o $(PWD)/getaddrinfo.so acceptance/cutils/getaddrinfo.c -ldl

# Run these tests serially to avoid port conflicts.
acceptance: install goreman getaddrinfo
	PATH=$(shell $(GO) env GOPATH)/bin:$(PATH) PROXY_AWARE_RESOLVER=$(PWD)/getaddrinfo.so $(GO) test -p 1 -v ./acceptance/... --tags=acceptance --timeout 30m

test:
	$(GO) test -v ./...

clean:
	rm -f goreman kronos
