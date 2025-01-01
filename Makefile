GO      ?= go
GIT		?= git
PATH	?= PATH
PWD    ?= $(shell pwd)
GO_BUILD := $(GO) build
GO_INSTALL := $(GO) install
ENV_VARS := LD_LIBRARY_PATH="$(PWD)/cdeps/sysfail/build/src" \
            CGO_LDFLAGS="-L$(PWD)/cdeps/sysfail/build/src -lsysfail" \
            CGO_CFLAGS="-I$(PWD)/cdeps/sysfail/include"
.PHONY: build install test acceptance goreman clean
build: sysfail
	$(ENV_VARS) $(GO_BUILD) -v ./cmd/...

install_with_failpoints: gofail sysfail
	$(shell $(GO) env GOPATH)/bin/gofail enable ./oracle
	$(ENV_VARS)  $(GO_INSTALL) -v ./cmd/...

install: sysfail
	$(ENV_VARS)  $(GO_INSTALL) -v ./cmd/...

goreman:
	@$(GO_INSTALL) github.com/mohanr-rubrik/goreman@latest

getaddrinfo:
	gcc -D_GNU_SOURCE -shared -fPIC -g -o $(PWD)/getaddrinfo.so acceptance/cutils/getaddrinfo.c -ldl

gofail:
	@$(GO_INSTALL) go.etcd.io/gofail@v0.1.0

sysfail:
	cd cdeps/sysfail && \
            mkdir -p build && \
            cd build && \
            cmake .. && \
            make -j4;

# Run these tests serially to avoid port conflicts.
acceptance: install_with_failpoints goreman getaddrinfo sysfail
	PATH=$(shell $(GO) env GOPATH)/bin:$(PATH) PROXY_AWARE_RESOLVER=$(PWD)/getaddrinfo.so $(ENV_VARS)  $(GO) test -p 1 -v ./acceptance/... --tags=acceptance --timeout 60m
	$(shell $(GO) env GOPATH)/bin/gofail disable ./oracle
	./acceptance/run_upgrade_test.sh b9.1 $(shell $(GIT) rev-parse --abbrev-ref HEAD)

test: sysfail
	$(ENV_VARS) go test -v $(go list ./... | grep -v -e github.com/rubrikinc/kronos/cdeps/sysfail/test)

clean:
	rm -f goreman kronos
