PROTOC      ?= protoc

PROTO_FILES := $(wildcard *.proto)

generate:
	@$(PROTOC) -I ../vendor/github.com/gogo/protobuf/gogoproto -I . --go_out . $(PROTO_FILES)

clean:
	rm -f *.pb.go