SHELL := /bin/bash
PLATFORM := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)
GOPATH := $(shell go env GOPATH)
PROJECT_PROTO_DIR := pkg/client
PROJECT_NAME := "agavesftp"

.DEFAULT_GOAL := default

firstGOPATH := $(firstword $(subst :, ,$(GOPATH)) <invalid_path>)

.PHONY:	help
help: ## Print help for targets with comments.
	@echo "Usage:"
	@echo "  make [target...]"
	@echo ""
	@echo "Useful commands:"
	@grep -Eh '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(cyan)%-30s$(term-reset) %s\n", $$1, $$2}'
	@echo ""
	@echo "Useful variables:"
	@awk 'BEGIN { FS = ":=" } /^## /{x = substr($$0, 4); getline; if (NF >= 2) printf "  $(cyan)%-30s$(term-reset) %s\n", $$1, x}' $(MAKEFILE_LIST) | sort
	@echo ""
	@echo "Typical usage:"
	@printf "  $(cyan)%s$(term-reset)\n    %s\n\n" \
	"make get-deps" "Run all unit tests." \
	"make genproto" "Re-build protobuf libraries" \

.PHONY:	default
	default: genproto

	PROJECT_PROTOS := $(wildcard proto/*.proto)
	PROJECT_GEN_PB := $(addprefix pkg/sftp/,$(notdir $(patsubst %.proto,%.pb.go,$(PROJECT_PROTOS))))

$(PROJECT_GEN_PB):
	mkdir -p $(PROJECT_PROTO_DIR)
	protoc \
	-I./proto \
	--go_out=plugins=grpc:$(PROJECT_PROTO_DIR) \
	$(PROJECT_PROTOS)
	mv $(PROJECT_PROTO_DIR)/$(PROJECT_PROTO_PKG)/*.pb.go $(dir $@)

# Build protobuf stubs
.PHONY:	genproto
genproto:	$(PROJECT_GEN_PB)

.PHONY:	clean
clean:
	go clean -i -cache
	rm -rf release/*
	rm -rf $(PROJECT_PROTO_DIR)

# disallow any parallelism (-j) for Make. This is necessary since some
# commands during the build process create temporary files that collide
# under parallel conditions.
.NOTPARALLEL:
