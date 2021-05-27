SHELL := /bin/bash
PLATFORM := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)
GOPATH := $(shell go env GOPATH)
PWD := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
PROJECT_PROTO_SRC_DIR := $(abspath $(PWD)/../)/proto/
PROJECT_PROTO_OUT_DIR := pkg/sftpproto
PROJECT_NAME := "protos-go"

.DEFAULT_GOAL := default

firstGOPATH := $(firstword $(subst :, ,$(GOPATH)) <invalid_path>)

PROJECT_PROTOS := $(wildcard $(PROJECT_PROTO_SRC_DIR)*.proto)
PROJECT_GEN_PB := $(addprefix $(PROJECT_PROTO_SRC_DIR)/,$(PROJECT_PROTOS))

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
	"make clean" "Run all unit tests." \
	"make default" "default goal. Runs genproto" \
	"make genproto" "Builds go protobuf stubs" \

.PHONY:	default
default: genproto


$(PROJECT_GEN_PB):
	@mkdir -p $(PROJECT_PROTO_OUT_DIR)

	protoc \
	-I$(PROJECT_PROTO_SRC_DIR) \
	--go_out=plugins=grpc:$(PROJECT_PROTO_OUT_DIR) \
	$(PROJECT_PROTOS)

# Build protobuf stubs
.PHONY:	genproto
genproto:$(PROJECT_GEN_PB)

.PHONY:	clean
clean:
	rm -rf $(PROJECT_PROTO_OUT_DIR)

# disallow any parallelism (-j) for Make. This is necessary since some
# commands during the build process create temporary files that collide
# under parallel conditions.
.NOTPARALLEL:
