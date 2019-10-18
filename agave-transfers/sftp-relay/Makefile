SHELL := /bin/bash
PLATFORM := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)
GOPATH := $(shell go env GOPATH)
PROJECT_DIR := $(shell pwd)
PWD := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))
PROJECT_PROTO_SRC_DIR := $(abspath $(PWD)/../../agave-protos)/proto/
PROJECT_PROTO_OUT_DIR := pkg/sftpproto
PROJECT_NAME := "sftp-relay"
#PROJECT_PROTOS := $(wildcard $(PROJECT_PROTO_SRC_DIR)/*.proto)
#PROJECT_GEN_PB := $(addprefix pkg/sftprelay/,$(notdir $(patsubst %.proto,%.pb.go,$(PROJECT_PROTOS))))

.DEFAULT_GOAL := default

firstGOPATH := $(firstword $(subst :, ,$(GOPATH)) <invalid_path>)

.PHONY: help
help: ## Print help for targets with comments.
	@echo $(PROJECT_GEN_PB)
	@echo "Usage:"
	@echo "  make [target...]"
	@echo ""
	@echo $(PROJECT_GEN_PB)
	@echo $(addprefix pkg/sftprelay/,$(notdir $(patsubst %.proto,%.pb.go,$(PROJECT_PROTOS))))
	@echo ""
	@echo "Useful commands:"
	@grep -Eh '^[a-zA-Z._-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  $(cyan)%-30s$(term-reset) %s\n", $$1, $$2}'
	@echo ""
	@echo "Useful variables:"
	@awk 'BEGIN { FS = ":=" } /^## /{x = substr($$0, 4); getline; if (NF >= 2) printf "  $(cyan)%-30s$(term-reset) %s\n", $$1, x}' $(MAKEFILE_LIST) | sort
	@echo ""
	@echo "Typical usage:"
	@printf "  $(cyan)%s$(term-reset)\n    %s\n\n" \
	"make test" "Run all unit tests." \
	"make genproto" "Re-build protobuf libraries" \
	"make build" "Build $(PROJECT_NAME) binaries" \
	"make validate" "Run linter and code quality tool against Go source." \
	"make install" "Install $(PROJECT_NAME) binary to local GOBIN directory" \
	"make image" "Build Docker images for the $(PROJECT_NAME)" \
	"make clean" "Clear protobuf libraries, build artifacts, and application binary"


PROJECT_PROTOS := $(wildcard $(PROJECT_PROTO_SRC_DIR)*.proto)
PROJECT_GEN_PB := $(addprefix pkg/sftprelay/,$(notdir $(patsubst %.proto,%.pb.go,$(PROJECT_PROTOS))))

$(PROJECT_GEN_PB):
	@mkdir -p $(PROJECT_PROTO_OUT_DIR)
	protoc -I $(PROJECT_PROTO_SRC_DIR) --go_out=plugins=grpc:$(PROJECT_PROTO_OUT_DIR) $(PROJECT_PROTOS)
	@#protoc -I ./proto --java_out=./proto/java/ ./proto/sftp.proto

.PHONY: all
all: genproto test build image

.PHONY: default
default: genproto deps build

.PHONY: test
test:
	./scripts/test.sh

# Install the application binary into $GOBIN
.PHONY: install
install:
	cp release/client-$(PLATFORM)-$(ARCH) $(firstGOPATH)/bin/$(PROJECT_NAME)-client
	cp release/$(PROJECT_NAME)-$(PLATFORM)-$(ARCH) $(firstGOPATH)/bin/$(PROJECT_NAME)


# Build the application into a Docker image
.PHONY: image
image:
	@echo Starting Image process
	cd $(PROJECT_DIR); docker build --rm=true -t $(PROJECT_NAME):latest .
	@echo Done with image

# Build protobuf
.PHONY: genproto
genproto: $(PROJECT_GEN_PB)

# Vendor all dependencies
.PHONY: deps
deps:
	if [[ ! -e vendor/modules.txt ]]; then go mod vendor; else go mod tidy; fi

.PHONY: build 
build:
	#BASE_CMD_NAME=client BUILD_PLATFORMS=$(PLATFORM) ./scripts/build.sh
	BASE_CMD_NAME=$(PROJECT_NAME) BUILD_PLATFORMS=$(PLATFORM) ./scripts/build.sh

.PHONY: clean
clean:
	@go clean -i -cache
	@rm -rf release/*
	rm -rf proto/go/*
	rm -rf proto/java/*
	rm -rf pkg/sftpproto/*

# disallow any parallelism (-j) for Make. This is necessary since some
# commands during the build process create temporary files that collide
# under parallel conditions.
.NOTPARALLEL: