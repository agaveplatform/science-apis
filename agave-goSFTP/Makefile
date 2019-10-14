SHELL := /bin/bash
PLATFORM := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)
GOPATH := $(shell go env GOPATH)
PROJECT_DIR := $(shell pwd)
PROJECT_PROTO_PKG := github.com/agaveplatform/Go-SFTP/agaveSFTP/pkg/client
PROJECT_PROTO_DIR := proto
PROJECT_NAME := "agavesftp"
PROJECT_PROTOS := $(wildcard *.proto)
PROJECT_GEN_PB := $(addprefix pkg/sftp/,$(notdir $(patsubst %.proto,%.pb.go,$(PROJECT_PROTOS))))

.DEFAULT_GOAL := default

firstGOPATH := $(firstword $(subst :, ,$(GOPATH)) <invalid_path>)

.PHONY: help
help: ## Print help for targets with comments.
	@echo $(PROJECT_GEN_PB)
	@echo "Usage:"
	@echo "  make [target...]"
	@echo ""
	@echo $(PROJECT_GEN_PB)
	@echo $(addprefix pkg/sftp/,$(notdir $(patsubst %.proto,%.pb.go,$(PROJECT_PROTOS))))
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


$(PROJECT_GEN_PB):
	mkdir -p $(PROJECT_PROTO_DIR)
	#protoc -I ./proto ./proto/sftp.proto --go_out=plugins=grpc:proto/go
	mkdir -p $(PROJECT_PROTO_DIR)
	protoc \
	-I./proto \
	-I./vendor/$(PROJECT_PROTO_REPO)/proto \
	--go_out=plugins=grpc:$(PROJECT_PROTO_DIR) \
	$(PROJECT_PROTOS)
	mv $(PROJECT_PROTO_DIR)/$(PROJECT_PROTO_PKG)/*.pb.go $(dir $@)

.PHONY: all
all: genproto test build image


.PHONY: default
default: genproto test build

	PROJECT_PROTOS := $(wildcard proto/*.proto)
	
	echo $(PROJECT_PROTOS)

	PROJECT_GEN_PB := $(addprefix pkg/sftp/,$(notdir $(patsubst %.proto,%.pb.go,$(PROJECT_PROTOS))))
	echo $(PROJECT_GEN_PB)

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
	echo Starting Image process
	cd $(PROJECT_DIR); docker build --rm=true -t $(PROJECT_NAME):latest .
	echo Done with image
	

# Build protobuf
.PHONY: genproto
genproto: 
	
	mkdir -p $(PROJECT_PROTO_DIR)
	protoc -I ./proto ./proto/sftp.proto --go_out=plugins=grpc:proto/go
	protoc -I ./proto --java_out=./proto/java/ ./proto/sftp.proto
	mkdir -p $(PROJECT_DIR)/cmd/proto
	cp $(PROJECT_DIR)/proto/go/*.go $(PROJECT_DIR)/cmd/proto/


.PHONY: build 
build:
	./scripts/go_build.sh
	

.PHONY: clean
clean:
	go clean -i -cache
	rm -rf release/*
	rm -rf proto/go/*
	rm -rf proto/java/*
	rm -rf cmd/proto/*.go

# disallow any parallelism (-j) for Make. This is necessary since some
# commands during the build process create temporary files that collide
# under parallel conditions.
.NOTPARALLEL: