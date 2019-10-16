SHELL := /bin/bash
PLATFORM := $(shell go env GOOS)
ARCH := $(shell go env GOARCH)
GOPATH := $(shell go env GOPATH)
PROJECT_PROTO_PKG := github.com/agaveplatform/Go-SFTP/agaveSFTP/pkg/client
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
	"make test" "Run all unit tests." \
	"make genproto" "Re-build protobuf libraries" \
	"make build" "Build $(PROJECT_NAME) binaries" \
	"make validate" "Run linter and code quality tool against Go source." \
	"make install" "Install $(PROJECT_NAME) binary to local GOBIN directory" \
	"make image" "Build Docker images for the $(PROJECT_NAME)" \
	"make clean" "Clear protobuf libraries, build artifacts, and application binary"

.PHONY:	default
	default: get-deps build test

	PROJECT_PROTOS := $(wildcard proto/*.proto)
	PROJECT_GEN_PB := $(addprefix pkg/sftp/,$(notdir $(patsubst %.proto,%.pb.go,$(PROJECT_PROTOS))))


$(PROJECT_GEN_PB):
	mkdir -p $(PROJECT_PROTO_DIR)
	protoc \
	-I./proto \
	-I./vendor/$(PROJECT_PROTO_REPO)/proto \
	--go_out=plugins=grpc:$(PROJECT_PROTO_DIR) \
	$(PROJECT_PROTOS)
	mv $(PROJECT_PROTO_DIR)/$(PROJECT_PROTO_PKG)/*.pb.go $(dir $@)

.PHONY:	default
	default: get-deps build test

.PHONY:	get-deps
	get-deps:
		dep ensure

.PHONY:	test
test:
	./scripts/test.sh

# Install the application binary into $GOBIN
.PHONY: install
install:
	cp release/client-$(PLATFORM)-$(ARCH) $(firstGOPATH)/bin/$(PROJECT_NAME)-client
	cp release/$(PROJECT_NAME)-$(PLATFORM)-$(ARCH) $(firstGOPATH)/bin/$(PROJECT_NAME)


.PHONY:	image
# Build the application into a Docker image
image:
	docker build --rm=true -t $(PROJECT_NAME):latest .

# Build protobuf stubs
.PHONY:	genproto
genproto:	$(PROJECT_GEN_PB)

.PHONY:	build
build: genproto
    # build binary with generated stubs
	BASE_CMD_NAME=client BUILD_PLATFORMS=$(PLATFORM) ./scripts/build.sh
	BASE_CMD_NAME=$(PROJECT_NAME) BUILD_PLATFORMS=$(PLATFORM) ./scripts/build.sh

.PHONY:	clean
clean:
	go clean -i -cache
	rm -rf release/*
	rm -rf $(PROJECT_PROTO_DIR)

# disallow any parallelism (-j) for Make. This is necessary since some
# commands during the build process create temporary files that collide
# under parallel conditions.
.NOTPARALLEL:
