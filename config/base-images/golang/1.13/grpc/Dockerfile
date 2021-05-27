#############################################################
#
# golang grpc base image
# Tag: agaveplatform/grpc-go:1.13
#
# Build image for grpc applications written in golang.
#
# The image can be built by issuing this command from the
# directory containing this file:
#
#   docker build -t agaveplatform/grpc-go:1.13 .
#
# https://github.com/agaveplatform/docker-golang
# https://agaveplatform.org
#
#############################################################

FROM agaveplatform/golang:1.13

ENV GRPC_VERSION 1.24.0
ENV PROTOBUF_VERSION 3.10.0
ENV PROTOC_VERSION 1.3.2

LABEL maintainer="Agave Platform<help@agaveplatform.org>"
LABEL language="golang:${GO_VERSION}"
LABEL grpc_version="${GRPC_VERSION}"
LABEL proto_version="${PROTOBUF_VERSION}"

# install protobuf library
RUN mkdir -p /tmp/protoc && \
    cd /tmp/protoc && \
	curl -L -o protoc-${PROTOBUF_VERSION}-linux-x86_64.zip https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip && \
    unzip protoc-${PROTOBUF_VERSION}-linux-x86_64.zip && \
    cp /tmp/protoc/bin/protoc /usr/local/bin && \
    cp -R /tmp/protoc/include/* /usr/local/include && \
    chmod +rx /usr/local/bin/protoc && \
    cd /tmp && \
    rm -r /tmp/protoc ; \
    \
	# Get the grpc source from GitHub \
	mkdir -p /tmp/grpc-go && \
    curl -L https://github.com/grpc/grpc-go/archive/v${GRPC_VERSION}.zip > /tmp/grpc-go/grpc-go.zip && \
    cd /tmp/grpc-go && \
    unzip grpc-go.zip && \
    mkdir -p /go/src/google.golang.org/grpc/ && \
    cp -r /tmp/grpc-go/grpc-go-${GRPC_VERSION}/* /go/src/google.golang.org/grpc/ ; \
	\
	# Install protoc-gen-go \
	mkdir -p /tmp/protobuf && \
    cd /tmp/protobuf && \
    curl -L -o protobuf.zip https://github.com/golang/protobuf/archive/v${PROTOC_VERSION}.zip && \
    unzip protobuf.zip && \
    mkdir -p /go/src/github.com/golang/protobuf && \
    cp -r protobuf-${PROTOC_VERSION}/* /go/src/github.com/golang/protobuf/ && \
    go install github.com/golang/protobuf/protoc-gen-go
