FROM agaveplatform/grpc-go:1.14

LABEL maintainer="Agave Platform<help@agaveplatform.org>"
LABEL protoc_version="${PROTOC_VERSION}"
LABEL language="golang:1.14"
LABEL agave_platform=2.0
LABEL tooling=transfer,sftp,worker,put

COPY . $GOPATH/src/github.com/agaveplatform/science-apis/agave-transfers/sftp-relay
COPY tmp/* $GOPATH/src/github.com/agaveplatform/science-apis/agave-protos/proto/
COPY ./scripts/docker-entrypoint.sh /docker-entrypoint.sh

WORKDIR $GOPATH/src/github.com/agaveplatform/science-apis/agave-transfers/sftp-relay

RUN make default && \
	make install && \
	make client && \
	make install-client

ENV AGAVE_GRPCSERVICE "localhost:50051"

# Commenting out to avoid insanely long buid times every time
#FROM alpine:latest
#RUN apk --no-cache add ca-certificates openssl curl bash
#
#LABEL maintainer="Agave Platform<help@agaveplatform.org>"
#LABEL protoc_version="${PROTOC_VERSION}"
#LABEL language="golang:1.13"
#LABEL agave_platform=2.0
#LABEL tooling=transfer,sftp,worker,put
#
#WORKDIR /go/
#COPY --from=0 /go/src/github.com/agaveplatform/science-apis/agave-transfers/sftp-relay/release/sftp-relay-linux-amd64 /usr/local/bin/sftp-relay
#
#RUN \
#    chmod +x /usr/local/bin/sftp-relay

EXPOSE 50051 9092
ENTRYPOINT ["/docker-entrypoint.sh"]
