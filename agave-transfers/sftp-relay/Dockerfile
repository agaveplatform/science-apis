FROM agaveplatform/grpc-go:1.13

COPY . $GOPATH/src/github.com/agaveplatform/agave-goSFTP

WORKDIR $GOPATH/src/github.com/agaveplatform/agave-goSFTP

RUN make genproto; make build

FROM alpine:latest
RUN apk --no-cache add ca-certificates openssl curl bash

LABEL maintainer="Agave Platform<help@agaveplatform.org>"
LABEL protoc_version="${PROTOC_VERSION}"
LABEL language="golang:1.11"
LABEL agave_platform=2.0
LABEL tooling=transfer,sftp,worker,put

WORKDIR /go/
COPY --from=0 /go/src/github.com/agaveplatform/agave-goSFTP/release/fileServer /usr/local/bin/agave-sftp

RUN \
    chmod +x /usr/local/bin/agave-sftp

ENTRYPOINT ["agave-sftp"]

CMD ["--help"]
