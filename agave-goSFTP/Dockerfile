FROM agaveplatform/grpc-go:1.11

COPY . $GOPATH/src/github.com/agaveplatform/go-SFTP

WORKDIR $GOPATH/src/github.com/agaveplatform/go-SFTP

FROM alpine:latest
RUN apk --no-cache add ca-certificates openssl curl bash

LABEL maintainer="Agave Platform<help@agaveplatform.org>"
LABEL protoc_version="${PROTOC_VERSION}"
LABEL language="golang:1.11"
LABEL agave_platform=2.0
LABEL tooling=transfer,sftp,worker,put

WORKDIR /go/
COPY --from=0 /go/src/github.com/agaveplatform/go-SFTP/release/fileServer /usr/local/bin/agave-sftp

RUN \
    chmod +x /usr/local/bin/agave-sftp && \
    mkdir /home/agaveops && \
    adduser -S -D -H agaveops

RUN chown -R agaveops:agaveops /home/agaveops

USER agaveops

WORKDIR /home/agaveops

ENTRYPOINT ["agave-sftp"]

CMD ["--help"]
