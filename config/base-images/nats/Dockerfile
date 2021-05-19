FROM golang:1.16-alpine AS builder

ENV NSC_VERSION 2.2.3
ENV NATS_CLI_VERSION 0.0.23
ENV NATS_TOP_VERSION v0.4.0

WORKDIR $GOPATH/src/github.com/nats-io/

RUN apk add -U --no-cache git binutils

RUN go get github.com/nats-io/nats-top@$NATS_TOP_VERSION

RUN go get -u -ldflags "-X main.version=$(date +%Y%m%d)" github.com/nats-io/nsc@$NSC_VERSION

RUN mkdir -p src/github.com/nats-io && \
    cd src/github.com/nats-io/ && \
    git clone https://github.com/nats-io/natscli.git -b $NATS_CLI_VERSION && \
    cd natscli/nats && \
    go build -ldflags "-s -w -X main.version=$(date +%Y%m%d)" -o /nats


FROM alpine:3.13

RUN apk add -U --no-cache ca-certificates figlet

COPY --from=builder /go/bin/* /usr/local/bin/
COPY --from=builder /nats /usr/local/bin/

WORKDIR /root

USER root

ENV NKEYS_PATH /nsc/nkeys
ENV NSC_HOME /nsc/accounts
ENV NATS_CONFIG_HOME /nsc/config
ENV NATS_URL nats://nats:4222

ENTRYPOINT ["/bin/sh", "-l"]