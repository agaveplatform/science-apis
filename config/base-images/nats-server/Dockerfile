#############################################################
#
# NATS server
# Tag: agaveplatform/nats-server:2.2.4
#
# Contains NATS server along with nats cli, top, and nsc command
# line tools. The server version is pinned. The cli tooling
# builds the latest versions every time.
#
# The image can be built by issuing this command from the
# directory containing this file:
#
#   docker build -t agaveplatform/golang:1.13 .
#
# https://github.com/agaveplatform/docker-golang
# https://agaveplatform.org
#
#############################################################

FROM golang:1.16-alpine AS builder

WORKDIR $GOPATH/src/github.com/nats-io/

RUN apk add -U --no-cache git binutils

RUN go get github.com/nats-io/nats-top

RUN go get -u -ldflags "-X main.version=$(date +%Y%m%d)" github.com/nats-io/nsc@master

RUN mkdir -p src/github.com/nats-io && \
    cd src/github.com/nats-io/ && \
    git clone https://github.com/nats-io/natscli.git && \
    cd natscli/nats && \
    go build -ldflags "-s -w -X main.version=$(date +%Y%m%d)" -o /nats


FROM alpine:3.13

ENV NATS_SERVER 2.2.4

RUN apk add -U --no-cache ca-certificates figlet

COPY --from=builder /go/bin/* /usr/local/bin/
COPY --from=builder /nats /usr/local/bin/

RUN set -eux; \
	apkArch="$(apk --print-arch)"; \
	case "$apkArch" in \
		aarch64) natsArch='arm64'; sha256='88ad3d5fbef04ad3298ab528a1dc59f8531e313a0b47dc39606d8fbe29e71341' ;; \
		armhf) natsArch='arm6'; sha256='cbcf6db915022c723149b609706d5fcf87f02ddca1c0dd0367870e394005b89d' ;; \
		armv7) natsArch='arm7'; sha256='80be30d9caba8aa4661ed50fe64eeb454ff023f54a3687b85293f55bf6725d33' ;; \
		x86_64) natsArch='amd64'; sha256='419b3a8761d1120c40851e2ac6d820695ca6b300e95933d88603cbd1255043fa' ;; \
		x86) natsArch='386'; sha256='fd82800d141072f27f2c9e34ac287b40b3f086be4dca5a159611ce0e03f04ec2' ;; \
		*) echo >&2 "error: $apkArch is not supported!"; exit 1 ;; \
	esac; \
	\
	wget -O nats-server.zip "https://github.com/nats-io/nats-server/releases/download/v${NATS_SERVER}/nats-server-v${NATS_SERVER}-linux-${natsArch}.zip"; \
	echo "${sha256} *nats-server.zip" | sha256sum -c -; \
	\
	apk add --no-cache ca-certificates; \
	apk add --no-cache --virtual buildtmp unzip; \
	\
	unzip nats-server.zip "nats-server-v${NATS_SERVER}-linux-${natsArch}/nats-server"; \
	rm nats-server.zip; \
	mv "nats-server-v${NATS_SERVER}-linux-${natsArch}/nats-server" /usr/local/bin; \
	rmdir "nats-server-v${NATS_SERVER}-linux-${natsArch}"; \
	\
	apk del --no-cache --no-network buildtmp

COPY ./nats-server.conf /etc/nats/nats-server.conf
COPY ./docker-entrypoint.sh /usr/local/bin

ENV NKEYS_PATH /nsc/nkeys
ENV NSC_HOME /nsc/accounts
ENV NATS_CONFIG_HOME /nsc/config
ENV NATS_URL nats://localhost:4222

EXPOSE 4222 8222 6222
ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["nats-server", "--config", "/etc/nats/nats-server.conf"]