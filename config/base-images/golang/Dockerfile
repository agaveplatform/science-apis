FROM golang:1.11

ENV GO_VERSION 1.11

LABEL maintainer="Agave Platform<help@agaveplatform.org>"
LABEL language="golang:${GO_VERSION}"

ENV GOROOT /usr/local/go
ENV GOPATH /go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

RUN apt update && \
	apt -y install unzip \
				   jq \
				   curl \
				   ca-certificates \
				   make && \
    apt clean; \
    \
    # Adding dep for legacy support \
    curl -s -o dep-install.sh https://raw.githubusercontent.com/golang/dep/master/install.sh && \
	chmod +x dep-install.sh && \
	./dep-install.sh
