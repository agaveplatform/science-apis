#############################################################
#
# golang base image
# Tag: agaveplatform/golang:1.13
#
# Build image for golang applications. Dep included.
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

FROM golang:1.13

ENV GO_VERSION 1.13

LABEL maintainer="Agave Platform<help@agaveplatform.org>"
LABEL language="golang:${GO_VERSION}"

ENV GOROOT /usr/local/go
ENV GOPATH /go
ENV PATH $GOPATH/bin:$GOROOT/bin:$PATH

RUN apt-get update && \
	apt-get install -y --no-install-recommends unzip \
				   jq \
				   curl \
				   ca-certificates \
				   make && \
    apt-get clean && \

    # Adding dep for legacy support \
    curl -s -o dep-install.sh https://raw.githubusercontent.com/golang/dep/master/install.sh && \
	chmod +x dep-install.sh && \
	./dep-install.sh && \

	# clean out apt get lists
	rm -rf /var/lib/apt/lists/*
