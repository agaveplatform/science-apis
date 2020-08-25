#############################################################
#
# Maven 3.6 base image
# Tag: agaveplatform/maven:3.6-proto
#
# Build image for java 9 target runtime.
#
# The image can be built by issuing this command from the
# directory containing this file:
#
#   docker build -t agaveplatform/maven:3.6 .
#
# https://github.com/agaveplatform/docker-maven
# https://agaveplatform.org
#
#############################################################

FROM adoptopenjdk/openjdk9:x86_64-ubuntu-jdk-9.0.4.11

MAINTAINER Agave Platform <help@agaveplatform.org>

LABEL java_version=openjdk9
LABEL jdk_version=9
LABEL maven_version=3.6.2
LABEL grpc_version=1.29
LABEL grpc_java_version=1.29

ENV MAVEN_VERSION 3.6.3
ENV PROTOBUF_VERSION 3.12.2

RUN set -ex && apt-get update && \
    apt-get install -y	bash \
    				   	jq \
    				   	wget \
    				   	curl \
    				   	vim-tiny \
    				   	ca-certificates \
    				   	zip \
    				   	bzip2 \
    				   	git && \

	BUILD_DEPS="wget" \
	# Fail on error
	&& set -x \
	# Install BUILD_DEPS
	&& apt-get update && apt-get install -y $BUILD_DEPS --no-install-recommends \
	# Download Protobuf
	&& wget -O "/tmp/protoc-${PROTOBUF_VERSION}.zip" "https://github.com/google/protobuf/releases/download/v${PROTOBUF_VERSION}/protoc-${PROTOBUF_VERSION}-linux-x86_64.zip" \
	# Unpackage the zip
	&& unzip "/tmp/protoc-${PROTOBUF_VERSION}.zip" -d "/opt/protoc-${PROTOBUF_VERSION}" \
	# Symlink onto PATH
	&& ln -s "/opt/protoc-${PROTOBUF_VERSION}/bin/protoc" "/usr/bin/protoc" \
	# Cleanup tmp dir
	&& rm -f "/tmp/protoc-${PROTOBUF_VERSION}.zip" \
	# Install mvn
	&& cd /usr/share \
	&& wget https://downloads.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.zip \
    && unzip apache-maven-$MAVEN_VERSION-bin.zip \
    && ln -s /usr/share/apache-maven-$MAVEN_VERSION/bin/mvn /usr/bin/mvn \
    && rm apache-maven-$MAVEN_VERSION-bin.zip \
    && mkdir /sources \
	# Uninstall BUILD_DEPS
	&& apt-get purge -y --auto-remove $BUILD_DEPS \
	# Cleanup cache
	&& rm -rf /var/lib/apt/lists/*


#ADD all/entrypoint.sh /usr/local/bin
#RUN chmod +x /usr/local/bin/entrypoint.sh

#RUN apt-get install wget  && \
#	cd /usr/share && \
#	wget "https://downloads.apache.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.zip" && \
#    unzip apache-maven-$MAVEN_VERSION-bin.zip && \
#    ln -s /usr/share/apache-maven-$MAVEN_VERSION/bin/mvn /usr/bin/mvn && \
#    rm apache-maven-$MAVEN_VERSION-bin.zip && \
#    mkdir /sources


WORKDIR /sources

VOLUME /sources

CMD ["mvn","clean","package"]