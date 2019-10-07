#############################################################
#
# Maven 3.6 base image
# Tag: agaveplatform/maven:3.6
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

FROM adoptopenjdk/openjdk9:alpine

MAINTAINER Agave Platform <help@agaveplatform.org>

LABEL java_version=openjdk9
LABEL jdk_version=9
LABEL maven_version=3.6.2

ENV MAVEN_VERSION 3.6.2

RUN apk --update add bash zip bzip2 git curl ca-certificates && \
	cd /usr/share && \
	wget http://apache.osuosl.org/maven/maven-3/$MAVEN_VERSION/binaries/apache-maven-$MAVEN_VERSION-bin.zip && \
    unzip apache-maven-$MAVEN_VERSION-bin.zip && \
    ln -s /usr/share/apache-maven-$MAVEN_VERSION/bin/mvn /usr/bin/mvn && \
    rm apache-maven-$MAVEN_VERSION-bin.zip && \
    rm -rf /var/cache/apk/* && \
    mkdir /sources

WORKDIR /sources

VOLUME /sources

CMD mvn clean package