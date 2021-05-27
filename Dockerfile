#############################################################
#
# Agave Platform CI Build file
#
# This Dockerfile is used to build and test the Agave Platform
# services in such a way that the source and intermediate
# files are present on the native file system of the container
# to speed up the build without need of bind mounting the
# checkout. This can significantly speed up the build on
# systems utilizing a network mount or slower disks
# for the container storage.
#
# To use this Dockerfile, start by building the image, tagging with the
# commit hash for proper isolation.
#
#   docker build --rm -t science-apis:$(git log -n 1 --pretty=format:'%h')
#
# Then, you may run the integration tests in parallel with full isolation
# simply by attaching the container to the network created by each
# submodule's docker compose stack.
#
#   docker run -i --rm \
#            -v $(which docker):/bin/docker \
#            -v /var/run/docker.sock:/var/run/docker.sock
#            --net files-core_services \
#            science-apis:$(git log -n 1 --pretty=format:'%h') \
#                mvn -P agave,integration-test,jenkins -pl :files-core verify
#
# https://github.com/agaveplatform/science-apis
# https://agaveplatform.org
#
#############################################################

FROM agaveplatform/maven:3.6.3-proto

ARG BUILD_UID=995
ARG BUILD_GID=991
ARG BUILD_USERNAME=jenkins

RUN addgroup --gid $BUILD_GID $BUILD_USERNAME && \
	adduser --shell /bin/bash --uid $BUILD_UID --ingroup root --disabled-password $BUILD_USERNAME

ADD . /sources

RUN cd /sources && \
	mvn -P agave,dev clean && \
	chown -R $BUILD_USERNAME:$BUILD_USERNAME /sources

RUN chown -R $BUILD_USERNAME:$BUILD_USERNAME /opt/protoc* /usr/bin/protoc

RUN mvn -P agave,dev install
