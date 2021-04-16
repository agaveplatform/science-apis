######################################################
#
# Agave Tomcat 8.45 + OpenJDK 9 Base Image
# Tag: agaveplatform/tomcat:8.5
#
# This is the base image for Agave's Java APIs. It
# contains OpenJDK 9, Tomcat 8.5, implicit CORS support,
# and configs to autowire a MySQL or MariaDB
# server from the environment.
#
# It can be built from its root directory with the following
# to give it the 8.5 tag:
#
#   docker build --rm -t agaveplatform/tomcat:8.5 .
#
# https://github.com/agaveplatform/docker-tomcat
# https://agaveplatform.org
#
######################################################

FROM adoptopenjdk/openjdk9:alpine

LABEL maintainer="Agave Platform<help@agaveplatform.org>"

COPY tcp/limits.conf /etc/security/limits.conf
COPY tcp/sysctl.conf /etc/sysctl.conf

ENV CATALINA_HOME /opt/tomcat
ENV ANT_VERSION 1.9.7
ENV TOMCAT_VERSION 8.5.46

RUN addgroup -g 50 -S tomcat && \
    adduser -u 1000 -g tomcat -G tomcat -S tomcat  && \
    apk --no-cache add bash apr openssl apr-dev openssl-dev build-base curl pwgen && \

	# download tomcat
	cd /opt && \
	curl -sL -O http://us.mirrors.quenda.co/apache/tomcat/tomcat-8/v$TOMCAT_VERSION/bin/apache-tomcat-$TOMCAT_VERSION.tar.gz && \
	tar xzf apache-tomcat-$TOMCAT_VERSION.tar.gz && \
	rm -f apache-tomcat-$TOMCAT_VERSION.tar.gz && \
	mv apache-tomcat-$TOMCAT_VERSION tomcat && \

    # Install ant so we can build the tcnative jar
    cd / && \
    curl -O http://archive.apache.org/dist/ant/binaries/apache-ant-$ANT_VERSION-bin.tar.gz && \
    tar xzf apache-ant-$ANT_VERSION-bin.tar.gz && \

    # Build Tomcat APR for native ssl + threading
	cd $CATALINA_HOME/bin && \
	tar xzf tomcat-native.tar.gz && \
	cd $CATALINA_HOME/bin/tomcat-native-*-src/native/ && \
	./configure --with-apr=/usr/bin/apr-1-config --with-java-home=$JAVA_HOME  --with-ssl=yes --prefix=/usr && \
	make && \
	make install && \

	# Package and install the tcnative jar
    cd $CATALINA_HOME/bin/tomcat-native-*-src && \
    /apache-ant-$ANT_VERSION/bin/ant download && \
    /apache-ant-$ANT_VERSION/bin/ant && \
    /apache-ant-$ANT_VERSION/bin/ant jar && \
    cp dist/tomcat-native-*.jar $CATALINA_HOME/lib/ && \

    # link shared libraries installed in non-standard places on alpine
    ln -s /lib/libuuid.so.1 /usr/lib/libuuid.so.1 && \
    ln -s /lib/libc.musl-x86_64.so.1 /usr/lib/libc.musl-x86_64.so.1 && \

	# create common folder paths
	mkdir -p $CATALINA_HOME/.globus && \
    mkdir -p /scratch && \
    mkdir -p $CATALINA_HOME/logs && \

	# cleanup after build to reduce final image size
    apk del --no-cache apr-dev openssl-dev build-base && \
	rm -rf /apache-ant-$ANT_VERSION* && \
	rm -rf $CATALINA_HOME/bin/tomcat-native-*-src && \
	rm -rf $CATALINA_HOME/webapps/ROOT $CATALINA_HOME/webapps/docs && \
	rm -ff $CATALINA_HOME/conf/logging.properties && \
    rm -rf /var/cache/apk/*


# Install Tomcat config files for JNDI and better file upload/throughput
COPY tomcat/conf/* /opt/tomcat/conf/
COPY tomcat/lib/log4j.properties /opt/tomcat/lib/log4j.properties
COPY lib/* /opt/tomcat/lib/
COPY docker_entrypoint.sh /docker_entrypoint.sh

ENV X509_CERT_DIR $CATALINA_HOME/.globus
ENV CATALINA_OPTS "-Duser.timezone=UTC -Djsse.enableCBCProtection=false -Djava.awt.headless=true -Dfile.encoding=UTF-8 -server -Xms1024m -Xmx1024m -XX:+DisableExplicitGC -Djava.security.egd=file:/dev/urandom"
ENV PATH $PATH:$CATALINA_HOME/bin
ENV JAVA_OPTS "-Duser.timezone=UTC -Dfile.encoding=UTF-8"

WORKDIR /opt/tomcat

# http, https, jmx admin, jmx rmi, jpda debugging
EXPOSE 80 443 10001 10002 52911

ENTRYPOINT ["/docker_entrypoint.sh"]

CMD ["/opt/tomcat/bin/catalina.sh", "run", "2>&1"]
