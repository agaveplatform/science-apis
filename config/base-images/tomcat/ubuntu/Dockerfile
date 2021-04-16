######################################################
#
# Agave Tomcat 8.45 + OpenJDK 9 Ubuntu Base Image
# Tag: agaveplatform/tomcat:8.5-ubuntu
#
# This is the base image for Agave's Java APIs. It
# contains OpenJDK 9, Tomcat 8.5, implicit CORS support,
# and configs to autowire a MySQL or MariaDB
# server from the environment.
#
# It can be built from its root directory with the following
# to give it the 8.5 tag:
#
#   docker build --rm -t agaveplatform/tomcat:8.5-ubuntu .
#
# https://github.com/agaveplatform/docker-tomcat
# https://agaveplatform.org
#
######################################################

FROM adoptopenjdk/openjdk9:x86_64-ubuntu-jdk-9.0.4.11

LABEL maintainer="Agave Platform<help@agaveplatform.org>"

COPY tcp/limits.conf /etc/security/limits.conf
COPY tcp/sysctl.conf /etc/sysctl.conf

ENV CATALINA_HOME /opt/tomcat
ENV ANT_VERSION 1.9.14
ENV TOMCAT_VERSION 8.5.46
ENV DEBIAN_FRONTEND=noninteractive

RUN addgroup --gid 1000 --system tomcat && \
    adduser --uid 1000 --shell /bin/bash --disabled-password --system tomcat && \
    usermod -g tomcat -G tomcat tomcat && \
	id tomcat && \
    apt-get update && \
    apt-get install -y gcc curl pwgen libapr1 libapr1-dev make openssl libssl-dev vim-tiny unzip ntp && \
    # set timezone to America/Chicago
    ln -sf /usr/share/zoneinfo/America/Chicago /etc/localtime && \

	# download tomcat
	cd /opt && \
	curl -kL -O https://archive.apache.org/dist/tomcat/tomcat-8/v$TOMCAT_VERSION/bin/apache-tomcat-$TOMCAT_VERSION.tar.gz && \
	tar xzf apache-tomcat-$TOMCAT_VERSION.tar.gz && \
	rm -f apache-tomcat-$TOMCAT_VERSION.tar.gz && \
	mv apache-tomcat-$TOMCAT_VERSION tomcat && \

	# Build Tomcat APR for native ssl + threading
	cd $CATALINA_HOME/bin && \
	tar xzf tomcat-native.tar.gz && \
	cd $CATALINA_HOME/bin/tomcat-native-*-src/native/ && \
	./configure --with-apr=/usr/bin/apr-1-config --with-java-home=$JAVA_HOME  --with-ssl=yes --prefix=$CATALINA_HOME && \
	make && \
	make install && \

	# create common folder paths
	mkdir -p $CATALINA_HOME/.globus && \
    mkdir -p /scratch && \
    mkdir -p $CATALINA_HOME/logs && \

	# cleanup after build to reduce final image size
    apt-get -y remove gcc g++ && \
	apt-get -y autoremove libapr1-dev libssl-dev  && \
	rm -rf /apache-ant-$ANT_VERSION* && \
	rm -rf $CATALINA_HOME/bin/tomcat-native-*-src && \
	rm -rf $CATALINA_HOME/webapps/ROOT $CATALINA_HOME/webapps/docs && \
	rm -ff $CATALINA_HOME/conf/logging.properties


# Install Tomcat config files for JNDI and better file upload/throughput
COPY tomcat/conf/* /opt/tomcat/conf/
COPY tomcat/lib/log4j.properties /opt/tomcat/lib/log4j.properties
COPY lib/* /opt/tomcat/lib/
COPY docker_entrypoint.sh /docker_entrypoint.sh

ENV X509_CERT_DIR $CATALINA_HOME/.globus
ENV CATALINA_OPTS "-Duser.timezone=America/Chicago -Djsse.enableCBCProtection=false -Djava.awt.headless=true -Dfile.encoding=UTF-8 -server -Xms1024m -Xmx1024m -XX:+DisableExplicitGC -Djava.security.egd=file:/dev/urandom"
ENV PATH $PATH:$CATALINA_HOME/bin
ENV JAVA_OPTS "-Duser.timezone=America/Chicago -Dfile.encoding=UTF-8"
ENV LD_LIBRARY_PATH $LD_LIBRARY_PATH:$CATALINA_HOME/lib

WORKDIR /opt/tomcat

# http, https, jmx admin, jmx rmi, jpda debugging
EXPOSE 80 443 10001 10002 52911

ENTRYPOINT ["/docker_entrypoint.sh"]

CMD ["/opt/tomcat/bin/catalina.sh", "run", "2>&1"]
