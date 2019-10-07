######################################################
#
# Apache 2.4 Base Image
# Tag: agaveplatform/apache:2.4
#
# Minimal Apache 2.4 image with CORS, rewrite, and proxy
# support. Default unified logging to standard out.
#
# https://github.com/agaveplatform/docker-apache
# https://agaveplatform.org
#
######################################################

FROM httpd:2.4-alpine
MAINTAINER Agave Platform <help@agaveplatform.org>

ADD tcp/limits.conf /etc/security/limits.conf
ADD tcp/sysctl.conf /etc/sysctl.conf

RUN apk add --upgrade --no-cache openssl gzip bash curl && \

	# enable ssl and default config file
	sed -i'' -e 's/^#\(Include .*httpd-default.conf\)/\1/' /usr/local/apache2/conf/httpd.conf && \
	sed -i'' -e 's/^#\(Include .*httpd-ssl.conf\)/\1/' /usr/local/apache2/conf/httpd.conf && \
	sed -i'' -e 's/^#\(LoadModule .*mod_ssl.so\)/\1/' /usr/local/apache2/conf/httpd.conf && \
	sed -i'' -e 's/^#\(LoadModule .*mod_socache_shmcb.so\)/\1/' /usr/local/apache2/conf/httpd.conf && \

	# Update trusted cipher
	sed -i'' -e 's/SSLCipherSuite .*/SSLCipherSuite HIGH:MEDIUM:!MD5:!RC4:!3DES:!ADH/' /usr/local/apache2/conf/extra/httpd-ssl.conf && \
	sed -i'' -e 's/SSLProxyCipherSuite .*/SSLProxyCipherSuite HIGH:MEDIUM:!MD5:!RC4:!3DES:!ADH/' /usr/local/apache2/conf/extra/httpd-ssl.conf && \

	# bump default timeout to 12 hours
    sed -i 's#Timeout 60#Timeout 43200#' /usr/local/apache2/conf/extra/httpd-default.conf


#ADD apache/httpd.conf /etc/apache2/httpd.conf
#ADD apache/conf.d/* /etc/apache2/conf.d/
ADD docker_entrypoint.sh /docker_entrypoint.sh

WORKDIR /usr/local/apache2/htdocs

EXPOSE 80 443

ENTRYPOINT ["/docker_entrypoint.sh"]

CMD ["httpd", "-DFOREGROUND"]
