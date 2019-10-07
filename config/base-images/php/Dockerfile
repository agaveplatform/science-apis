######################################################
#
# Agave Apache PHP Base Image
# Tag: agaveplatform/php:7.3-apache
#
# This is the base image for Agave's PHP APIs. It
# builds a minimal image with Apache2 + php 7.3
#
# with support for auto-wiring database connections,
# CORS support, and unified logging to standard out.
#
# https://github.com/agaveplatform/docker-php
# https://agaveplatform.org
#
######################################################

FROM alpine:3.10

MAINTAINER Agave Platform <help@agaveplatform.org>

RUN \
    echo "Installing php and dependencies..." && \
    apk update && \
    apk apk upgrade && \
    apk add bash \
    	tzdata \
        curl \
        ca-certificates \
        git \
        openssl \
        apache2 \
        apache2-ssl \
        apache2-proxy \
        php7 \
        php7-apache2 \
        php7-phar \
        php7-json \
        php7-iconv \
        php7-openssl \
        openntpd && \

	# Add composer
    curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin --filename=composer && \

	# Add php extensions for common functionality. We can probably prune this list quite a bit.
	apk add php7-apcu \
        php7-bcmath \
        php7-bz2 \
        php7-ctype \
        php7-curl \
        php7-dom \
        php7-exif \
        php7-fileinfo \
        php7-ftp \
        php7-gd \
        php7-gettext \
        php7-gmp \
        php7-intl \
        php7-ldap \
        php7-mbstring \
        php7-mcrypt \
        php7-mysqli \
        php7-odbc \
        php7-opcache \
        php7-pecl-ssh2 \
        php7-pecl-zmq \
        php7-pdo \
        php7-pdo_dblib \
        php7-pdo_mysql \
        php7-pdo_odbc \
        php7-pdo_pgsql \
        php7-pdo_sqlite \
        php7-redis \
        php7-session \
        php7-soap \
        php7-sqlite3 \
        php7-tokenizer \
        php7-xdebug \
        php7-xml \
        php7-xmlreader \
        php7-xmlrpc \
        php7-xmlwriter \
        php7-zip \
        php7-zlib && \

	# Caused issues when bundled with the above extensions
    apk add php7-simplexml && \

	# Clean up image cache and temp files
    rm -f /var/cache/apk/* && \

	# Update Apache2 config to enable php
    sed -i "s/#LoadModule\ rewrite_module/LoadModule\ rewrite_module/" /etc/apache2/httpd.conf \
	&& sed -i "s/#LoadModule\ session_module/LoadModule\ session_module/" /etc/apache2/httpd.conf \
	&& sed -i "s/#LoadModule\ session_cookie_module/LoadModule\ session_cookie_module/" /etc/apache2/httpd.conf \
	&& sed -i "s/#LoadModule\ session_crypto_module/LoadModule\ session_crypto_module/" /etc/apache2/httpd.conf \
	&& sed -i "s/#LoadModule\ deflate_module/LoadModule\ deflate_module/" /etc/apache2/httpd.conf \
	&& sed -i "s#^DocumentRoot \".*#DocumentRoot \"/app/public\"#g" /etc/apache2/httpd.conf \
	&& sed -i "s#/var/www/localhost/htdocs#/app/public#" /etc/apache2/httpd.conf \
	&& printf "\n<Directory \"/app/public\">\n\tAllowOverride All\n</Directory>\n" >> /etc/apache2/httpd.conf \
	&& sed -i'' -e 's/SSLCipherSuite .*/SSLCipherSuite HIGH:MEDIUM:!MD5:!RC4:!3DES:!ADH/' /etc/apache2/conf.d/ssl.conf \
    && sed -i'' -e 's/SSLProxyCipherSuite .*/SSLProxyCipherSuite HIGH:MEDIUM:!MD5:!RC4:!3DES:!ADH/' etc/apache2/conf.d/ssl.conf \
    && sed -i 's#Timeout 60#Timeout 43200#' etc/apache2/conf.d/default.conf && \

	# Create app directory and add phpinfo page
	mkdir -p /app/public && \
    echo "<?php phpinfo(); ?>" > /app/public/index.php && \
    chown -R apache:apache /app && \
    chmod -R 755 /app && \

    # Add custom log format with unique id passed in all agave sessions
    sed -i 's#LogFormat "%h#LogFormat "[%{UNIQUE_ID}i] %{CONTAINER_STACK}e %h#g' /etc/apache2/httpd.conf && \

    # bump max cookie size to avoid issues with firefox
    echo "#" >> /etc/apache2/conf.d/default.conf && \
    echo "# Limit on maximum size of an HTTP request header field" >> /etc/apache2/conf.d/default.conf && \
    echo "#" >> /etc/apache2/conf.d/default.conf && \
    echo "LimitRequestFieldSize 16380" >> /etc/apache2/conf.d/default.conf && \

    # Update php.ini with large file support
	sed -i 's#^post_max_size =.*#post_max_size = 1G#' /etc/php7/php.ini && \
    sed -i 's#^max_file_uploads =.*#max_file_uploads = 512M#' /etc/php7/php.ini && \
    sed -i 's#^upload_max_filesize =.*#upload_max_filesize = 1G#' /etc/php7/php.ini && \
    sed -i 's#^max_execution_time =.*#max_execution_time = 43200#' /etc/php7/php.ini && \
    sed -i 's#^memory_limit =.*#memory_limit = 512M#' /etc/php7/php.ini


ADD docker_entrypoint.sh /docker_entrypoint.sh

WORKDIR /var/www/html

EXPOSE 80 443

ENTRYPOINT ["/docker_entrypoint.sh"]

CMD ["httpd", "-DFOREGROUND"]
