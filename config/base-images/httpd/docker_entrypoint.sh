#!/bin/bash

# Set the timezone. Base image does not contain the setup-timezone script, so an alternate way is used.
if [ "$CONTAINER_TIMEZONE" ]; then
    cp /usr/share/zoneinfo/${CONTAINER_TIMEZONE} /etc/localtime && \
	echo "${CONTAINER_TIMEZONE}" >  /etc/timezone && \
	echo "Container timezone set to: $CONTAINER_TIMEZONE"
fi


# Dynamically set document root at container startup
DOCUMENT_ROOT=${DOCUMENT_ROOT:-/var/www/html}

# Ensure the document root exists
mkdir -p "$DOCUMENT_ROOT"

# Apache server name change
if [ ! -z "$APACHE_SERVER_NAME" ]
	then
		sed -i "s/^\#\?ServerName www.example.com:80/ServerName ${APACHE_SERVER_NAME}/" /usr/local/apache2/conf/httpd.conf
		sed -i "s/^\#\?ServerName www.example.com:443/ServerName ${APACHE_SERVER_NAME}:443/" /usr/local/apache2/conf/extra/httpd-ssl.conf
		echo "Changed server name to '$APACHE_SERVER_NAME'..."
	else
		echo "NOTICE: Change 'ServerName' globally and hide server message by setting environment variable >> 'APACHE_SERVER_NAME=your.server.name' in docker command or docker-compose file"
fi


# Enable logging to std out
if [[ -n "$LOG_TARGET_STDOUT" ]]; then
	ACCESS_LOG=/proc/self/fd/1
	ERROR_LOG=/proc/self/fd/2
	COMBINED_LOG=/proc/self/fd/1
else
	ACCESS_LOG=logs/access_log
	ERROR_LOG=logs/error_log
	COMBINED_LOG=logs/access_log
fi

# Enable toggling the log level at startup
if [[ -n "$LOG_LEVEL_DEBUG" ]]; then
  LOG_LEVEL=debug
elif [[ -n "$LOG_LEVEL_WARN" ]]; then
  LOG_LEVEL=warn
elif [[ -n "$LOG_LEVEL_ERROR" ]]; then
  LOG_LEVEL=error
else
  LOG_LEVEL=info
fi

# Update httpd.conf config with environment variables
sed -i \
	-e 's#^DocumentRoot ".*"#DocumentRoot "'$DOCUMENT_ROOT'"#' \
	-e '2,/^<Directory \".*\">/ s#^<Directory \".*\">#<Directory "'$DOCUMENT_ROOT'">#' \
	-e 's#^LogLevel .*#LogLevel '$LOG_LEVEL'#' \
	-e 's#^ErrorLog .*#ErrorLog '$ERROR_LOG'#' \
	-e 's#CustomLog .*#CustomLog '$ACCESS_LOG' combined#' \
	/usr/local/apache2/conf/httpd.conf


# Update httpd-ssl.conf config with environment variables
sed -i \
	-e 's#^DocumentRoot ".*"#DocumentRoot "'$DOCUMENT_ROOT'"#' \
	-e '2,/^<Directory \".*\">/ s#^<Directory \".*\">#<Directory "'$DOCUMENT_ROOT'">#' \
	-e 's#^LogLevel .*#LogLevel '$LOG_LEVEL'#' \
	-e 's#^ErrorLog .*#ErrorLog '$ERROR_LOG'#' \
	-e 's#CustomLog .*#CustomLog '$ACCESS_LOG' \\#' \
	/usr/local/apache2/conf/extra/httpd-ssl.conf

sed -i \
	-e 's#^LogLevel .*#LogLevel '$LOG_LEVEL'#' \
	-e 's#ServerName .*#ServerName '$HOSTNAME'#' \
	/usr/local/apache2/conf/extra/httpd-default.conf

#################################################################
# Configure ssl certs to use mounted files or the container defaults
#################################################################

echo "=> Creating SSL keys for secure communcation..."
if [[ -z "$SSL_KEY" ]]; then
	export KEY=/usr/local/apache2/conf/server.key
	export DOMAIN=$(hostname)
	export PASSPHRASE=$(cat /dev/urandom | tr -cd 'a-f0-9' | head -c 16)
	export SUBJ="
C=US
ST=Texas
O=AgavePlatform
localityName=Austin
commonName=$DOMAIN
organizationalUnitName=AgavePlatform
emailAddress=ssl@agaveplatform.org"
	openssl genrsa -des3 -out /usr/local/apache2/conf/server.key -passout env:PASSPHRASE 2048
	openssl req -new -batch -subj "$(echo -n "$SUBJ" | tr "\n" "/")" -key $KEY -out /usr/local/apache2/conf/$DOMAIN.csr -passin env:PASSPHRASE
	cp $KEY $KEY.orig
	openssl rsa -in $KEY.orig -out $KEY -passin env:PASSPHRASE
	openssl x509 -req -days 365 -in /usr/local/apache2/conf/$DOMAIN.csr -signkey $KEY -out /usr/local/apache2/conf/server.crt
fi

# Configure SSL as needed, defaulting to the self-signed cert unless otherwise specified.
if [[ -n "$SSL_CERT" ]]; then
  sed -i 's#SSLCertificateFile=".*#SSLCertificateFile="'$SSL_CERT'"#g' /usr/local/apache2/conf/extra/httpd-ssl.conf
fi

if [[ -n "$SSL_KEY" ]]; then
  sed -i 's#SSLCertificateKeyFile=".*#SSLCertificateKeyFile="'$SSL_KEY'"#g' /usr/local/apache2/conf/extra/httpd-ssl.conf
fi

if [[ -n "$SSL_CA_CERT" ]]; then
  sed -i 's#SSLCACertificatePath=".*#SSLCACertificatePath="'$SSL_CA_CERT'"#g' /usr/local/apache2/conf/extra/httpd-ssl.conf
fi

# finally, run the command passed into the container
exec "$@"
