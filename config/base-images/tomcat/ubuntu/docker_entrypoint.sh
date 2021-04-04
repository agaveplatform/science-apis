#!/bin/bash
set +e

genpasswd() {
	local _len=${1:-16}
	if [ -n "$(which pwgen)" ]; then
		pwgen -s -B -1 ${1:-16}
	else
      	tr -dc A-Za-z0-9_ < /dev/urandom | head -c ${l} | xargs
    fi
}

if [[ -f "/app/config/environment.sh" ]]; then
  echo "=> Sourcing environment file found at /app/config/environment.sh"
	source /app/config/environment.sh
fi

#################################################################
# Configure MySQL jndi connection
#################################################################

if [[ -z "$MYSQL_HOST" ]]; then
  MYSQL_HOST=mysql
fi

if [[ -n "$MYSQL_PORT_3306_TCP_PORT" ]]; then
  MYSQL_PORT=3306
elif [[ -z "$MYSQL_PORT" ]]; then
  MYSQL_PORT=3306
fi

if [[ -n "$MYSQL_ENV_MYSQL_USERNAME" ]]; then
  MYSQL_USERNAME=$MYSQL_ENV_MYSQL_USERNAME
elif [[ -z "$MYSQL_USERNAME" ]]; then
  MYSQL_USERNAME=agaveuser
fi

if [[ -n "$MYSQL_ENV_MYSQL_PASSWORD" ]]; then
  MYSQL_PASSWORD=$MYSQL_ENV_MYSQL_PASSWORD
elif [[ -z "$MYSQL_PASSWORD" ]]; then
  MYSQL_PASSWORD=password
fi

if [[ -n "$MYSQL_ENV_MYSQL_DATABASE" ]]; then
  MYSQL_DATABASE=$MYSQL_ENV_MYSQL_DATABASE
elif [[ -z "$MYSQL_DATABASE" ]]; then
  MYSQL_DATABASE=agavecore
fi

# Update database config
echo "=> Setting container mysql connection to jdbc://mysql/${MYSQL_HOST}:${MYSQL_PORT}/${MYSQL_DATABASE}..."
for i in /opt/tomcat/conf/context.xml;
do
  sed -i -e "s/%MYSQL_HOST%/$MYSQL_HOST/" $i
  sed -i -e "s/%MYSQL_PORT%/$MYSQL_PORT/" $i
  sed -i -e "s/%MYSQL_USERNAME%/$MYSQL_USERNAME/" $i
  sed -i -e "s/%MYSQL_PASSWORD%/$MYSQL_PASSWORD/" $i
  sed -i -e "s/%MYSQL_DATABASE%/$MYSQL_DATABASE/" $i
done

# #################################################################
# # Enable/disable Tomcat manager
# #################################################################
#
# # Enable Tomcat Manager if a valid key was passed in
# if [[ -n "$ENABLE_TOMCAT_MANAGER" ]]; then
#   if [[ -z "$TOMCAT_MANGER_USERNAME" ]]; then
#     TOMCAT_MANGER_USERNAME=admin
#   fi
#
#   if [[ -z "$TOMCAT_MANGER_PASSWORD" ]]; then
#     TOMCAT_MANGER_PASSWORD=$(genpasswd)
#   fi
#
#   sed -i 's#<user name="admin" password="admin"#<user name="'$TOMCAT_MANGER_USERNAME'" password="'$TOMCAT_MANGER_PASSWORD'"#g' /opt/tomcat/conf/tomcat-users.xsd
#   echo "Tomcat Manager admin user: $TOMCAT_MANGER_USERNAME / $TOMCAT_MANGER_PASSWORD"
#
# else
#   if [[ -e "/opt/tomcat/conf/tomcat-users.xml" ]]
#   then
#       rm /opt/tomcat/conf/tomcat-users.xml
#       echo "Tomcat Manager disabled"
#   fi
# fi

#################################################################
# Configure ssl certs to use mounted files or the container defaults
#################################################################

echo "=> Creating SSL keys for secure communcation..."
if [[ -z "$SSL_KEY" ]]; then
	export KEY=/etc/ssl/private/server.key
	export DOMAIN=$(hostname)
	export PASSPHRASE=$(cat /dev/urandom | tr -cd 'a-f0-9' | head -c 16)
	export SUBJ="
C=US
ST=TX
localityName=Austin
commonName=$DOMAIN
organizationalUnitName=Agave Platform
emailAddress=admin@$DOMAIN"
	openssl rand -writerand ~/.rnd
	openssl genrsa -des3 -out /etc/ssl/private/server.key -passout env:PASSPHRASE 2048
	openssl req -new -batch -subj "$(echo -n "$SUBJ" | tr "\n" "/")" -key $KEY -out /etc/ssl/certs/$DOMAIN.csr -passin env:PASSPHRASE
	cp $KEY $KEY.orig
	openssl rsa -in $KEY.orig -out $KEY -passin env:PASSPHRASE
	openssl x509 -req -days 365 -in /etc/ssl/certs/$DOMAIN.csr -signkey $KEY -out /etc/ssl/certs/server.crt
fi

if [[ -n "$SSL_CERT" ]]; then
  sed -i 's#SSLCertificateFile=".*#SSLCertificateFile="'$SSL_CERT'"#g' /opt/tomcat/conf/server.xml
fi

if [[ -n "$SSL_KEY" ]]; then
  sed -i 's#SSLCertificateKeyFile=".*#SSLCertificateKeyFile="'$SSL_KEY'"#g' /opt/tomcat/conf/server.xml
fi

# # export SSL_CA_CHAIN=we_done_switched_the_cert_chain
# if [[ -n "$SSL_CA_CHAIN" ]]; then
#   sed -i 's#\#SSLCertificateChainFile=".*#SSLCertificateChainFile="'$SSL_CA_CHAIN'"#g' /opt/tomcat/conf/server.xml
# fi
# grep "we_done_switched_the_cert_chain" /etc/apache2/conf.d/ssl.conf

if [[ -n "$SSL_CA_CERT" ]]; then
  sed -i 's#SSLCACertificatePath=".*#SSLCACertificatePath="'$SSL_CA_CERT'"#g' /opt/tomcat/conf/server.xml
fi

#################################################################
# Scratch directory init
#################################################################

# create the scratch directory
if [[ -z "$IPLANT_SERVER_TEMP_DIR" ]]; then
	IPLANT_SERVER_TEMP_DIR=/scratch
fi

echo "=> Setting service temp directory to $IPLANT_SERVER_TEMP_DIR..."
mkdir -p "$IPLANT_SERVER_TEMP_DIR"

# ################################################################
# NTPD init
# ################################################################
#
# start ntpd because clock skew is astoundingly real
# ntpd -d -p pool.ntp.org 1>/dev/null &

#################################################################
# Unpack webapp
#
# This saves about a minute on startup time
#################################################################

WAR_NAME=$(ls $CATALINA_HOME/webapps/*.war 2> /dev/null)
if [[ -n "$WAR_NAME" ]]; then
	APP_NAME=$(basename $WAR_NAME | cut -d'.' -f1)
	echo "=> Expanding service war ${WAR_NAME}..."
	mkdir "$CATALINA_HOME/webapps/$APP_NAME"
	unzip -q -o -d "$CATALINA_HOME/webapps/$APP_NAME" "$WAR_NAME"
	rm -f ${WAR_NAME}
	echo "...done expanding war"
else
	echo "=> No war found in webapps directory."
fi

#################################################################
# Configure logging
#################################################################

# Configure logging output target. Logs to a file unless explicitly
# configured to send to standard out
if [[ -n "$LOG_TARGET_STDOUT" ]]; then
  LOG_TARGET=stdout
else
	LOG_TARGET=fileout
fi

# Enable toggling the log level at startup. DEBUG for all api services
# by default.
if [[ -n "$LOG_LEVEL_INFO" ]]; then
  LOG_LEVEL=INFO
elif [[ -n "$LOG_LEVEL_ERROR" ]]; then
  LOG_LEVEL=ERROR
elif [[ -n "$LOG_LEVEL_WARN" ]]; then
  LOG_LEVEL=WARN
elif [[ -n "$LOG_LEVEL_NONE" ]]; then
  LOG_LEVEL=NONE
else
	LOG_LEVEL=DEBUG
fi

echo "=> Setting service log level to $LOG_LEVEL..."
sed -i 's#^agaveLogLevel=.*#agaveLogLevel='$LOG_LEVEL'#g' $CATALINA_HOME/webapps/*/WEB-INF/classes/log4j.properties

echo "=> Setting service log target to $LOG_TARGET..."
sed -i 's#^logTarget=.*$#logTarget='$LOG_TARGET'#g' $CATALINA_HOME/webapps/*/WEB-INF/classes/log4j.properties


#################################################################
# Hibernate caching
#################################################################

if [[ -n "$HIBERNATE_CACHE_USE_SECOND_LEVEL_CACHE" ]]; then
  echo "=> Enabling hibernate second level caching..."
  sed -i 's#hibernate.cache.use_second_level_cache">false<#hibernate.cache.use_second_level_cache">true<#g' $CATALINA_HOME/webapps/*/WEB-INF/classes/hibernate.cfg.xml
fi

if [[ -n "$ENABLE_REMOTE_DEBUG" ]]; then
	echo "=> Enabled remote debugging via JMX"
	export CATALINA_OPTS="${CATALINA_OPTS} -Xdebug "
	export JPDA_TRANSPORT=${JPDA_TRANSPORT:-$(echo 'dt_socket')}
	export JPDA_ADDRESS=52911
	export JPDA_SUSPEND=${JPDA_SUSPEND:-$(echo 'n')}
	export ENABLE_JMX=1
else
	echo "=> Remote JMX debugging is disabled"
fi

if [[ -n "$ENABLE_JMX" ]]; then
	JMX_PASS=${JMX_ADMIN_PASS:-$(pwgen -s 12 1)}
	JMX_USER=${JMX_ADMIN_USER:-$(echo 'admin')}
	_word=$( [ ${JMX_ADMIN_PASS} ] && echo "preset" || echo "random" )
	echo "=> Creating an admin account with a ${_word} password in Tomcat"

	echo "=> Configuring Tomcat manager for new admin account"
	sed -i -e "s/MANAGER_USER/"$JMX_USER"/" $CATALINA_HOME/conf/tomcat-users.xml
	sed -i -e "s/MANAGER_PASS/"$JMX_PASS"/" $CATALINA_HOME/conf/tomcat-users.xml
	sed -i -e "s#<!-- ##" $CATALINA_HOME/conf/tomcat-users.xml
	sed -i -e "s# -->##" $CATALINA_HOME/conf/tomcat-users.xml

	echo "========================================================================"
	echo "You can now connect to this Tomcat Manager app at:"
	echo ""
	echo "    http://$HOSTNAME/manager/text"
	echo ""
	echo "With the credentials $JMX_USER / $JMX_PASS"
	echo "========================================================================"

	echo "=> Configuring JMX for new admin account"
	sed -i -e "s/ADMIN_USER/"$JMX_USER"/" $CATALINA_HOME/conf/jmxremote.password
	sed -i -e "s/ADMIN_USER/"$JMX_USER"/" $CATALINA_HOME/conf/jmxremote.access
	sed -i -e "s/JMX_ADMIN_PASSWORD/"$JMX_PASS"/" $CATALINA_HOME/conf/jmxremote.password
	sed -i -e "s/JMX_HOSTNAME/"$HOSTNAME"/" $CATALINA_HOME/conf/server.xml
	sed -i -e "s#<!-- JMX_RMI_LISTENER##" $CATALINA_HOME/conf/server.xml
	sed -i -e "s#JMX_RMI_LISTENER -->##" $CATALINA_HOME/conf/server.xml
	
	echo "========================================================================"
	echo "You can now connect to this Tomcat server using jmx:"
	echo ""
	echo "    service:jmx:rmi://$HOSTNAME:10002/jndi/rmi://$HOSTNAME:10001/jmxrmi "
	echo ""
	echo "or through the JMX Proxy Servlet:"
	echo ""
	echo "    http://$HOSTNAME/manager/jmxproxy/ "
	echo ""
	echo "With the credentials $JMX_USER / $JMX_PASS"
	echo "========================================================================"

	export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.authenticate=true"
	export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.password.file=$CATALINA_HOME/conf/jmxremote.password"
	export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.access.file=$CATALINA_HOME/conf/jmxremote.access"
	export CATALINA_OPTS="${CATALINA_OPTS} -Dcom.sun.management.jmxremote.ssl=false"

else
	echo "=> Disabled JMX support"
fi


####################################################################
#
# Synchronization and Service Discovery
#
# Make sure the system clock is up to data and provide any discovery
# functions needed to initialize the environment or application.
#####################################################################
#
## start ntpd because clock skew is astoundingly real
#ntpdate

# finally, run the command passed into the container
exec "$@"
