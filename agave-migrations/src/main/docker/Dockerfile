FROM flyway/flyway:6.0

MAINTAINER Agave Platform <help@agaveplatform.org>

ENV FLYWAY_VERSION 6.0
ENV FLYWAY_HOME /flyway
ENV FLYWAY_EDITION community

# Now move in the filtered data and migrations
# from the build.
COPY conf/flyway.conf 			$FLYWAY_HOME/conf/flyway.conf
COPY sql 						$FLYWAY_HOME/sql
COPY docker-entrypoint.sh 		/docker-entrypoint.sh
COPY lib/** 					$FLYWAY_HOME/jars/

USER root

RUN chown -R flyway:flyway $FLYWAY_HOME && \
	chmod +x /docker-entrypoint.sh

USER flyway

WORKDIR $FLYWAY_HOME

ENTRYPOINT ["/docker-entrypoint.sh"]

CMD ["$FLYWAY_HOME/flyway", "info"]
