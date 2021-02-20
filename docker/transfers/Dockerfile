FROM adoptopenjdk/openjdk9:alpine

LABEL java_version=openjdk9
LABEL jdk_version=9

RUN addgroup -g 50 -S vertx && \
    adduser -u 1000 -g vertx -G vertx -S vertx  && \
    apk --no-cache add bash apr openssl apr-dev openssl-dev build-base curl pwgen

EXPOSE 8085 8080 80

COPY transfers-api-2.2.27.jar verticles/
COPY transfers-api-2.2.27.lib verticles/lib/
COPY ./*.yml ./*.json verticles/
RUN jar tf verticles/transfers-api-2.2.27.jar

ENV TRANSFERTASK_DB_SQL_QUERIES_RESOURCE_FILE /verticles/db-queries.yml

# Launch the verticle
ENTRYPOINT ["sh", "-c","java -jar verticles/transfers-api-2.2.27.jar"]
