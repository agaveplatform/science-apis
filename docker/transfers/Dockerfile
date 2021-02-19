FROM adoptopenjdk/openjdk9:alpine

LABEL java_version=openjdk9
LABEL jdk_version=9
LABEL maven_version=3.6.2
LABEL grpc_version=1.29
LABEL grpc_java_version=1.29

RUN addgroup -g 50 -S vertx && \
    adduser -u 1000 -g vertx -G vertx -S vertx  && \
    apk --no-cache add bash apr openssl apr-dev openssl-dev build-base curl pwgen

ENV VERTICLE_JAR *.jar
ENV VERTICLE_HOME verticles

EXPOSE 8080
EXPOSE 8085
#SFTP
EXPOSE 22

COPY transfers-api-2.2.27.jar verticles/
COPY transfers-api-2.2.27.lib verticles/lib/
RUN jar tf verticles/transfers-api-2.2.27.jar

# Launch the verticle
ENTRYPOINT ["sh", "-c","java -jar verticles/transfers-api-2.2.27.jar"]
