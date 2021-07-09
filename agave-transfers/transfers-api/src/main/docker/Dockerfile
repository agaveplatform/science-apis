FROM adoptopenjdk/openjdk9:alpine

LABEL java_version=openjdk9
LABEL jdk_version=9

RUN addgroup -g 50 -S vertx && \
    adduser -u 1000 -g vertx -G vertx -S vertx  && \
    apk --no-cache add bash apr openssl apr-dev openssl-dev build-base curl pwgen

EXPOSE 8085 8080 80

COPY ${project.artifactId}-${project.version}.jar verticles/
COPY ${project.artifactId}-${project.version}.lib verticles/lib/
COPY ./db-queries.yml ./config.json verticles/
RUN jar tf verticles/${project.artifactId}-${project.version}.jar

# Launch the verticle
ENTRYPOINT ["sh", "-c","java -jar verticles/${project.artifactId}-${project.version}.jar"]
