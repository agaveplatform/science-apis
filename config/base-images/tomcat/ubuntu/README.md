## Agave Tomcat 8.5 image

This is the base image used to create the Agave Java API Images. It has Tomcat 8.5 and OpenJDK 9 installed and configured with a JNDI connection to a [MariaDB](https://registry.hub.docker.com/u/library/mariadb) container defined in the environment and/or linked at runtime. CORS support is implicit in this image, so all webapps extending it will have proper support out of the box.

## What is the Agave Platform?

The Agave Platform ([https://agaveplatform.org](https://agaveplatform.org)) is an open source, science-as-a-service API platform for powering your digital lab. Agave allows you to bring together your public, private, and shared high performance computing (HPC), high throughput computing (HTC), Cloud, and Big Data resources under a single, web-friendly REST API.

* Run scientific codes

  *your own or community provided codes*

* ...on HPC, HTC, or cloud resources

  *your own, shared, or commercial systems*

* ...and manage your data

  *reliable, multi-protocol, async data movement*

* ...from the web

  *webhooks, rest, json, cors, OAuth2*

* ...and remember how you did it

  *deep provenance, history, and reproducibility built in*

For more information, visit the [Agave Developer’s Portal](https://docs.agaveplatform.org).


## Using this image

This image can be used as a base image for all Java APIs. Simply create a Dockerfile that inherits this base image and add your war file to the Tomcat webapps folder /usr/local/tomcat/webapps.

Tomcat has a preconfigured JNDI connection with the following configuration:

```xml
<Resource name="jdbc/iplant_io" auth="Container"
		  factory="com.zaxxer.hikari.HikariJNDIFactory"
		  type="javax.sql.DataSource"
		  minimumIdle="1"
		  maximumPoolSize="10"
		  connectionTimeout="300000"
		  dataSourceClassName="org.mariadb.jdbc.MariaDbDataSource"
		  dataSource.serverName="%MYSQL_HOST%"
		  dataSource.port="%MYSQL_PORT%"
		  dataSource.databaseName="%MYSQL_DATABASE%"
		  dataSource.user="%MYSQL_USERNAME%"
		  dataSource.password="%MYSQL_PASSWORD%"/>
```

The MariaDB and Tomcat jdbc drivers are already present in the image. If not specified in the container environment, the tokens will be replaced with the values of a trusted [MariaDB](https://registry.hub.docker.com/u/library/mariadb) container linked at runtime.


### Running this image

When running in production, both the access and application logs will stream to standard out so they can be access via the Docker logs facility by default.

```
docker run --name some-api \
           -p 80:8080 \
           -e MYSQL_USERNAME=agaveuser \
           -e MYSQL_PASSWORD=password \
           -e MYSQL_HOST=mysql \
           -e MYSQL_PORT=3306 \
           agaveplatform/tomcat:8.5-ubuntu
```
