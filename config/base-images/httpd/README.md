# Apache 2.4 Base Image

This is a Minimal apache image with support for dynamic document root definition, SSL, and default unified logging to standard out.

## Extending this image

This image can be used as a standard web server for most apps. It is sufficient to add a .htaccess file to
the server root to set up redirects, etc.

### Developing with this image

If you are developing with this image, mount your code into the `/var/www/html` directory in the container. Your local changes will be reflected instantly when you refresh your page.

```
docker run -h docker.example.com
           -p 80:80 \
           --name apache \
           -v `pwd`:/var/www/html \
           --link mysql:mysql
           agaveplatform/apache:2.4
```

Alternatively, you can specify a different web root if needed by your application.
```
docker run -h docker.example.com
           -p 80:80 \
           --name apache \
           -v /usr/local/src/code:/var/www \
           --link mysql:mysql
           -e DOCUMENT_ROOT=/var/www/public
           agaveplatform/apache:2.4
```

### Running in production

When running in production, both the access and error logs will stream to standard out so they can be access via the Docker logs facility by default.

```
docker run -h docker.example.com \
           -p 80:80 \
           -p 443:443 \
           --name apache \
           agaveplatform/apache:2.4
```

### SSL Support

To add ssl support, volume mount your ssl cert, key, ca cert file, and ca chain file as needed. In the following example, a folder containing the necessary files is volume mounted to /ssl in the container.

```
docker run -h docker.example.com \
           -p 80:80 \
           -p 443:443 \
           --name apache \
           -v `pwd`/ssl:/ssl:ro \
           -e SSL_CERT=/ssl/docker_example_com_cert.cer \
           -e SSL_KEY=/ssl/docker.example.com.key \
           -e SSL_CA_CERT=/ssl/docker_example_com.cer \
           agaveplatform/apache:2.4
```

### Logging

All apache access and error logs are written to /var/log/apache2 by default. You man access them by mounting the folder as a host volume. You can optionally consolidate and stream logs to STDOUT by setting the environment variable `LOG_TARGET_STDOUT` to any truthy value.

The default log level is `INFO`. You can alter the log level by setting any of the following environment variables to a truthy value: `LOG_LEVEL_INFO`, `LOG_LEVEL_WARN`, `LOG_LEVEL_ERROR`, and `LOG_LEVEL_DEBUG`.
