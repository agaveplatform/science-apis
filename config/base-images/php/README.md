## Agave PHP 7 Base Image

This is the base image used to create the Agave PHP API Images. It has Apache2 and PHP 7.3 installed and configured with a custom php.ini and apache config. Webapps using this image may access a database connection to a [MySQL](https://registry.hub.docker.com/u/library/mysql) or [MariaDB](https://registry.hub.docker.com/u/library/mariadb) container defined in the environment and/or linked at runtime.

## Extending this image

This image can be used as a base image for all PHP APIs. Simply create a Dockerfile that inherits this base image and add your PHP app to the web root folder at /var/www/html.

### Developing with this image

If you are developing with this image, mount your code into the `/var/www/html` directory in the container. Your local changes will be reflected instantly when you refresh your page.

```
docker run -h docker.example.com
           -p 80:80 \
           --name apache \
           -v `pwd`:/var/www/html \
           --link mysql:mysql
           -e DOCUMENT_ROOT=/var/www/html
           agaveplatform/php:7
```

Alternatively, you can specify a different web root if needed by your application. For example, if you had a Laravel project where the project `composer.json` file was located at `/usr/local/src/laravel/composer.json`, the following would start the container with the proper web root for the project.

```
docker run -h docker.example.com
           -p 80:80 \
           --name apache \
           -v /usr/local/src/laravel:/var/www \
           --link mysql:mysql
           -e DOCUMENT_ROOT=/var/www/public
           agaveplatform/php:7
```


### Running in production

When running in production, both the access and error logs will stream to standard out so they can be access via the Docker logs facility by default.

```
docker run -h docker.example.com \
           -p 80:80 \
           -p 443:443 \
           --name apache \
           -e MYSQL_USERNAME=agaveuser \
           -e MYSQL_PASSWORD=password \
           -e MYSQL_HOST=mysql \
           -e MYSQL_PORT=3306 \
           agaveplatform/php:7
```

### SSL Support

To add ssl support, volume mount your ssl cert, key, ca cert file, and ca chain file as needed. In the following example, a folder containing the necessary files is volume mounted to /ssl in the container.

```
docker run -h docker.example.com \
           -p 80:80 \
           -p 443:443 \
           --name apache \
           -e MYSQL_USERNAME=agaveuser \
           -e MYSQL_PASSWORD=password \
           -e MYSQL_HOST=mysql \
           -e MYSQL_PORT=3306 \
           -v `pwd`/ssl:/ssl:ro \
           -e SSL_CERT=/ssl/docker_example_com_cert.cer \
           -e SSL_KEY=/ssl/docker.example.com.key \
           -e SSL_CA_CERT=/ssl/docker_example_com.cer \
           aagaveplatform/php:7
```

### Environment Variables

The following environment variables are available to customize the PHP environment.

| Variable                     | Description                                                                                                                                               |
|:-----------------------------|:----------------------------------------------------------------------------------------------------------------------------------------------------------|
| APACHE_SERVER_NAME           | Change server name to match your domain name in httpd.conf                                                                                                |
| LOG_TARGET_STDOUT            | Add this env and give it a value to turn it on. Set to a non-empty value to log to stdout                                                                                                                 |
| LOG_LEVEL_DEBUG              | Add this env and give it a value to turn it on. Sets log level to debug                                                                                                                                   |
| LOG_LEVEL_ERROR              | Add this env and give it a value to turn it on. Sets log level to error                                                                                                                                   |
| LOG_LEVEL_INFO               | Add this env and give it a value to turn it on. Sets log level to info                                                                                                                                    |
| LOG_LEVEL_WARN               | Add this env and give it a value to turn it on. Sets log level to warn                                                                                                                                    |
| MYSQL_HOST                   | Hostname or IP of the database server                                                                                                                     |
| MYSQL_PORT                   | Port of the database server                                                                                                                               |
| MYSQL_USERNAME               | Username used to login to the database                                                                                                                    |
| MYSQL_PASSWORD               | Password used to login to database                                                                                                                        |
| MYSQL_DATABASE               | Name of the database schema name                                                                                                                          |
| PHP_SHORT_OPEN_TAG           | Maps to php.ini 'short_open_tag'                                                                                                                          |
| PHP_OUTPUT_BUFFERING         | Maps to php.ini 'output_buffering'                                                                                                                        |
| PHP_OPEN_BASEDIR             | Maps to php.ini 'open_basedir'                                                                                                                            |
| PHP_MAX_EXECUTION_TIME       | Maps to php.ini 'max_execution_time'                                                                                                                      |
| PHP_MAX_INPUT_TIME           | Maps to php.ini 'max_input_time'                                                                                                                          |
| PHP_MAX_INPUT_VARS           | Maps to php.ini 'max_input_vars'                                                                                                                          |
| PHP_MEMORY_LIMIT             | Maps to php.ini 'memory_limit'                                                                                                                            |
| PHP_ERROR_REPORTING          | Maps to php.ini 'error_reporting'                                                                                                                         |
| PHP_DISPLAY_ERRORS           | Maps to php.ini 'display_errors'                                                                                                                          |
| PHP_DISPLAY_STARTUP_ERRORS   | Maps to php.ini 'display_startup_errors'                                                                                                                  |
| PHP_LOG_ERRORS               | Maps to php.ini 'log_errors'                                                                                                                              |
| PHP_LOG_ERRORS_MAX_LEN       | Maps to php.ini 'log_errors_max_len'                                                                                                                      |
| PHP_IGNORE_REPEATED_ERRORS   | Maps to php.ini 'ignore_repeated_errors'                                                                                                                  |
| PHP_REPORT_MEMLEAKS          | Maps to php.ini 'report_memleaks'                                                                                                                         |
| PHP_HTML_ERRORS              | Maps to php.ini 'html_errors'                                                                                                                             |
| PHP_ERROR_LOG                | Maps to php.ini 'error_log'                                                                                                                               |
| PHP_POST_MAX_SIZE            | Maps to php.ini 'post_max_size'                                                                                                                           |
| PHP_DEFAULT_MIMETYPE         | Maps to php.ini 'default_mimetype'                                                                                                                        |
| PHP_DEFAULT_CHARSET          | Maps to php.ini 'default_charset'                                                                                                                         |
| PHP_FILE_UPLOADS             | Maps to php.ini 'file_uploads'                                                                                                                            |
| PHP_UPLOAD_TMP_DIR           | Maps to php.ini 'upload_tmp_dir'                                                                                                                          |
| PHP_UPLOAD_MAX_FILESIZE      | Maps to php.ini 'upload_max_filesize'                                                                                                                     |
| PHP_MAX_FILE_UPLOADS         | Maps to php.ini 'max_file_uploads'                                                                                                                        |
| PHP_ALLOW_URL_FOPEN          | Maps to php.ini 'allow_url_fopen'                                                                                                                         |
| PHP_ALLOW_URL_INCLUDE        | Maps to php.ini 'allow_url_include'                                                                                                                       |
| PHP_DEFAULT_SOCKET_TIMEOUT   | Maps to php.ini 'default_socket_timeout'                                                                                                                  |
| PHP_DATE_TIMEZONE            | Maps to php.ini 'date.timezone'                                                                                                                           |
| PHP_PDO_MYSQL_CACHE_SIZE     | Maps to php.ini 'pdo_mysql.cache_size'                                                                                                                    |
| PHP_PDO_MYSQL_DEFAULT_SOCKET | Maps to php.ini 'pdo_mysql.default_socket'                                                                                                                |
| PHP_SESSION_SAVE_HANDLER     | Maps to php.ini 'session.save_handler'                                                                                                                    |
| PHP_SESSION_SAVE_PATH        | Maps to php.ini 'session.save_path'                                                                                                                       |
| PHP_SESSION_USE_STRICT_MODE  | Maps to php.ini 'session.use_strict_mode'                                                                                                                 |
| PHP_SESSION_USE_COOKIES      | Maps to php.ini 'session.use_cookies'                                                                                                                     |
| PHP_SESSION_COOKIE_SECURE    | Maps to php.ini 'session.cookie_secure'                                                                                                                   |
| PHP_SESSION_NAME             | Maps to php.ini 'session.name'                                                                                                                            |
| PHP_SESSION_COOKIE_LIFETIME  | Maps to php.ini 'session.cookie_lifetime'                                                                                                                 |
| PHP_SESSION_COOKIE_PATH      | Maps to php.ini 'session.cookie_path'                                                                                                                     |
| PHP_SESSION_COOKIE_DOMAIN    | Maps to php.ini 'session.cookie_domain'                                                                                                                   |
| PHP_SESSION_COOKIE_HTTPONLY  | Maps to php.ini 'session.cookie_httponly'                                                                                                                 |
| PHP_XDEBUG_ENABLED           | Add this env and give it a value to turn it on. Turns on xdebug (which is not for production) |



### Logging

All apache access and error logs are written to /var/log/apache2 by default. You man access them by mounting the folder as a host volume. You can optionally consolidate and stream logs to STDOUT by setting the environment variable `LOG_TARGET_STDOUT` to any truthy value.

The default log level is `INFO`. You can alter the log level by setting any of the following environment variables to a truthy value: `LOG_LEVEL_INFO`, `LOG_LEVEL_WARN`, `LOG_LEVEL_ERROR`, and `LOG_LEVEL_DEBUG`.
