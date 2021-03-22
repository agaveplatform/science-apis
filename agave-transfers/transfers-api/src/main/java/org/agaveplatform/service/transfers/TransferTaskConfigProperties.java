package org.agaveplatform.service.transfers;

/**
 * String literal constants representing the keys to the service's config file.
 */
public class TransferTaskConfigProperties {
    public static final String CONFIG_TRANSFERTASK_HTTP_PORT = "TRANSFERTASK_HTTP_PORT";
    public static final String CONFIG_TRANSFERTASK_JWT_AUTH = "TRANSFERTASK_JWT_AUTH";
    public static final String CONFIG_TRANSFERTASK_JWT_VERIFY = "TRANSFERTASK_JWT_VERIFY";
    public static final String CONFIG_TRANSFERTASK_JWT_PUBLIC_KEY = "TRANSFERTASK_JWT_PUBLIC_KEY";
    public static final String CONFIG_TRANSFERTASK_JWT_PRIVATE_KEY= "TRANSFERTASK_JWT_PRIVATE_KEY";
    public static final String CONFIG_TRANSFERTASK_DB_JDBC_URL = "TRANSFERTASK_DB_JDBC_URL";
    public static final String CONFIG_TRANSFERTASK_DB_JDBC_USERNAME = "TRANSFERTASK_DB_JDBC_USERNAME";
    public static final String CONFIG_TRANSFERTASK_DB_JDBC_PASSWORD = "TRANSFERTASK_DB_JDBC_PASSWORD";
    public static final String CONFIG_TRANSFERTASK_DB_JDBC_DRIVER_CLASS = "TRANSFERTASK_DB_JDBC_DRIVER_CLASS";
    public static final String CONFIG_TRANSFERTASK_DB_JDBC_MAX_POOL_SIZE = "TRANSFERTASK_DB_JDBC_MAX_POOL_SIZE";
    public static final String CONFIG_TRANSFERTASK_DB_SQL_QUERIES_RESOURCE_FILE = "TRANSFERTASK_DB_SQL_QUERIES_RESOURCE_FILE";
    public static final String CONFIG_TRANSFERTASK_DB_QUEUE = "TRANSFERTASK_DB_QUEUE";
    public static final String TRANSFERTASK_MAX_ATTEMPTS = "TRANSFERTASK_MAX_ATTEMPTS";
    public static final String MAX_TIME_FOR_TASK = "MAX_TIME_FOR_TASK";

    public static final String MAX_TIME_FOR_HEALTHCHECK = "MAX_TIME_FOR_HEALTHCHECK";
    public static final String MAX_TIME_FOR_HEALTHCHECK_PARENT = "MAX_TIME_FOR_HEALTHCHECK_PARENT";
    public static final int FLUSH_DELAY_NATS = 500;
}
