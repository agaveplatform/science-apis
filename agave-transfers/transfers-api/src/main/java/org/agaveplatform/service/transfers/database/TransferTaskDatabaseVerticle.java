/*
 *  Copyright (c) 2017 Red Hat, Inc. and/or its affiliates.
 *  Copyright (c) 2017 INSA Lyon, CITI Laboratory.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.agaveplatform.service.transfers.database;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.serviceproxy.ServiceBinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;
/**
 * @author deardooley
 */
// tag::dbverticle[]
public class TransferTaskDatabaseVerticle extends AbstractVerticle {
  private static final  Logger log = LoggerFactory.getLogger(TransferTaskDatabaseVerticle.class);
  private static final String DEFAULT_SQL_QUERIES_FILE = "db-queries.yml";

  @Override
  public void start(Promise<Void> promise) throws Exception {

    HashMap<SqlQuery, String> sqlQueries = loadSqlQueries();

    JDBCClient dbClient = null;
    try {
      dbClient = JDBCClient.createShared(getVertx(), new JsonObject()
              .put("provider_class", "io.vertx.ext.jdbc.spi.impl.HikariCPDataSourceProvider")
              .put("jdbcUrl", config().getString(CONFIG_TRANSFERTASK_DB_JDBC_URL, "jdbc:mysql://127.0.0.1:3306/agavecore")) //
              .put("username", config().getString(CONFIG_TRANSFERTASK_DB_JDBC_USERNAME))
              .put("password", config().getString(CONFIG_TRANSFERTASK_DB_JDBC_PASSWORD))
              .put("autoCommit", true)
              .put("connectionTimeout", 60000)
              .put("validationTimeout", 45000)
              .put("leakDetectionThreshold", 5000)
              .put("driverClassName", config().getString(CONFIG_TRANSFERTASK_DB_JDBC_DRIVER_CLASS, "com.mysql.cj.jdbc.Driver"))
              .put("maximumPoolSize", config().getInteger(CONFIG_TRANSFERTASK_DB_JDBC_MAX_POOL_SIZE, 30)));
    }
    catch (Throwable t) {
      log.error("Failed to create shared database connection.", t);
      throw t;
    }

    TransferTaskDatabaseService.create(dbClient, sqlQueries, ready -> {
      if (ready.succeeded()) {
        ServiceBinder binder = new ServiceBinder(getVertx());
        binder
          .setAddress(config().getString(CONFIG_TRANSFERTASK_DB_QUEUE))
          .register(TransferTaskDatabaseService.class, ready.result());
        promise.complete();
      } else {
        ready.result();
        promise.fail(ready.cause());
      }
    });
  }

  /*
   * Note: this uses blocking APIs, but data is small...
   */
  private HashMap<SqlQuery, String> loadSqlQueries() throws IOException {
    String queriesFilePath = config().getString(CONFIG_TRANSFERTASK_DB_SQL_QUERIES_RESOURCE_FILE);

    if (queriesFilePath != null && !queriesFilePath.isEmpty()) {
      if (Files.exists(Paths.get(queriesFilePath))) {
        log.info("Loading sql queries from: {}", queriesFilePath);
        try (InputStream queriesInputStream = new FileInputStream(queriesFilePath)) {
          // return them all right away
          return initQueryHashMap(queriesInputStream);
        }
        catch (Exception e) {
          log.warn("Unable to load sql query file from config file location, {}.", e.getMessage());
        }
      } else {
        log.debug("No sql query file found at the config file location, {}. falling back on default sql query file.", queriesFilePath);
      }
    }

    log.info("Loading sql queries from default location, {}", DEFAULT_SQL_QUERIES_FILE);
    try (InputStream queriesInputStream = TransferTaskDatabaseVerticle.class.getClassLoader().getResourceAsStream(DEFAULT_SQL_QUERIES_FILE)) {
      return initQueryHashMap(queriesInputStream);
    }
    catch (Exception e) {
      throw new IOException("Unable to load default db queries file from application classpath. {}", e);
    }
  }

  /**
   * Reads the sql config files as yaml from the given input stream.
   * @param queriesInputStream the stream to the sql config file location.
   * @return a map of query names to actual sql values.
   * @throws IOException if unable to read the stream or parse the yaml.
   */
  private HashMap<SqlQuery, String> initQueryHashMap(InputStream queriesInputStream) throws IOException {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    JsonNode node = mapper.readTree(queriesInputStream);

    HashMap<SqlQuery, String> sqlQueries = new HashMap<>();
    sqlQueries.put(SqlQuery.CREATE_TRANSFERTASKS_TABLE, node.get("create-transfertasks-table").textValue());
    sqlQueries.put(SqlQuery.ALL_TRANSFERTASKS, node.get("all-transfertasks").textValue());
    sqlQueries.put(SqlQuery.ALL_USER_TRANSFERTASKS, node.get("all-user-transfertasks").textValue());
    sqlQueries.put(SqlQuery.GET_TRANSFERTASK, node.get("get-transfertask").textValue());
    sqlQueries.put(SqlQuery.CREATE_TRANSFERTASK, node.get("create-transfertask").textValue());
    sqlQueries.put(SqlQuery.SAVE_TRANSFERTASK, node.get("save-transfertask").textValue());
    sqlQueries.put(SqlQuery.DELETE_TRANSFERTASK, node.get("delete-transfertask").textValue());
    sqlQueries.put(SqlQuery.DELETE_ALL_TRANSFERTASKS, node.get("delete-all-transfertasks").textValue());
    sqlQueries.put(SqlQuery.UPDATE_TRANSFERTASK_STATUS, node.get("update-transfertask-status").textValue());
    sqlQueries.put(SqlQuery.ALL_TRANSFERTASK_CHILDREN_CANCELLED_OR_COMPLETED, node.get("all-transfertask-children-cancelled-or-completed").textValue());
    sqlQueries.put(SqlQuery.ALL_TRANSFERTASK_CHILDREN_ACTIVE, node.get("all-transfertask-children-active").textValue());
    sqlQueries.put(SqlQuery.ALL_ACTIVE_ROOT_TRANSFERTASK_IDS, node.get("all-active-root-transfertask-ids").textValue());
    sqlQueries.put(SqlQuery.SINGLE_NOT_CANCELED_OR_COMPLETED, node.get("single-not-canceled-or-completed").textValue());
    sqlQueries.put(SqlQuery.ALL_CHILDREN_CANCELED_OR_COMPLETED, node.get("all-children-canceled-or-completed").textValue());
    sqlQueries.put(SqlQuery.SET_TRANSFERTASK_CANCELLED_WHERE_NOT_COMPLETED, node.get("set-transfertask-cancelled-where-not-completed").textValue());
    sqlQueries.put(SqlQuery.GET_TRANSFERTASK_TREE, node.get("get-transfertask-tree").textValue());
    sqlQueries.put(SqlQuery.FIND_TRANSFERTASK_BY_ROOT_TASK_ID_SRC_DEST, node.get("find_transfertask_by_root_task_id_src_dest").textValue());
    sqlQueries.put(SqlQuery.PARENTS_NOT_CANCELED_OR_COMPLETED, node.get("parents-not-canceled-or-completed").textValue());
    sqlQueries.put(SqlQuery.CANCEL_ALL_TRANSFERTASKS, node.get("cancel-all-transfertasks").textValue());
    sqlQueries.put(SqlQuery.GET_TRANSFERTASK_BY_ID, node.get("get-transfertask-by-id").textValue());
    sqlQueries.put(SqlQuery.GET_BYTES_TRANSFERRED_FOR_ALL_CHILDREN, node.get("get_bytes_transferred_for_all_children").textValue());
    sqlQueries.put(SqlQuery.PING, node.get("ping").textValue());

    return sqlQueries;
  }


}
// end::dbverticle[]
