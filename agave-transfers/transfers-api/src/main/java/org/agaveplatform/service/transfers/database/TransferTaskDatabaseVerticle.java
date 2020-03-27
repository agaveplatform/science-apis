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
import org.apache.commons.io.IOUtils;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Properties;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;
/**
 * @author deardooley
 */
// tag::dbverticle[]
public class TransferTaskDatabaseVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> promise) throws Exception {

    HashMap<SqlQuery, String> sqlQueries = loadSqlQueries();

    JDBCClient dbClient = JDBCClient.createShared(vertx, new JsonObject()
      .put("url", config().getString(CONFIG_TRANSFERTASK_DB_JDBC_URL, "jdbc:hsqldb:mem:db/wiki"))
      .put("driver_class", config().getString(CONFIG_TRANSFERTASK_DB_JDBC_DRIVER_CLASS, "org.hsqldb.jdbcDriver"))
      .put("max_pool_size", config().getInteger(CONFIG_TRANSFERTASK_DB_JDBC_MAX_POOL_SIZE, 30)), "agave-io");

    TransferTaskDatabaseService.create(dbClient, sqlQueries, ready -> {
      if (ready.succeeded()) {
        ServiceBinder binder = new ServiceBinder(vertx);
        binder
          .setAddress(CONFIG_TRANSFERTASK_DB_QUEUE)
          .register(TransferTaskDatabaseService.class, ready.result());
        promise.complete();
      } else {
        promise.fail(ready.cause());
      }
    });
  }

  /*
   * Note: this uses blocking APIs, but data is small...
   */
  private HashMap<SqlQuery, String> loadSqlQueries() throws IOException {

    String queriesFile = config().getString(CONFIG_TRANSFERTASK_DB_SQL_QUERIES_RESOURCE_FILE);

    InputStream queriesInputStream;
    if (queriesFile != null) {
      queriesInputStream = new FileInputStream(queriesFile);
    } else {
      queriesInputStream = getClass().getResourceAsStream("/db-queries.yml");
    }

    String content = IOUtils.toString(queriesInputStream);
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    JsonNode node = mapper.readTree(content);

    HashMap<SqlQuery, String> sqlQueries = new HashMap<>();
    sqlQueries.put(SqlQuery.CREATE_TRANSFERTASKS_TABLE, node.get("create-transfertasks-table").textValue());
    sqlQueries.put(SqlQuery.ALL_TRANSFERTASKS, node.get("all-transfertasks").textValue());
    sqlQueries.put(SqlQuery.GET_TRANSFERTASK, node.get("get-transfertask").textValue());
    sqlQueries.put(SqlQuery.CREATE_TRANSFERTASK, node.get("create-transfertask").textValue());
    sqlQueries.put(SqlQuery.SAVE_TRANSFERTASK, node.get("save-transfertask").textValue());
    sqlQueries.put(SqlQuery.DELETE_TRANSFERTASK, node.get("delete-transfertask").textValue());
    sqlQueries.put(SqlQuery.UPDATE_TRANSFERTASK_STATUS, node.get("update-transfertask-status").textValue());
    sqlQueries.put(SqlQuery.UPDATE_TRANSFERTASK_PROGRESS, node.get("update-transfertask-progress").textValue());
    sqlQueries.put(SqlQuery.ALL_TRANSFERTASK_CHILDREN_CANCELLED_OR_COMPLETED, node.get("all-transfertask-children-cancelled-or-completed").textValue());
    return sqlQueries;
  }
}
// end::dbverticle[]
