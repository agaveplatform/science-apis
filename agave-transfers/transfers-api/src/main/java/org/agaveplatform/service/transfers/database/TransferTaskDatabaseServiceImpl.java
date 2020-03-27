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

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author deardooley
 */
// tag::implementation[]
class TransferTaskDatabaseServiceImpl implements TransferTaskDatabaseService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransferTaskDatabaseServiceImpl.class);

  private final HashMap<SqlQuery, String> sqlQueries;
  private final JDBCClient dbClient;

  TransferTaskDatabaseServiceImpl(JDBCClient dbClient, HashMap<SqlQuery, String> sqlQueries, Handler<AsyncResult<TransferTaskDatabaseService>> readyHandler) {
    this.dbClient = dbClient;
    this.sqlQueries = sqlQueries;

    dbClient.getConnection(ar -> {
      if (ar.failed()) {
        LOGGER.error("Could not open a database connection", ar.cause());
        readyHandler.handle(Future.failedFuture(ar.cause()));
      } else {
        SQLConnection connection = ar.result();
        connection.execute(sqlQueries.get(SqlQuery.CREATE_TRANSFERTASKS_TABLE), create -> {
          connection.close();
          if (create.failed()) {
            LOGGER.error("Database preparation error", create.cause());
            readyHandler.handle(Future.failedFuture(create.cause()));
          } else {
            readyHandler.handle(Future.succeededFuture(this));
          }
        });
      }
    });
  }

  @Override
  public TransferTaskDatabaseService getAll(String tenantId, Handler<AsyncResult<JsonArray>> resultHandler) {
    dbClient.query(sqlQueries.get(SqlQuery.ALL_TRANSFERTASKS), res -> {
      if (res.succeeded()) {
        JsonArray response = new JsonArray(res.result()
          .getRows()
          .stream()
          .map(TransferTask::new)
          .collect(Collectors.toList()));
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Database query error", res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  @Override
  public TransferTaskDatabaseService getById(String tenantId, String uuid, Handler<AsyncResult<JsonObject>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(uuid)
            .add(tenantId);
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_TRANSFERTASK), data, fetch -> {
      if (fetch.succeeded()) {
        JsonObject response = new JsonObject();
        ResultSet resultSet = fetch.result();
        if (resultSet.getNumRows() == 0) {
          response.put("found", false);
        } else {
          List<JsonObject> rows = resultSet.getRows();
          response.put("found", true);
          JsonObject row = rows.get(0);
          response.put("transfertask", row);
        }
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Database query error", fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  @Override
  public TransferTaskDatabaseService allChildrenCancelledOrCompleted(String tenantId, String uuid, Handler<AsyncResult<Boolean>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(uuid)
            .add(tenantId);
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.ALL_TRANSFERTASK_CHILDREN_CANCELLED_OR_COMPLETED), data, fetch -> {
      if (fetch.succeeded()) {
        ResultSet resultSet = fetch.result();
        Boolean response = Boolean.FALSE;
        if (resultSet.getNumRows() == 1 && resultSet.getRows().get(0).getInteger("active_child_count") > 0) {
          response = Boolean.TRUE;
        }
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Database query error", fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  @Override
  public TransferTaskDatabaseService create(String tenantId, TransferTask transferTask, Handler<AsyncResult<JsonObject>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(transferTask.getAttempts())
            .add(transferTask.getBytesTransferred())
            .add(transferTask.getCreated())
            .add(transferTask.getDest())
            .add(transferTask.getEndTime())
            .add(transferTask.getEventId())
            .add(transferTask.getLastUpdated())
            .add(transferTask.getOwner())
            .add(transferTask.getSource())
            .add(transferTask.getStartTime())
            .add(transferTask.getStatus())
            .add(tenantId)
            .add(transferTask.getTotalSize())
            .add(transferTask.getTransferRate())
            .add(transferTask.getParentTaskId())
            .add(transferTask.getRootTaskId())
            .add(transferTask.getUuid())
            .add(transferTask.getTotalFiles())
            .add(transferTask.getTotalSkippedFiles());
    dbClient.updateWithParams(sqlQueries.get(SqlQuery.CREATE_TRANSFERTASK), data, res -> {
      if (res.succeeded()) {
        JsonObject response = new JsonObject().put("transfertask", res.result().toJson());
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Database query error", res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  @Override
  public TransferTaskDatabaseService update(String tenantId, String uuid, TransferTask transferTask, Handler<AsyncResult<JsonObject>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(transferTask.getAttempts())
            .add(transferTask.getBytesTransferred())
            .add(transferTask.getEndTime())
            .add(transferTask.getOwner())
            .add(transferTask.getStartTime())
            .add(transferTask.getStatus())
            .add(transferTask.getTotalSize())
            .add(transferTask.getTransferRate())
            .add(transferTask.getTotalFiles())
            .add(transferTask.getTotalSkippedFiles())
            .add(uuid)
            .add(tenantId);
    dbClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_TRANSFERTASK), data, res -> {
      if (res.succeeded()) {
        JsonObject response = new JsonObject().put("transfertask", res.result().toJson());
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Database query error", res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  @Override
  public TransferTaskDatabaseService updateStatus(String tenantId, String uuid, String status, Handler<AsyncResult<JsonObject>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(status)
            .add(uuid)
            .add(tenantId);
    dbClient.updateWithParams(sqlQueries.get(SqlQuery.SAVE_TRANSFERTASK), data, res -> {
      if (res.succeeded()) {
        JsonObject response = new JsonObject().put("transfertask", res.result().toJson());
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Database query error", res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  @Override
  public TransferTaskDatabaseService delete(String tenantId, String uuid, Handler<AsyncResult<Void>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(uuid)
            .add(tenantId);
    dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_TRANSFERTASK), data, res -> {
      if (res.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        LOGGER.error("Database query error", res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }
}
// end::implementation[]
