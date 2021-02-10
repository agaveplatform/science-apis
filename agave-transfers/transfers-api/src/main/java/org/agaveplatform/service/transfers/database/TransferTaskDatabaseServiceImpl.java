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
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.exception.ObjectNotFoundException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

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
        connection.close();
        readyHandler.handle(Future.succeededFuture(this));
      }
    });
  }

//  getTransferTaskTree
  /**
   * Retrieve tree of parent and children {@link TransferTask}
   *
   * @param tenantId the tenant of root {@link TransferTask}
   * @param uuid the id of root {@link TransferTask}
   * @param resultHandler the handler to resolve with {@link JsonArray} of parent and children {@link TransferTask}
   **/
  @Override
  public TransferTaskDatabaseService getTransferTaskTree(String tenantId, String uuid, Handler<AsyncResult<JsonArray>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(uuid)
            .add(tenantId)
            .add(uuid)
            .add(tenantId);

    dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_TRANSFERTASK_TREE), data, res -> {
      if (res.succeeded()) {
        JsonArray response = new JsonArray(res.result().getRows());
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Failed to fetch the entire transfer task tree for root task {}", uuid, res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  /**
   * Retrieve all children of parent with id {@code uuid} that are cancelled or completed
   *
   * @param tenantId the tenant of parent {@link TransferTask}
   * @param uuid the id of parent {@link TransferTask}
   * @param resultHandler the handler to resolve with {@link JsonArray} of all cancelled or completed children {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService getAllChildrenCanceledOrCompleted(String tenantId, String uuid, Handler<AsyncResult<JsonArray>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(tenantId)
            .add(uuid);

    dbClient.queryWithParams(sqlQueries.get(SqlQuery.ALL_CHILDREN_CANCELED_OR_COMPLETED), data, res -> {
      if (res.succeeded()) {
        JsonArray response = new JsonArray(res.result().getRows());
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Failed to fetch all CANCELLED or COMPLETED child transfer tasks of {}.", uuid, res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  /**
   * Set {@link TransferTask} with status of {@link TransferStatusType#CANCELLED} if not completed or failed
   *
   * @param tenantId the tenant of {@link TransferTask}
   * @param uuid the id of {@link TransferTask}
   * @param resultHandler the handler to resolve with {@link Boolean#TRUE} on successful update
   */
  @Override
  public TransferTaskDatabaseService setTransferTaskCanceledWhereNotCompleted(String tenantId, String uuid, Handler<AsyncResult<Boolean>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(tenantId)
            .add(uuid);

    dbClient.updateWithParams(sqlQueries.get(SqlQuery.SET_TRANSFERTASK_CANCELLED_WHERE_NOT_COMPLETED), data, res -> {
      if (res.succeeded()) {
//        JsonArray response = new JsonArray(res.result().getRows());
        resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
      } else {
        LOGGER.error("Failed to cancel all active child transfer tasks of {}.", uuid, res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  /**
   * Retrieve all {@link TransferTask}
   *
   * @param tenantId the tenant of {@link TransferTask}
   * @param limit number of {@link TransferTask} to return
   * @param offset number of {@link TransferTask} to offset in search
   * @param resultHandler the handler to resolve with {@link JsonArray} of the matching {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService getAll(String tenantId, int limit, int offset, Handler<AsyncResult<JsonArray>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(tenantId)
            .add(Math.min(Settings.MAX_PAGE_SIZE, Math.max(0, limit))) // throttle the max response size to preserve performance
            .add(Math.max(0, offset)); // ensure pagination is positive
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.ALL_TRANSFERTASKS), data, res -> {
      if (res.succeeded()) {
        JsonArray response = new JsonArray(res.result().getRows());
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Failed to fetch paginated transfer tasks.", res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  /**
   * Retrieve all {@link TransferTask} matching with owner {@code username}
   *
   * @param tenantId the tenant of {@link TransferTask}
   * @param username the id of {@link TransferTask}
   * @param limit number of {@link TransferTask} to return
   * @param offset number of {@link TransferTask} to offset in search
   * @param resultHandler the handler to resolve with {@link JsonArray} of the matching {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService getAllForUser(String tenantId, String username, int limit, int offset, Handler<AsyncResult<JsonArray>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(username)
            .add(tenantId)
            .add(Math.min(Settings.MAX_PAGE_SIZE, Math.max(0, limit))) // throttle the max response size to preserve performance
            .add(Math.max(0, offset)); // ensure pagination is positive
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.ALL_USER_TRANSFERTASKS), data, res -> {
      if (res.succeeded()) {
        JsonArray response = new JsonArray(res.result().getRows());
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Failed to fetch all transfer tasks for user {}.", username, res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  /**
   * Retrieve {@link TransferTask} matching the {@code id} for partial {@link TransferTask}
   * @param id datebase id of the {@link TransferTask}
   * @param resultHandler the handler to resolve with {@link JsonObject} of the matching {@link TransferTask}
   * @return
   */
  @Override
  public TransferTaskDatabaseService getById(String id, Handler<AsyncResult<JsonObject>> resultHandler){
    JsonArray data = new JsonArray()
            .add(id);
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_TRANSFERTASK_BY_ID), data, ar -> {
      if (ar.succeeded()) {
        resultHandler.handle(Future.succeededFuture(ar.result().getRows().get(0)));
      } else {
        LOGGER.error("Failed to fetch transfer task record with id {} after insert. Database query error", id, ar.cause());
        resultHandler.handle(Future.failedFuture(ar.cause()));
      }
    });
    return this;
  }

  /**
   * Retrieve {@link TransferTask} matching {@code uuid} and {@code tenantId}
   *
   * @param tenantId the tenant of {@link TransferTask}
   * @param uuid the id of {@link TransferTask}
   * @param resultHandler the handler to resolve with {@link JsonObject} of the matching {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService getByUuid(String tenantId, String uuid, Handler<AsyncResult<JsonObject>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(uuid)
            .add(tenantId);
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_TRANSFERTASK), data, fetch -> {
      if (fetch.succeeded()) {
        JsonObject response = new JsonObject();
        ResultSet resultSet = fetch.result();
        if (resultSet.getNumRows() == 0) {
          resultHandler.handle(Future.succeededFuture(null));//new ObjectNotFoundException("No transfer task found")));
        } else {
          resultHandler.handle(Future.succeededFuture(resultSet.getRows().get(0)));
        }
      } else {
        LOGGER.error("Failed to query for transfer task {} by uuid.", uuid, fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  /**
   * Retrieve the uuid and tenant_id of all current {@link TransferStatusType#isActive()} {@link TransferTask}
   *
   * @param resultHandler the handler to resolve with {@link JsonArray} of the uuid and tenant_id of all active root tasks
   */
  public TransferTaskDatabaseService getActiveRootTaskIds(Handler<AsyncResult<JsonArray>> resultHandler) {
    try {
      dbClient.query(sqlQueries.get(SqlQuery.ALL_ACTIVE_ROOT_TRANSFERTASK_IDS), fetch -> {
        if (fetch.succeeded()) {
          JsonArray response = new JsonArray(fetch.result().getRows());
          if (response.isEmpty()) {
            LOGGER.info("getActiveRootTaskIds is empty");
            resultHandler.handle(Future.succeededFuture(response));
          } else {
            LOGGER.info("getActiveRootTaskIds is not empty");
            LOGGER.info("Num Rows in query for getActiveRootTaskIds is = {}", fetch.result().getNumRows());
            resultHandler.handle(Future.succeededFuture(response));
          }
        } else {
          LOGGER.error("Failed to query active root transfer tasks.", fetch.cause());
          resultHandler.handle(Future.failedFuture(fetch.cause()));
        }
      });
    }catch(Exception e) {
      LOGGER.error(e.toString());
    }
    return this;
  }

  /**
   * Retrieve all parent {@link TransferTask} ids that are not cancelled/completed
   * but all children are cancelled/completed
   *
   * @param resultHandler the handler to resolve with {@link JsonArray} of parent {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService getAllParentsCanceledOrCompleted( Handler<AsyncResult<JsonArray>> resultHandler){
    LOGGER.trace("Got into db.getAllParentsCanceledOrCompleted");

    try{
      dbClient.query(sqlQueries.get(SqlQuery.PARENTS_NOT_CANCELED_OR_COMPLETED), fetch -> {

      if (fetch.succeeded()) {
        //ResultSet resultSet = fetch.result();
        JsonArray response = new JsonArray(fetch.result().getRows());
        if (response.isEmpty()) {
          LOGGER.debug("PARENTS_NOT_CANCELED_OR_COMPLETED is empty - all parents inactive");
          resultHandler.handle(Future.succeededFuture(response));
        } else {
          LOGGER.info("Parents not canceled or completed is not empty - some parents active");
          resultHandler.handle(Future.succeededFuture(response));
        }
      } else {
        LOGGER.error("Failed to query for existence of any child transfer tasks not in a CANCELED or COMPLETED state.", fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    }catch(Exception e) {
      LOGGER.error(e.toString());
    }
    return this;
  }


  /**
   * Checks the status of all child tasks for the parent and returns true if all child tasks have a status that is not
   * {@link TransferStatusType#isActive()}
   *
   * @param tenantId the tenant of the parent {@link TransferTask}
   * @param uuid the id of the parent {@link TransferTask}
   * @param resultHandler the handler to resolve with {@link Boolean#TRUE} if all children of with parent_id of
   * {@code uuid} is cancelled or completed
   *
   * @return
   */
  @Override
  public TransferTaskDatabaseService allChildrenCancelledOrCompleted(String tenantId, String uuid, Handler<AsyncResult<Boolean>> resultHandler) {
    LOGGER.trace("Got into db.allChildrenCancelledOrCompleted");
    JsonArray data = new JsonArray()
            .add(tenantId)
            .add(uuid);

    //first query if any children are active
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.ALL_TRANSFERTASK_CHILDREN_ACTIVE), data, fetch -> {
      LOGGER.info("dbClient.queryWithParams(sqlQueries.get(SqlQuery.ALL_TRANSFERTASK_CHILDREN_CANCELLED_OR_COMPLETED)");
      if (fetch.succeeded()) {
        ResultSet resultSet = fetch.result();
        LOGGER.info("db.allChildrenCancelledOrCompleted, Number of rows = {}", resultSet.getNumRows());

        Boolean response = Boolean.FALSE;
        if (resultSet.getNumRows() == 1 && resultSet.getRows().get(0).getInteger("active_child_count") == 0) {
          //no active children
          LOGGER.info("db.allChildrenCancelledOrCompleted {}, no active children", uuid);
          response = Boolean.TRUE;
        }
        else if (resultSet.getNumRows() == 1 && resultSet.getRows().get(0).getInteger("active_child_count") > 0) {
          //active children
          LOGGER.info("db.allChildrenCancelledOrCompleted {}, active children", uuid);
          response = Boolean.FALSE;
        } else if (resultSet.getNumRows() < 1 && resultSet.getRows().get(0).getInteger("active_child_count") < 0){
          // this should be marked as status of 'ERROR' since we don't know what caused the task to fail
          LOGGER.trace("active_child_count = {}", resultSet.getRows().get(0).getInteger("active_child_count"));
          LOGGER.info("This should be marked as status of 'ERROR' since we don't know what caused the task to fail uuid = {}", uuid);
          response = Boolean.FALSE;
        }
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Failed to query for existence of any child transfer tasks of {} not in a CANCELLED or COMPLETED state.", uuid, fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  /**
   * Checks the status of {@link TransferTask} with {@code uuid} and returns true if the task status is
   * {@link TransferStatusType#isActive()}
   *
   * @param tenantId the tenant of the {@link TransferTask}
   * @param uuid the id of the {@link TransferTask}
   * @param resultHandler the handler to resolve with {@link Boolean#TRUE} if there is at least one task that is not
   *                      cancelled or completed (active)
   *
   * @return
   */
  @Override
  public TransferTaskDatabaseService singleNotCancelledOrCompleted(String tenantId, String uuid, Handler<AsyncResult<Boolean>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(uuid)
            .add(tenantId);
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.SINGLE_NOT_CANCELED_OR_COMPLETED), data, fetch -> {
      if (fetch.succeeded()) {
        ResultSet resultSet = fetch.result();
        Boolean response = Boolean.FALSE;
        if (resultSet.getNumRows() == 1 && resultSet.getRows().get(0).getInteger("active_child_count") > 0) {
          response = Boolean.TRUE;
        }
        resultHandler.handle(Future.succeededFuture(response));
      } else {
        LOGGER.error("Failed to query for inactive child transfer tasks of {}.", uuid, fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  /**
   * Create {@link TransferTask}
   *
   * @param tenantId the tenant of the {@link TransferTask} to add
   * @param transferTask the {@link TransferTask} to add
   * @param resultHandler the handler to resolve with {@link JsonObject} of added {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService create(String tenantId, TransferTask transferTask, Handler<AsyncResult<JsonObject>> resultHandler) {
    final JsonArray data = new JsonArray()
            .add(transferTask.getAttempts())
            .add(transferTask.getBytesTransferred())
            .add(transferTask.getCreated())
            .add(transferTask.getDest())
            .add(transferTask.getEndTime())
            .add(transferTask.getEventId())
            .add(transferTask.getLastUpdated())
            .add(transferTask.getLastAttempt())
            .add(transferTask.getNextAttempt())
            .add(transferTask.getOwner())
            .add(transferTask.getSource())
            .add(transferTask.getStartTime())
            .add(transferTask.getStatus().name())
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
        int transferTaskId = res.result().getKeys().getInteger(0);
        JsonArray params = new JsonArray()
                .add(transferTask.getUuid())
                .add(tenantId);
        dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_TRANSFERTASK), params, ar -> {
          if (ar.succeeded()) {
            resultHandler.handle(Future.succeededFuture(ar.result().getRows().get(0)));
          } else {
            LOGGER.error("Failed to fetch new transfer task record after insert. Database query error", ar.cause());
            resultHandler.handle(Future.failedFuture(ar.cause()));
          }
        });

      } else {
        LOGGER.error("Failed to save new transfertask record: {}", res.cause().getMessage(), res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  /**
   * @deprecated
   * @see TransferTaskDatabaseService#updateStatus(String, String, String, Handler)
   *
   * Update {@link TransferTask} status matching {@code id}
   *
   * @param id of the {@link TransferTask}
   * @param statusChangeTo the {@link TransferStatusType} to be updated
   * @param resultHandler the handler to resolve with {@link JsonObject} of updated {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService updateById(String id, String statusChangeTo, Handler<AsyncResult<JsonObject>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(statusChangeTo)
            .add(id);
    dbClient.getConnection(conn -> {
      conn.result().setAutoCommit(false, res -> {
        if (res.failed()) {
          throw new RuntimeException(res.cause());
        }

        conn.result().updateWithParams(sqlQueries.get(SqlQuery.CANCEL_TRANSFERTASK_BY_ID), data, update -> {
          if (update.succeeded()) {
            conn.result().commit(commit -> {
              if (commit.failed()) {
                conn.result().rollback(rollback -> {
                  if (res.failed()) {
                    LOGGER.error("Failed to commit transaction after updating transfer task record {}. Database rollback failed. Data may be corrupted.", id, rollback.cause());
                    resultHandler.handle(Future.failedFuture(rollback.cause()));
                  } else {
                    LOGGER.error("Failed to commit transaction after updating transfer task record {}. Database rollback succeeded.", id, commit.cause());
                    resultHandler.handle(Future.failedFuture(commit.cause()));
                  }
                });
              } else {
                JsonArray params = new JsonArray().add(id);
                conn.result().queryWithParams(sqlQueries.get(SqlQuery.GET_TRANSFERTASK_BY_ID), params, ar -> {
                  if (ar.succeeded()) {
                    resultHandler.handle(Future.succeededFuture(ar.result().getRows().get(0)));
                  } else {
                    LOGGER.error("Failed to fetch updated transfer task record for {} after insert. Database query error", id, ar.cause());
                    resultHandler.handle(Future.failedFuture(ar.cause()));
                  }
                });
              }
            });
          } else {
            LOGGER.error("Failed to fetch new transfer task record after insert. Database query error", update.cause());
            resultHandler.handle(Future.failedFuture(update.cause()));
          }
        });
      });
    });
    return this;
  }


  /**
   * Update entire {@link TransferTask} with {@code transferTask}
   *
   * @param tenantId of the {@link TransferTask}
   * @param uuid id of the {@link TransferTask}
   * @param transferTask the {@link TransferTask} to update with
   * @param resultHandler the handler to resolve with {@link JsonObject} of updated {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService update(String tenantId, String uuid, TransferTask transferTask, Handler<AsyncResult<JsonObject>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(transferTask.getAttempts())
            .add(transferTask.getBytesTransferred())
            .add(transferTask.getEndTime())
            .add(transferTask.getStartTime())
            .add(transferTask.getLastAttempt())
            .add(transferTask.getNextAttempt())
            .add(transferTask.getStatus())
            .add(transferTask.getTotalSize())
            .add(transferTask.getTransferRate())
            .add(transferTask.getTotalFiles())
            .add(transferTask.getTotalSkippedFiles())
            .add(uuid)
            .add(tenantId);
    dbClient.getConnection(conn -> {
      conn.result().setAutoCommit(false, res -> {
        if (res.failed()) {
          throw new RuntimeException(res.cause());
        }

        conn.result().updateWithParams(sqlQueries.get(SqlQuery.SAVE_TRANSFERTASK), data, update -> {
          if (update.succeeded()) {
            conn.result().commit(commit -> {
              if (commit.failed()) {
                conn.result().rollback(rollback -> {
                  if (res.failed()) {
                    LOGGER.error("Failed to commit transaction after updating transfer task record {}. Database rollback failed. Data may be corrupted.", uuid, rollback.cause());
                    resultHandler.handle(Future.failedFuture(rollback.cause()));
                  } else {
                    LOGGER.error("Failed to commit transaction after updating transfer task record {}. Database rollback succeeded.", uuid, commit.cause());
                    resultHandler.handle(Future.failedFuture(commit.cause()));
                  }
                });
              } else {
                JsonArray params = new JsonArray().add(uuid).add(tenantId);
                conn.result().queryWithParams(sqlQueries.get(SqlQuery.GET_TRANSFERTASK), params, ar -> {
                  if (ar.succeeded()) {
                    resultHandler.handle(Future.succeededFuture(ar.result().getRows().get(0)));
                  } else {
                    LOGGER.error("Failed to fetch updated transfer task record for {} after insert. Database query error", uuid, ar.cause());
                    resultHandler.handle(Future.failedFuture(ar.cause()));
                  }
                });
              }
            });
          } else {
            LOGGER.error("Failed to fetch new transfer task record after insert. Database query error", update.cause());
            resultHandler.handle(Future.failedFuture(update.cause()));
          }
        });
      });
    });
    return this;
  }

  /**
   * Find child {@link TransferTask} with a root task matching {@code rootTaskId}, source {@code src}, and destination {@code dest}
   *
   * @param tenantId of the root{@link TransferTask}
   * @param rootTaskId id of the root{@link TransferTask}
   * @param src source path of {@link TransferTask}
   * @param dest destination path of {@link TransferTask}
   * @param resultHandler the handler to resolve with {@link JsonObject} of the matching {@link TransferTask},
   *                      or null if no matching {@link TransferTask} found
   */
  @Override
  public TransferTaskDatabaseService findChildTransferTask(String tenantId, String rootTaskId, String src, String dest, Handler<AsyncResult<JsonObject>> resultHandler) {
    final JsonArray data = new JsonArray()
            .add(rootTaskId)
            .add(src)
            .add(dest)
            .add(tenantId);
    dbClient.queryWithParams(sqlQueries.get(SqlQuery.FIND_TRANSFERTASK_BY_ROOT_TASK_ID_SRC_DEST), data, fetch -> {
      if (fetch.succeeded()) {
        ResultSet resultSet = fetch.result();
        if (resultSet.getNumRows() == 0) {
          LOGGER.debug("No child transfer task found with root task {}, {} -> {} in tenant {}.", rootTaskId, src, dest, tenantId);
          resultHandler.handle(Future.succeededFuture(null));
        } else {
          LOGGER.debug("Matching child transfer task found with root task {}, {} -> {} in tenant {}.", rootTaskId, src, dest, tenantId);
          resultHandler.handle(Future.succeededFuture(resultSet.getRows().get(0)));
        }
      } else {
        LOGGER.error("Failed to query for child transfertask record: {}", fetch.cause().getMessage(), fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  /**
   * Create a child {@link TransferTask} if it doesn't exist, otherwise update the existing task
   *
   * @param tenantId of the child{@link TransferTask}
   * @param childTransferTask {@link TransferTask} to be created or updated
   * @param resultHandler the handler to resolve with {@link JsonObject} of the added/updated {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService createOrUpdateChildTransferTask(String tenantId, TransferTask childTransferTask, Handler<AsyncResult<JsonObject>> resultHandler) {
    findChildTransferTask(tenantId, childTransferTask.getRootTaskId(), childTransferTask.getSource(), childTransferTask.getDest(), fetch -> {
      if (fetch.succeeded()) {
        // no match. we need to create a new TransferTask
        if (fetch.result() == null) {
          LOGGER.debug("Creating child transfer task with root task {}, {} -> {} in tenant {}", childTransferTask.getRootTaskId(), childTransferTask.getSource(), childTransferTask.getDest(), tenantId);
          create(tenantId, childTransferTask, resultHandler);
        } else {
          // found a match, just need to update the status
          LOGGER.debug("Found existing transfer task with root task {}, {} -> {} in tenant {}. Task will be updated.", childTransferTask.getRootTaskId(), childTransferTask.getSource(), childTransferTask.getDest(), tenantId);
          updateStatus(tenantId, fetch.result().getString("uuid"), childTransferTask.getStatus().name(), resultHandler);
        }
      } else {
        LOGGER.error("Failed to update child transfertask record: {}", fetch.cause().getMessage(), fetch.cause());
        resultHandler.handle(Future.failedFuture(fetch.cause()));
      }
    });
    return this;
  }

  /**
   * Update {@link TransferTask} status matching {@code id}
   *
   * @param tenantId of the {@link TransferTask}
   * @param uuid id of the {@link TransferTask}
   * @param status the {@link TransferStatusType} to be updated
   * @param resultHandler the handler to resolve with {@link JsonObject} of updated {@link TransferTask}
   */
  @Override
  public TransferTaskDatabaseService updateStatus(String tenantId, String uuid, String status, Handler<AsyncResult<JsonObject>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(status)
            .add(uuid)
            .add(tenantId);
    dbClient.updateWithParams(sqlQueries.get(SqlQuery.UPDATE_TRANSFERTASK_STATUS), data, res -> {
      if (res.succeeded()) {
        JsonArray params = new JsonArray().add(uuid).add(tenantId);
        dbClient.queryWithParams(sqlQueries.get(SqlQuery.GET_TRANSFERTASK), params, ar -> {
          if (ar.succeeded()) {
            LOGGER.info("updateStatus succeeded");
            if (ar.result().getRows().isEmpty()) {
              LOGGER.info("Number of rows updates is 0.");
              resultHandler.handle(Future.failedFuture(new ObjectNotFoundException("Transform task was deleted by another process after the update completed successfully. ")));
            } else {
              LOGGER.info("updateStatus succeeded, result = " + ar.result().getRows().get(0));
              resultHandler.handle(Future.succeededFuture(ar.result().getRows().get(0)));
            }
          } else {
            LOGGER.error("Database query error", res.cause());
            resultHandler.handle(Future.failedFuture(res.cause()));
          }
        });
      } else {
        LOGGER.error("Database query error", res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  /**
   * Delete {@link TransferTask} matching {@code uuid}
   *
   * @param tenantId of the {@link TransferTask}
   * @param uuid id of the {@link TransferTask}
   * @param resultHandler the handler to resolve with empty result on successful delete
   */
  @Override
  public TransferTaskDatabaseService delete(String tenantId, String uuid, Handler<AsyncResult<Void>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(uuid)
            .add(tenantId);
    dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_TRANSFERTASK), data, res -> {
      if (res.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        LOGGER.error("Failed to delete transfer task {}.", uuid, res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }

  /**
   * Delete all {@link TransferTask} from table
   *
   * @param tenantId of the {@link TransferTask}
   * @param resultHandler the handler to resolve with empty result on successful delete
   */
  @Override
  public TransferTaskDatabaseService deleteAll(String tenantId, Handler<AsyncResult<Void>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(tenantId);
    dbClient.updateWithParams(sqlQueries.get(SqlQuery.DELETE_ALL_TRANSFERTASKS), data, res -> {
      if (res.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        LOGGER.error("Failed to delete all transfer tasks for tenant {}.", tenantId, res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }


  /**
   * Set status of all {@link TransferTask} to {@link TransferStatusType#CANCELING_WAITING}
   *
   * @param tenantId of the {@link TransferTask}
   * @param resultHandler the handler to resolve with empty result on successful cancel
   */
  @Override
  public TransferTaskDatabaseService cancelAll(String tenantId, Handler<AsyncResult<Void>> resultHandler) {
    JsonArray data = new JsonArray()
            .add(tenantId);
    dbClient.updateWithParams(sqlQueries.get(SqlQuery.CANCEL_ALL_TRANSFERTASKS), data, res -> {
      if (res.succeeded()) {
        resultHandler.handle(Future.succeededFuture());
      } else {
        LOGGER.error("Failed to delete all transfer tasks for tenant {}.", tenantId, res.cause());
        resultHandler.handle(Future.failedFuture(res.cause()));
      }
    });
    return this;
  }
}


// end::implementation[]
