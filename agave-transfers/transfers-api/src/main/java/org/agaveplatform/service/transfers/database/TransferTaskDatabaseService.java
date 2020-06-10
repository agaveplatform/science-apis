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

import io.vertx.codegen.annotations.Fluent;
import io.vertx.codegen.annotations.GenIgnore;
import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.codegen.annotations.VertxGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import org.agaveplatform.service.transfers.model.TransferTask;

import java.util.HashMap;

/**
 * @author <a href="https://julien.ponge.org/">Julien Ponge</a>
 */
// tag::interface[]
@ProxyGen
@VertxGen
public interface TransferTaskDatabaseService {

  @Fluent
  TransferTaskDatabaseService getAll(String tenantId, int limit, int offset, Handler<AsyncResult<JsonArray>> resultHandler);

  @Fluent
  TransferTaskDatabaseService getAllForUser(String tenantId, String username, int limit, int offset, Handler<AsyncResult<JsonArray>> resultHandler);

  @Fluent
  TransferTaskDatabaseService getAllChildrenCanceledOrCompleted(String tenantId, String uuid, Handler<AsyncResult<JsonArray>> resultHandler);

  @Fluent
  TransferTaskDatabaseService getTransferTaskTree(String tenantId, String uuid, Handler<AsyncResult<JsonArray>> resultHandler);

  @Fluent
  TransferTaskDatabaseService setTransferTaskCanceledWhereNotCompleted(String tenantId, String uuid, Handler<AsyncResult<Boolean>> resultHandler);

  @Fluent
  TransferTaskDatabaseService getById(String tenantId, String uuid, Handler<AsyncResult<JsonObject>> resultHandler);

  @Fluent
  TransferTaskDatabaseService getActiveRootTaskIds(Handler<AsyncResult<JsonArray>> resultHandler);
  
  @Fluent
  TransferTaskDatabaseService allChildrenCancelledOrCompleted(String tenantId, String uuid, Handler<AsyncResult<Boolean>> resultHandler);

  @Fluent
  TransferTaskDatabaseService singleNotCancelledOrCompleted(String tenantId, String uuid, Handler<AsyncResult<Boolean>> resultHandler);

  @Fluent
  TransferTaskDatabaseService create(String tenantId, TransferTask transferTask, Handler<AsyncResult<JsonObject>> resultHandler);

  @Fluent
  TransferTaskDatabaseService update(String tenantId, String uuid, TransferTask transferTask, Handler<AsyncResult<JsonObject>> resultHandler);

  @Fluent
  TransferTaskDatabaseService updateStatus(String tenantId, String uuid, String status, Handler<AsyncResult<JsonObject>> resultHandler);

  @Fluent
  TransferTaskDatabaseService delete(String tenantId, String uuid, Handler<AsyncResult<Void>> resultHandler);


  @GenIgnore
  static TransferTaskDatabaseService create(JDBCClient dbClient, HashMap<SqlQuery, String> sqlQueries, Handler<AsyncResult<TransferTaskDatabaseService>> readyHandler) {
    return new TransferTaskDatabaseServiceImpl(dbClient, sqlQueries, readyHandler);
  }

  @GenIgnore
  static TransferTaskDatabaseService createProxy(Vertx vertx, String address) {
    return new TransferTaskDatabaseServiceVertxEBProxy(vertx, address);
  }

}
