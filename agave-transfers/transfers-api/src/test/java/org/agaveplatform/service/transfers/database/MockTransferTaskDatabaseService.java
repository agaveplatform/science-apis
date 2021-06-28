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
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.time.Instant;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Mocks a {@link TransferTaskDatabaseService} using a builder interface.
 */
public class MockTransferTaskDatabaseService {

  public static class Builder {
    TransferTaskDatabaseService dbService;

    public Builder() {
      dbService = mock(TransferTaskDatabaseService.class,
              withSettings().defaultAnswer(new Answer() {
                @Override
                public Object answer(InvocationOnMock invocation) throws Throwable {
                  for (Object arg: invocation.getArguments()) {
                    if (arg instanceof Handler) {
                      AsyncResult asyncResult = mock(AsyncResult.class);
                      when(asyncResult.result()).thenReturn(null);
                      when(asyncResult.succeeded()).thenReturn(false);
                      when(asyncResult.failed()).thenReturn(true);
                      when(asyncResult.cause()).thenReturn(new RuntimeException("Method not mocked."));
                      ((Handler)arg).handle(asyncResult);
                      return null;
                    }
                  }
                  return null;
                }
              }));
      //new ThrowsException(new RuntimeException("Method not mocked."))));
    }

    public Builder getAll(JsonArray result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {

        AsyncResult<JsonArray> resultHandler;
        if (failed) {
          resultHandler = getJsonArrayFailureAsyncResult(new SQLException("Mock test allChildrenCancelledOrCompleted failed"));
        } else {
          resultHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonArray >> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

      return this;
    }

    public Builder getAllForUser(JsonArray result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {

        AsyncResult<JsonArray> resultHandler;
        if (failed) {
          resultHandler = getJsonArrayFailureAsyncResult(new SQLException("Mock test getAllForUser failed"));
        } else {
          resultHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonArray >> handler = arguments.getArgumentAt(4, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).getAllForUser(any(), any(), any(), any(), any());

      return this;
    }

    public Builder getAllChildrenCanceledOrCompleted(JsonArray result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {

        AsyncResult<JsonArray> resultHandler;
        if (failed) {
          resultHandler = getJsonArrayFailureAsyncResult(new SQLException("Mock test getAllChildrenCanceledOrCompleted failed"));
        } else {
          resultHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonArray >> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).getAllChildrenCanceledOrCompleted(any(), any(), any());

      return this;
    }

    public Builder getTransferTaskTree(JsonArray result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {

        AsyncResult<JsonArray> resultHandler;
        if (failed) {
          resultHandler = getJsonArrayFailureAsyncResult(new SQLException("Mock test getTransferTaskTree failed"));
        } else {
          resultHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonArray >> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).getTransferTaskTree(any(), any(), any());

      return this;
    }

    public Builder setTransferTaskCanceledWhereNotCompleted(Boolean result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<Boolean> testHandler;
        if (failed) {
          testHandler = getBooleanFailureAsyncResult(new SQLException("Mock test setTransferTaskCanceledWhereNotCompleted failed"));
        } else {
          testHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(testHandler);
        return null;
      }).when(dbService).setTransferTaskCanceledWhereNotCompleted(any(), any(), any());

      return this;
    }

    public Builder getByUuid(JsonObject result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> testHandler;
        if (failed) {
          testHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test getByUuid failed"));
        } else {
          testHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(testHandler);
        return null;
      }).when(dbService).getByUuid(any(), any(), any());

      return this;
    }

    public Builder getActiveRootTaskIds(JsonArray result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {

        AsyncResult<JsonArray> resultHandler;
        if (failed) {
          resultHandler = getJsonArrayFailureAsyncResult(new SQLException("Mock test getActiveRootTaskIds failed"));
        } else {
          resultHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonArray >> handler = arguments.getArgumentAt(0, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).getActiveRootTaskIds(any());

      return this;
    }

    public Builder getAllParentsCanceledOrCompleted(JsonArray result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {

        AsyncResult<JsonArray> resultHandler;
        if (failed) {
          resultHandler = getJsonArrayFailureAsyncResult(new SQLException("Mock test getAllParentsCanceledOrCompleted failed"));
        } else {
          resultHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonArray >> handler = arguments.getArgumentAt(0, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).getAllParentsCanceledOrCompleted(any());

      return this;
    }

    public Builder allChildrenCancelledOrCompleted(boolean allChildrenCancelledOrCompleted, boolean failed) {
      doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {

        AsyncResult<Boolean> resultHandler;
        if (failed) {
          resultHandler = getBooleanFailureAsyncResult(new SQLException("Mock test allChildrenCancelledOrCompleted failed"));
        } else {
          resultHandler = getMockAsyncResult(allChildrenCancelledOrCompleted);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

      return this;
    }

    public Builder singleNotCancelledOrCompleted(boolean notCancelledOrCompleted, boolean failed) {
      doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {

        AsyncResult<Boolean> resultHandler;
        if (failed) {
          resultHandler = getBooleanFailureAsyncResult(new SQLException("Mock test updateStatus failed"));
        } else {
          resultHandler = getMockAsyncResult(notCancelledOrCompleted);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).singleNotCancelledOrCompleted(any(), any(), any());

      return this;
    }

    public Builder create(JsonObject result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> testHandler;
        if (failed) {
          testHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test create failed"));
        } else {
          testHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(testHandler);
        return null;
      }).when(dbService).create(anyString(), any(TransferTask.class), any());

      return this;
    }

    public Builder updateById(JsonObject result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> testHandler;
        if (failed) {
          testHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test updateById failed"));
        } else {
          testHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(testHandler);
        return null;
      }).when(dbService).updateById(any(), any(), any());

      return this;
    }

    public Builder update(JsonObject result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> testHandler;
        if (failed) {
          testHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test update failed"));
        } else {
          testHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
        handler.handle(testHandler);
        return null;
      }).when(dbService).update(any(), any(), any(), any());

      return this;
    }

    public Builder findChildTransferTask(JsonObject result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> testHandler;
        if (failed) {
          testHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test findChildTransferTask failed"));
        } else {
          testHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(4, Handler.class);
        handler.handle(testHandler);
        return null;
      }).when(dbService).findChildTransferTask(any(), any(), any(), any(), any());

      return this;
    }

    public Builder createOrUpdateChildTransferTask(JsonObject result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> testHandler;
        if (failed) {
          testHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test createOrUpdateChildTransferTask failed"));
        } else {
          testHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(testHandler);
        return null;
      }).when(dbService).createOrUpdateChildTransferTask(any(), any(), any());

      return this;
    }

    public Builder updateStatus(JsonObject result, boolean failed) {
      // mock the handler passed into updateStatus, resolving based on the value of failUpdateStatus
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
        @SuppressWarnings("unchecked")
        String updatedStatusName = arguments.getArgumentAt(2, String.class);
        JsonObject updatedTransferTask = result.copy()
                .put("status", updatedStatusName)
                .put("last_updated", Instant.now());

        AsyncResult<JsonObject> updateStatusHandler;
        if (failed) {
          updateStatusHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test updateStatus failed"));
        } else {
          updateStatusHandler = getMockAsyncResult(updatedTransferTask);
        }
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
        handler.handle(updateStatusHandler);

        return null;
      }).when(dbService).updateStatus(anyString(), any(), anyString(), any());

      return this;
    }

    public Builder getById(JsonObject result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        AsyncResult<JsonObject> resultHandler;
        if (failed) {
          resultHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test getById failed"));
        } else {
          resultHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).getById(anyString(), any());

      return this;
    }

    public Builder getBytesTransferredForAllChildren(JsonObject result, boolean failed) {
      doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {

        AsyncResult<JsonObject> resultHandler;
        if (failed) {
          resultHandler = getJsonObjectFailureAsyncResult(new SQLException("Mock test getBytesTransferredForAllChildren failed"));
        } else {
          resultHandler = getMockAsyncResult(result);
        }

        @SuppressWarnings("unchecked")
        Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
        handler.handle(resultHandler);

        return null;
      }).when(dbService).getBytesTransferredForAllChildren(any(), any(), any());

      return this;
    }

    public TransferTaskDatabaseService build() {
      return dbService;
    }


    /**
     * Creates a mock {@link AsyncResult<Boolean>} that can be used as a handler controlling
     * the success outcomes
     *
     * @param result the expected value of the call to {@link AsyncResult#result()}
     * @return a valid mock for testing boolean result behavior
     */
    protected AsyncResult<Boolean> getMockAsyncResult(Boolean result) {
      AsyncResult<Boolean> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(result);
      when(asyncResult.succeeded()).thenReturn(true);
      when(asyncResult.failed()).thenReturn(false);
      when(asyncResult.cause()).thenReturn(null);

      return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<Boolean>} that can be used as a handler controlling
     * failure outcomes and response
     *
     * @param cause the expected exception to be bubbled up
     * @return a valid mock for testing boolean result behavior
     */
    protected AsyncResult<Boolean> getBooleanFailureAsyncResult(Throwable cause) {
      AsyncResult<Boolean> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(null);
      when(asyncResult.succeeded()).thenReturn(false);
      when(asyncResult.failed()).thenReturn(true);
      when(asyncResult.cause()).thenReturn(cause);

      return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<JsonObject>} that can be used as a handler controlling
     * the success outcomes
     *
     * @param result the expected {@link JsonObject} returned from {@link AsyncResult#result()}
     * @return a valid mock for testing {@link JsonObject} result behavior
     */
    protected AsyncResult<JsonObject> getMockAsyncResult(JsonObject result) {
      AsyncResult<JsonObject> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(result);
      when(asyncResult.succeeded()).thenReturn(true);
      when(asyncResult.failed()).thenReturn(false);
      when(asyncResult.cause()).thenReturn(null);

      return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<JsonArray>} that can be used as a handler controlling
     * the success outcomes
     *
     * @param result the expected {@link JsonArray} returned from {@link AsyncResult#result()}
     * @return a valid mock for testing {@link JsonArray} result behavior
     */
    protected AsyncResult<JsonArray> getMockAsyncResult(JsonArray result) {
      AsyncResult<JsonArray> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(result);
      when(asyncResult.succeeded()).thenReturn(true);
      when(asyncResult.failed()).thenReturn(false);
      when(asyncResult.cause()).thenReturn(null);

      return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<JsonObject>} that can be used as a handler controlling
     * failure outcomes and response
     *
     * @param cause the expected exception to be bubbled up
     * @return a valid mock for testing {@link JsonObject} result behavior
     */
    protected AsyncResult<JsonObject> getJsonObjectFailureAsyncResult(Throwable cause) {
      AsyncResult<JsonObject> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(null);
      when(asyncResult.succeeded()).thenReturn(false);
      when(asyncResult.failed()).thenReturn(true);
      when(asyncResult.cause()).thenReturn(cause);

      return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<JsonArray>} that can be used as a handler controlling
     *
     * 9-
     * failure outcomes and response
     *
     * @param cause the expected exception to be bubbled up
     * @return a valid mock for testing {@link JsonArray} result behavior
     */
    protected AsyncResult<JsonArray> getJsonArrayFailureAsyncResult(Throwable cause) {
      AsyncResult<JsonArray> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(null);
      when(asyncResult.succeeded()).thenReturn(false);
      when(asyncResult.failed()).thenReturn(true);
      when(asyncResult.cause()).thenReturn(cause);

      return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<TransferTask>} that can be used as a handler controlling
     * the success outcomes
     *
     * @param result the expected {@link TransferTask} returned from {@link AsyncResult#result()}
     * @return a valid mock for testing {@link TransferTask} result behavior
     */
    protected AsyncResult<TransferTask> getMockAsyncResult(TransferTask result) {
      AsyncResult<TransferTask> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(result);
      when(asyncResult.succeeded()).thenReturn(true);
      when(asyncResult.failed()).thenReturn(false);
      when(asyncResult.cause()).thenReturn(null);

      return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<TransferTask>} that can be used as a handler controlling
     * the success outcomes
     *
     * @param result the expected {@link TransferTask} returned from {@link AsyncResult#result()}
     * @return a valid mock for testing {@link TransferTask} result behavior
     */
    protected AsyncResult<String> getMockStringResult(String result) {
      AsyncResult<String> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(result);
      when(asyncResult.succeeded()).thenReturn(true);
      when(asyncResult.failed()).thenReturn(false);
      when(asyncResult.cause()).thenReturn(null);

      return asyncResult;
    }

    /**
     * Creates a mock {@link AsyncResult<TransferTask>} that can be used as a handler controlling
     * failure outcomes and response
     *
     * @param cause the expected exception to be bubbled up
     * @return a valid mock for testing {@link TransferTask} result behavior
     */
    protected AsyncResult<TransferTask> getTransferTaskFailureAsyncResult(Throwable cause) {
      AsyncResult<TransferTask> asyncResult = mock(AsyncResult.class);
      when(asyncResult.result()).thenReturn(null);
      when(asyncResult.succeeded()).thenReturn(false);
      when(asyncResult.failed()).thenReturn(true);
      when(asyncResult.cause()).thenReturn(cause);

      return asyncResult;
    }
  }
}
