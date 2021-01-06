package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers completed task listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({ JDBCClient.class })
@Disabled
class TransferTaskCompleteTaskListenerTest extends BaseTestCase {
//    private static final Logger log = LoggerFactory.getLogger(TransferCompleteTaskListenerTest.class);

    @AfterAll
    public void tearDown(Vertx vertx, VertxTestContext ctx) {
        vertx.close(ctx.completing());
    }

    protected TransferTaskCompleteTaskListener getMockTransferCompleteListenerInstance(Vertx vertx) {
        TransferTaskCompleteTaskListener listener = mock(TransferTaskCompleteTaskListener.class );
        when(listener.config()).thenReturn(config);
        when(listener.getEventChannel()).thenReturn(TRANSFER_COMPLETED);
        when(listener.getVertx()).thenReturn(vertx);
        doCallRealMethod().when(listener).processEvent(any(), any());
        when(listener.getRetryRequestManager()).thenCallRealMethod();
        doNothing().when(listener)._doPublishEvent(any(), any());
        doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
        doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
        return listener;
    }


    @Test
    @DisplayName("testProcessEventWithNoParent")
    public void testProcessEventWithNoParent(Vertx vertx, VertxTestContext ctx) {
        // Set up our transfertask for testing
        TransferTask transferTask = _createTestTransferTask();
        transferTask.setStatus(TransferStatusType.TRANSFERRING);
        transferTask.setStartTime(Instant.now());

        // get the JsonObject to pass back and forth between verticles
        JsonObject json = transferTask.toJson();

        // mock out the verticle we're testing so we can observe that its methods were called as expected
        TransferTaskCompleteTaskListener transferTaskCompleteTaskListener = getMockTransferCompleteListenerInstance(vertx);

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);


        //###############
        // mock a successful outcome with updated json transfer task result from updateStatus
        JsonObject expectedUdpatedJsonObject = transferTask.toJson()
                .put("status", TransferStatusType.COMPLETED.name())
                .put("endTime", Instant.now());

        AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
        // mock the handler passed into updateStatus
        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
            return null;
        }).when(dbService).updateStatus(any(), any(), any(), any());
        //#####################


        //#####################
//        // mock a successful outcome with false result from processParentEvent indicating the parent has active children
        AsyncResult<Boolean> processParentEventHandler = getMockAsyncResult(Boolean.TRUE);

//        // mock the handler passed into processParentEvent
        doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(processParentEventHandler);
            return Future.succeededFuture(Boolean.TRUE);
        }).when(transferTaskCompleteTaskListener).processParentEvent(any(), any(), any());
        //#######################


        // mock the dbService getter in our mocked vertical so we don't need to use powermock
        when(transferTaskCompleteTaskListener.getDbService()).thenReturn(dbService);

        // now we run the actual test using our test transfer task data
        transferTaskCompleteTaskListener.processEvent(json, result -> ctx.verify(() -> {
            // verify the db service was called to update the task status
            // dont' mix and match raw values with mockito matchers. Hence wrapping the
            // values in the eq() method.
            verify(dbService).updateStatus(eq(transferTask.getTenantId()),
                    eq(transferTask.getUuid()), eq(TransferStatusType.COMPLETED.name()), any());

            // verify that the completed event was created. this should always be throws
            // if the updateStatus result succeeds.
            verify(transferTaskCompleteTaskListener)._doPublishEvent(TRANSFERTASK_FINISHED, json);

            // make sure the parent was not processed when none existed for the transfer task
            verify(transferTaskCompleteTaskListener, never()).processParentEvent(any(), any(), any());

            // make sure no error event is ever thrown
            verify(transferTaskCompleteTaskListener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
            verify(transferTaskCompleteTaskListener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

            Assertions.assertFalse(result.result(),
                    "TransferTask response should be true indicating the task completed successfully.");

            Assertions.assertTrue(result.succeeded(), "TransferTask update should have succeeded");

            ctx.completeNow();
        }));
    }

    @Test
    @DisplayName("testProcessEventWithInactiveParent")
    public void testProcessEventWithInactiveParent(Vertx vertx, VertxTestContext ctx) {
        // Set up our transfertask for testing
        TransferTask transferTask = _createTestTransferTask();
        transferTask.setStatus(TransferStatusType.TRANSFERRING);
        transferTask.setStartTime(Instant.now());

        String parentTaskID = new AgaveUUID(UUIDType.TRANSFER).toString();
        transferTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        transferTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

        TransferTask parentTask = _createTestTransferTask();
        //parentTask.setStatus(TransferStatusType.PAUSED);
        parentTask.setUuid(parentTaskID);


        // get the JsonObject to pass back and forth between verticles
        JsonObject json = transferTask.toJson();

        // mock out the verticle we're testing so we can observe that its methods were called as expected
        TransferTaskCompleteTaskListener ttc = getMockTransferCompleteListenerInstance(vertx);

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

        // mock a successful outcome with updated json transfer task result from updateStatus
        JsonObject expectedUdpatedJsonObject = transferTask.toJson()
                .put("status", TransferStatusType.COMPLETED.name())
                .put("endTime", Instant.now());

        AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

        // mock the handler passed into updateStatus
        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
            return null;
        }).when(dbService).updateStatus(any(), any(), any(), any());




//        // get the JsonObject to pass back and forth between verticles
//        JsonObject parentJson = parentTask.toJson();
//
//        // mock a successful outcome with updated json transfer task result from updateStatus
//        JsonObject expectedParentJsonObject = parentTask.toJson()
//                .put("status", TransferStatusType.COMPLETED.name())
//                .put("endTime", Instant.now());
//
        // mock a successful outcome with false result from processParentEvent indicating the parent has active children
        AsyncResult<Boolean> processParentEventHandler = getMockAsyncResult(true);

        // mock the handler passed into processParentEvent
        doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(processParentEventHandler);
            return null;
        }).when(ttc).processParentEvent(any(), any(), any() );









        // mock the dbService getter in our mocked vertical so we don't need to use powermock
        when(ttc.getDbService()).thenReturn(dbService);

        // now we run the actual test using our test transfer task data
        ttc.processEvent(json, result -> ctx.verify(() -> {

            // verify the db service was called to update the task status
            // dont' mix and match raw values with mockito matchers. Hence wrapping the
            // values in the eq() method.
            verify(dbService).updateStatus(eq(transferTask.getTenantId()),
                    eq(transferTask.getUuid()), eq(TransferStatusType.COMPLETED.name()), any());

            // verify that the completed event was created. this should always be throws
            // if the updateStatus result succeeds.
            verify(ttc)._doPublishEvent(TRANSFERTASK_FINISHED, json);

            // make sure the parent was processed
            verify(ttc).processParentEvent(eq(transferTask.getTenantId()), eq(transferTask.getParentTaskId()), any());

            // make sure no error event is ever thrown
            verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
            verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

            Assertions.assertTrue(result.succeeded(), "TransferTask update should have succeeded");

            Assertions.assertTrue(result.result(),
                    "TransferTask response should be false indicating no activity taken on the parent.");

            ctx.completeNow();
        }));
    }

    @Test
    @DisplayName("testProcessEventWithActiveParent")
    public void testProcessEventWithActiveParent(Vertx vertx, VertxTestContext ctx) {
        // Set up our transfertask for testing
        TransferTask transferTask = _createTestTransferTask();
        transferTask.setStatus(TransferStatusType.TRANSFERRING);
        transferTask.setStartTime(Instant.now());
        transferTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        transferTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

        // get the JsonObject to pass back and forth between verticles
        JsonObject json = transferTask.toJson();

        // mock out the verticle we're testing so we can observe that its methods were called as expected
        TransferTaskCompleteTaskListener ttc = getMockTransferCompleteListenerInstance(vertx);

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

        // mock a successful outcome with updated json transfer task result from updateStatus
        JsonObject expectedUdpatedJsonObject = transferTask.toJson()
                .put("status", TransferStatusType.COMPLETED.name())
                .put("endTime", Instant.now());
        AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

        // mock the handler passed into updateStatus
        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
            return null;
        }).when(dbService).updateStatus(any(), any(), any(), any());

        // mock a successful outcome with false result from processParentEvent indicating the parent has active children
        AsyncResult<Boolean> processParentEventHandler = getMockAsyncResult(Boolean.TRUE);

        // mock the handler passed into processParentEvent
        doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(processParentEventHandler);
            return null;
        }).when(ttc).processParentEvent(any(), any(), any());

        // mock the dbService getter in our mocked vertical so we don't need to use powermock
        when(ttc.getDbService()).thenReturn(dbService);

        // now we run the actual test using our test transfer task data
        ttc.processEvent(json, result -> ctx.verify(() -> {

            // verify the db service was called to update the task status
            // dont' mix and match raw values with mockito matchers. Hence wrapping the
            // values in the eq() method.
            verify(dbService).updateStatus(eq(transferTask.getTenantId()),
                    eq(transferTask.getUuid()), eq(TransferStatusType.COMPLETED.name()), any());

            // verify that the completed event was created. this should always be throws
            // if the updateStatus result succeeds.
            verify(ttc)._doPublishEvent(eq(TRANSFERTASK_FINISHED), eq(json));

            // make sure the parent was processed
            verify(ttc).processParentEvent(eq(transferTask.getTenantId()), eq(transferTask.getParentTaskId()), any());

            // make sure no error event is ever thrown
            verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
            verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

            Assertions.assertTrue(result.result(),
                    "TransferTask response should be true indicating the task completed successfully.");

            Assertions.assertTrue(result.succeeded(), "TransferTask update should have succeeded");

            ctx.completeNow();
        }));
    }

    @Test
    @DisplayName("testProcess creates parent error event when parent processing fails")
    public void testProcessEventCreatesParentErrorEventWhenParentProcessingFails(Vertx vertx, VertxTestContext ctx) {
        // Set up our transfertask for testings
        TransferTask transferTask = _createTestTransferTask();
        transferTask.setStatus(TransferStatusType.TRANSFERRING);
        transferTask.setStartTime(Instant.now());
        transferTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
        transferTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

        // get the JsonObject to pass back and forth between verticles
        JsonObject json = transferTask.toJson();

        // mock out the verticle we're testing so we can observe that its methods were called as expected
        TransferTaskCompleteTaskListener ttc = getMockTransferCompleteListenerInstance(vertx);

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

        // mock a successful outcome with updated json transfer task result from updateStatus
        JsonObject expectedUdpatedJsonObject = transferTask.toJson()
                .put("status", TransferStatusType.COMPLETED.name())
                .put("endTime", Instant.now());
        AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

        // mock the handler passed into updateStatus
        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
            return null;
        }).when(dbService).updateStatus(any(), any(), any(), any());

        // mock a successful outcome with false result from processParentEvent indicating the parent has active children
        AsyncResult<Boolean> processParentEventHandler = getBooleanFailureAsyncResult(
                new Exception("Testing Unknown parent Id"));

        // mock the handler passed into processParentEvent
        doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(processParentEventHandler);
            return null;
        }).when(ttc).processParentEvent(any(), any(), any());

        // mock the dbService getter in our mocked vertical so we don't need to use powermock
        when(ttc.getDbService()).thenReturn(dbService);

        // now we run the actual test using our test transfer task data
        ttc.processEvent(json, result -> ctx.verify(() -> {

            // verify the db service was called to update the task status
            // dont' mix and match raw values with mockito matchers. Hence wrapping the
            // values in the eq() method.
            verify(dbService).updateStatus(eq(transferTask.getTenantId()),
                    eq(transferTask.getUuid()), eq(TransferStatusType.COMPLETED.name()), any());

            // verify that the completed event was created. this should always be throws
            // if the updateStatus result succeeds.
            verify(ttc)._doPublishEvent(eq(TRANSFERTASK_FINISHED), eq(json));

            // make sure the parent was processed
            verify(ttc).processParentEvent(eq(transferTask.getTenantId()), eq(transferTask.getParentTaskId()), any());

            verify(ttc)._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

            // make sure no error event is ever thrown
            verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

            Assertions.assertTrue(result.succeeded(), "TransferTask update should have succeeded");

            Assertions.assertFalse(result.result(),
                    "TransferTask response should be false when the parent processing fails.");


            ctx.completeNow();
        }));
    }

    ////////////////////////////////////////
    // Parent processing tests
    ////////////////////////////////////////

    @Test
    @DisplayName("Test the ProcessParentEvent with a Status Completed that Reports False")
    public void testProcessParentEventStatusCompletedReportsFalse(Vertx vertx, VertxTestContext ctx) {
            // Set up our transfertask for testing
            TransferTask parentTask = _createTestTransferTask();
            parentTask.setStatus(TransferStatusType.COMPLETED);
            parentTask.setStartTime(Instant.now());
            parentTask.setEndTime(Instant.now());
            parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") -1));

            TransferTask transferTask = _createTestTransferTask();
            transferTask.setStatus(TransferStatusType.TRANSFERRING);
            transferTask.setStartTime(Instant.now());
            transferTask.setEndTime(Instant.now());
            transferTask.setRootTaskId(parentTask.getUuid());
            transferTask.setParentTaskId(parentTask.getUuid());


            // mock out the verticle we're testing so we can observe that its methods were called as expected
            TransferTaskCompleteTaskListener ttc = getMockTransferCompleteListenerInstance(vertx);
            // we switch the call chain when we want to pass through a method invocation to a method returning void
            doCallRealMethod().when(ttc).processParentEvent(any(), any(), any());

            // mock out the db service so we can can isolate method logic rather than db
            TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

            // mock a successful outcome with updated json transfer task result from getById call to db
            AsyncResult<JsonObject> getByAnyHandler = getMockAsyncResult(parentTask.toJson());

            // mock the handler passed into getById
            doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
                @SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByAnyHandler);
                return null;
            }).when(dbService).getById(any(), any(), any());

            // mock a successful response from allChildrenCancelledOrCompleted call to db
            AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);

            // mock the handler passed into allChildrenCancelledOrCompletedHandler
            doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
                @SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
                return null;
            }).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

            // mock the dbService getter in our mocked vertical so we don't need to use powermock
            when(ttc.getDbService()).thenReturn(dbService);

            Checkpoint processParentEventStartCheckpoint = ctx.checkpoint();
            Checkpoint processParentEventEndCheckpoint = ctx.checkpoint();

        // now we run the actual test using our test transfer task data

        ttc.processParentEvent(transferTask.getTenantId(), transferTask.getParentTaskId(), result -> {
            processParentEventStartCheckpoint.flag();
            ctx.verify(() -> {
                // verify the db service was called to update the task status
                // dont' mix and match raw values with mockito matchers. Hence wrapping the
                // values in the eq() method.
                verify(dbService).getById(eq(transferTask.getTenantId()),
                        eq(transferTask.getParentTaskId()), any());

                verify(dbService, never()).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()),
                        eq(transferTask.getParentTaskId()), any());

                verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

                // make sure no error event is ever thrown
                verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

                // new transfer complete event should be created for active parent with no active children
                verify(ttc, never())._doPublishEvent(eq(TRANSFER_COMPLETED), any());

                Assertions.assertTrue(result.succeeded(), "TransferTask processParentEvent should have succeeded when parent has completed");

                Assertions.assertFalse(result.result(),
                        "TransferTask processParentEvent callback result should be false when the parent has completed.");

                processParentEventEndCheckpoint.flag();

                ctx.completeNow();
            });
        });

    }

    @Test
    @DisplayName("Test the ProcessParentEvent with a status of Cancelled that reports false")
    public void testProcessParentEventStatusCancelledReportsFalse(Vertx vertx, VertxTestContext ctx) {
        // Set up our transfertask for testing
        TransferTask parentTask = _createTestTransferTask();
        parentTask.setStatus(TransferStatusType.CANCELLED);
        parentTask.setStartTime(Instant.now());
        parentTask.setEndTime(Instant.now());
        parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") -1));

        TransferTask transferTask = _createTestTransferTask();
        transferTask.setStatus(TransferStatusType.TRANSFERRING);
        transferTask.setStartTime(Instant.now());
        transferTask.setEndTime(Instant.now());
        transferTask.setRootTaskId(parentTask.getUuid());
        transferTask.setParentTaskId(parentTask.getUuid());

        // mock out the verticle we're testing so we can observe that its methods were called as expected
        TransferTaskCompleteTaskListener ttc = getMockTransferCompleteListenerInstance(vertx);
        // we switch the call chain when we want to pass through a method invocation to a method returning void
        doCallRealMethod().when(ttc).processParentEvent(any(), any(), any());

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> getByAnyHandler = getMockAsyncResult(parentTask.toJson());

        // mock the handler passed into getById
        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByAnyHandler);
            return null;
        }).when(dbService).getById(any(), any(), any());

        // mock a successful response from allChildrenCancelledOrCompleted call to db
        AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);

        // mock the handler passed into allChildrenCancelledOrCompletedHandler
        doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
            return null;
        }).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

        // mock the dbService getter in our mocked vertical so we don't need to use powermock
        when(ttc.getDbService()).thenReturn(dbService);

        Checkpoint processParentEventStartCheckpoint = ctx.checkpoint();
        Checkpoint processParentEventEndCheckpoint = ctx.checkpoint();

        // now we run the actual test using our test transfer task data

        ttc.processParentEvent(transferTask.getTenantId(), transferTask.getParentTaskId(), result -> {
            processParentEventStartCheckpoint.flag();
            ctx.verify(() -> {
                // verify the db service was called to update the task status
                // dont' mix and match raw values with mockito matchers. Hence wrapping the
                // values in the eq() method.
                verify(dbService).getById(eq(transferTask.getTenantId()),
                        eq(transferTask.getParentTaskId()), any());

                verify(dbService, never()).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()),
                        eq(transferTask.getParentTaskId()), any());

                verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

                // make sure no error event is ever thrown
                verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

                // new transfer complete event should be created for active parent with no active children
                verify(ttc, never())._doPublishEvent(eq(TRANSFER_COMPLETED), any());

                Assertions.assertTrue(result.succeeded(), "TransferTask processParentEvent should have succeeded when parent has been cancelled");

                Assertions.assertFalse(result.result(),
                        "TransferTask processParentEvent callback result should be false when the parent has been cancelled.");

                processParentEventEndCheckpoint.flag();

                ctx.completeNow();
            });
        });

    }

    @Test
    @DisplayName("Test ProcessParentEvent with a status of Failed but reports a False")
    public void testProcessParentEventStatusFailedReportsFalse(Vertx vertx, VertxTestContext ctx) {
        // Set up our transfertask for testing
        TransferTask parentTask = _createTestTransferTask();
        parentTask.setStatus(TransferStatusType.FAILED);
        parentTask.setStartTime(Instant.now());
        parentTask.setEndTime(Instant.now());
        parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") -1));

        TransferTask transferTask = _createTestTransferTask();
        transferTask.setStatus(TransferStatusType.TRANSFERRING);
        transferTask.setStartTime(Instant.now());
        transferTask.setEndTime(Instant.now());
        transferTask.setRootTaskId(parentTask.getUuid());
        transferTask.setParentTaskId(parentTask.getUuid());

        // mock out the verticle we're testing so we can observe that its methods were called as expected
        TransferTaskCompleteTaskListener ttc = getMockTransferCompleteListenerInstance(vertx);
        // we switch the call chain when we want to pass through a method invocation to a method returning void
        doCallRealMethod().when(ttc).processParentEvent(any(), any(), any());

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> getByAnyHandler = getMockAsyncResult(parentTask.toJson());

        // mock the handler passed into getById
        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByAnyHandler);
            return null;
        }).when(dbService).getById(any(), any(), any());

        // mock a successful response from allChildrenCancelledOrCompleted call to db
        AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);

        // mock the handler passed into allChildrenCancelledOrCompletedHandler
        doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
            return null;
        }).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

        // mock the dbService getter in our mocked vertical so we don't need to use powermock
        when(ttc.getDbService()).thenReturn(dbService);

        Checkpoint processParentEventStartCheckpoint = ctx.checkpoint();
        Checkpoint processParentEventEndCheckpoint = ctx.checkpoint();

        // now we run the actual test using our test transfer task data

        ttc.processParentEvent(transferTask.getTenantId(), transferTask.getParentTaskId(), result -> {
            processParentEventStartCheckpoint.flag();
            ctx.verify(() -> {
                // verify the db service was called to update the task status
                // dont' mix and match raw values with mockito matchers. Hence wrapping the
                // values in the eq() method.
                verify(dbService).getById(eq(transferTask.getTenantId()),
                        eq(transferTask.getParentTaskId()), any());

                verify(dbService, never()).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()),
                        eq(transferTask.getParentTaskId()), any());

                verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

                // make sure no error event is ever thrown
                verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

                // new transfer complete event should be created for active parent with no active children
                verify(ttc, never())._doPublishEvent(eq(TRANSFER_COMPLETED), any());

                Assertions.assertTrue(result.succeeded(), "TransferTask processParentEvent should have succeeded when parent has failed");

                Assertions.assertFalse(result.result(),
                        "TransferTask processParentEvent callback result should be false when the parent has failed.");

                processParentEventEndCheckpoint.flag();

                ctx.completeNow();
            });
        });

    }

    @Test
    @DisplayName("testProcessParentEventFoundActive")
    public void testProcessParentEventFoundActive(Vertx vertx, VertxTestContext ctx) {
        // Set up our transfertask for testing
        TransferTask parentTask = _createTestTransferTask();
        parentTask.setStatus(TransferStatusType.TRANSFERRING);
        parentTask.setStartTime(Instant.now());
        parentTask.setEndTime(Instant.now());
        parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") -1));

        TransferTask transferTask = _createTestTransferTask();
        transferTask.setStatus(TransferStatusType.TRANSFERRING);
        transferTask.setStartTime(Instant.now());
        transferTask.setEndTime(Instant.now());
        transferTask.setRootTaskId(parentTask.getUuid());
        transferTask.setParentTaskId(parentTask.getUuid());


        // mock out the verticle we're testing so we can observe that its methods were called as expected
        TransferTaskCompleteTaskListener ttc = getMockTransferCompleteListenerInstance(vertx);
        // we switch the call chain when we want to pass through a method invocation to a method returning void
        doCallRealMethod().when(ttc).processParentEvent(any(), any(), any());

        // mock out the db service so we can can isolate method logic rather than db
        TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

        // mock a successful outcome with updated json transfer task result from getById call to db
        AsyncResult<JsonObject> getByAnyHandler = getMockAsyncResult(parentTask.toJson());

        // mock the handler passed into getById
        doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByAnyHandler);
            return null;
        }).when(dbService).getById(any(), any(), any());

        // mock a successful response from allChildrenCancelledOrCompleted call to db
        AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);

        // mock the handler passed into allChildrenCancelledOrCompletedHandler
        doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
            @SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
            return null;
        }).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

        // mock the dbService getter in our mocked vertical so we don't need to use powermock
        when(ttc.getDbService()).thenReturn(dbService);

        Checkpoint processParentEventStartCheckpoint = ctx.checkpoint();
        Checkpoint processParentEventEndCheckpoint = ctx.checkpoint();

        // now we run the actual test using our test transfer task data

        ttc.processParentEvent(transferTask.getTenantId(), transferTask.getParentTaskId(), result -> {
            processParentEventStartCheckpoint.flag();
            ctx.verify(() -> {
                // verify the db service was called to update the task status
                // dont' mix and match raw values with mockito matchers. Hence wrapping the
                // values in the eq() method.
                verify(dbService).getById(eq(transferTask.getTenantId()),
                        eq(transferTask.getParentTaskId()), any());

                verify(dbService).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()),
                        eq(transferTask.getParentTaskId()), any());

                verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

                // make sure no error event is ever thrown
                verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

                // new transfer complete event should be created for active parent with no active children
                verify(ttc)._doPublishEvent(eq(TRANSFER_COMPLETED), any());

                Assertions.assertTrue(result.succeeded(), "TransferTask processParentEvent should have succeeded");

                Assertions.assertTrue(result.result(),
                        "TransferTask processParentEvent callback result should be true when the when the parent has active status.");

                processParentEventEndCheckpoint.flag();

                ctx.completeNow();
            });
        });

    }
}

