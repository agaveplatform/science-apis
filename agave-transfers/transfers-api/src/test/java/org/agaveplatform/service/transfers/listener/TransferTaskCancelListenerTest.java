package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.exception.ObjectNotFoundException;
import org.agaveplatform.service.transfers.exception.TransferException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Task Canceled test")
class TransferTaskCancelListenerTest extends BaseTestCase {

	private Object Handler;

	TransferTaskCancelListener getMockListenerInstance(Vertx vertx) {
		TransferTaskCancelListener listener = Mockito.mock(TransferTaskCancelListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_CANCELLED);
		when(listener.getVertx()).thenReturn(vertx);
		doNothing().when(listener)._doPublishEvent(any(), any());
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
		return listener;
	}

	TransferTaskCancelListener getMockCancelAckListenerInstance(Vertx vertx) {
		TransferTaskCancelListener listener = Mockito.mock(TransferTaskCancelListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_CANCELED_ACK);
		when(listener.getVertx()).thenReturn(vertx);
		doNothing().when(listener)._doPublishEvent(any(), any());
		return listener;
	}

	@Test
	@DisplayName("TransferTaskCancelListenerTest - process cancel request fails when unable to lookup transfer task by id")
	public void processCancelRequestFailDBTest(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a failed db lookup
		AsyncResult<JsonObject> failedUpdateGetById = getJsonObjectFailureAsyncResult(new ObjectNotFoundException("Test record shoud not be found."));
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(failedUpdateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);

		// mock the handler passed into processCancelRequest
		doCallRealMethod().when(listener).processCancelRequest(any(), any());

		// now we run the actual test using our test transfer task data
		listener.processCancelRequest(transferTask.toJson(), results -> {

			ctx.verify(() -> {
				assertTrue(results.failed(), "The call should faile.");
				assertEquals(ObjectNotFoundException.class, results.cause().getClass(),
						"DB exception should roll up to the calling method.");

				// verify the db service was called to fetch the current task
				verify(dbService).getById(eq(transferTask.getTenantId()),
						eq(transferTask.getUuid()), any());

				// update should never happen
				verify(dbService, never()).updateStatus(any(), any(), any(), any());

				// error event shoudl be thrown
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

				// sync should not happen
				verify(listener, never())._doPublishEvent(TRANSFERTASK_CANCELED_SYNC, transferTask.toJson());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TransferTaskCancelListenerTest - process cancel request fails for non root tasks")
	public void processCancelRequestFailsForNonRootTasks(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		transferTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		JsonObject errorEventBody = new JsonObject()
				.put("cause", TransferException.class.getName())
				.put("message", "Cannot cancel non-root transfer tasks.")
				.mergeIn(transferTask.toJson());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(updateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), anyObject());

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.FAILED.name())
//				.put("endTime", Instant.now())
				.put("lastUpdated", Instant.now());

		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(expectedUpdateStatusHandler);
			return null;
		}).when(dbService).updateStatus( eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any(), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);

		// mock the handler passed into processCancelRequest
		doCallRealMethod().when(listener).processCancelRequest(anyObject(), any() );

		// now we run the actual test using our test transfer task data
		listener.processCancelRequest(transferTask.toJson(), results -> {

			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");

				assertFalse(results.result(), "The result of the handler should be false indicating the task was not processed.");

				// verify the db service was called to fetch the current task
				verify(dbService).getById(eq(transferTask.getTenantId()),
						eq(transferTask.getUuid()), any());

				// update should never happen
				verify(dbService, never()).updateStatus(any(), any(), any(), any());

				// error event should be thrown for non-transfer task request
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_ERROR), eq(errorEventBody));

				// no sync event should be sent
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_CANCELED_SYNC), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TransferTaskCancelListenerTest - process cancel request for assigned root task sends syncs and updates status")
	public void processCancelRequestUpdatesAndSendsSyncForActiveRootTask(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.ASSIGNED);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(updateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any(Handler.class));

		JsonObject expectedUpdate = transferTask.toJson().put("status", TransferStatusType.CANCELING_WAITING);
		AsyncResult<JsonObject> expectedUpdateResult = getMockAsyncResult(expectedUpdate);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(expectedUpdateResult);
			return null;
		}).when(dbService).updateStatus(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(TransferStatusType.CANCELING_WAITING.name()), any(Handler.class));

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);
		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
		//AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.TRUE);

		// mock the handler passed into processCancelRequest
		doCallRealMethod().when(listener).processCancelRequest(any(), any());


		// now we run the actual test using our test transfer task data
		listener.processCancelRequest(transferTask.toJson(), results -> {

			ctx.verify(() -> {

				assertTrue(results.succeeded(), "The call should succeed.");

				assertTrue(results.result(), "The processing should return a true result indicating the task was updated and the processing succeeded.");

				// verify the db service was called to fetch the current task
				verify(dbService).getById(eq(transferTask.getTenantId()),
						eq(transferTask.getUuid()), any());

				// verify the db service was called to update the task status
				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
						eq(transferTask.getUuid()), eq(TransferStatusType.CANCELING_WAITING.name()), any());

				// verify that the completed event was created. this should always be thrown
				// if the updateStatus result succeeds.
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_CANCELED_SYNC), eq(expectedUpdate));

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TransferTaskCancelListenerTest - process cancel request for cancelled root task sends syncs and skips update")
	public void processCancelRequestSkipsUpdateAndSendsSyncForInactiveRootTask(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.CANCELLED);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
//		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(updateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), anyObject());

		JsonObject expectedUpdate = transferTask.toJson().put("status", TransferStatusType.CANCELING_WAITING);
		AsyncResult<JsonObject> expectedUpdateResult = getMockAsyncResult(expectedUpdate);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(expectedUpdateResult);
			return null;
		}).when(dbService).updateStatus(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(TransferStatusType.CANCELING_WAITING.name()), any(Handler.class));

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);
		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
		//AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.TRUE);

		// mock the handler passed into processCancelRequest
		doCallRealMethod().when(listener).processCancelRequest(any(), any());


		// now we run the actual test using our test transfer task data
		listener.processCancelRequest(transferTask.toJson(), results -> {

			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");

				assertFalse(results.result(), "The processing should return a false result indicating the task was not updated, but the processing succeeded.");

				// verify the db service was called to fetch the current task
				verify(dbService).getById(eq(transferTask.getTenantId()),
						eq(transferTask.getUuid()), any());

				// verify the db service was called to update the task status
				verify(dbService, never()).updateStatus(eq(transferTask.getTenantId()),
						eq(transferTask.getUuid()), any(), any());

				// verify that the completed event was created. this should always be thrown
				// if the updateStatus result succeeds.
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_CANCELED_SYNC), eq(transferTask.toJson()));

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, never()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TransferTaskCancelListenerTest - process cancel ack without parent should succeed")
	public void processCancelAckTest(Vertx vertx, VertxTestContext ctx){
		// set up the parent task
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());

	// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setParentTaskId(parentTask.getUuid());
		transferTask.setRootTaskId(parentTask.getUuid());
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());
		transferTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTaskCancelListener ttc = getMockCancelAckListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);
		when(ttc.getDbService()).thenReturn(dbService);

		// allChildrenCancelledOrCompleted mock
		AsyncResult<Boolean> updateAllChildHandler = getMockAsyncResult(Boolean.TRUE);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(updateAllChildHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		JsonObject expectedUpdate = transferTask.toJson().put("status", TransferStatusType.CANCELING_WAITING);
		AsyncResult<JsonObject> expectedUpdateResult = getMockAsyncResult(expectedUpdate);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(expectedUpdateResult);
			return null;
		}).when(dbService).updateStatus(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(TransferStatusType.CANCELING_WAITING.name()), any(Handler.class));

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledIfNotCompletedNotHandler = getMockAsyncResult(Boolean.TRUE);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(setTransferTaskCanceledIfNotCompletedNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledWhereNotCompleted(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any());


		doCallRealMethod().when(ttc).processCancelAck( eq(transferTask.toJson()),any());

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateGetByIdAck = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(updateGetByIdAck);
			return null;
		}).when(dbService).getById(any(), any(), any());

		// mock the processParenetEvent process
		AsyncResult<Boolean> setTransferTaskCanceledProcessParentAckHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(setTransferTaskCanceledProcessParentAckHandler);
			return null;
		}).when(ttc).processParentAck( eq(transferTask.getTenantId()), eq(transferTask.getParentTaskId()), any());

		// mock getTransferTask
		AsyncResult<TransferTask> transferTaskHandler = getMockAsyncResult(parentTask);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<TransferTask>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(transferTaskHandler);
			return null;
		}).when(ttc).getTransferTask(any(), any(), any());


		ttc.processCancelAck(transferTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				assertTrue(results.result(), "The async should return true indicating the task was updated");

				verify(ttc)._doPublishEvent(TRANSFERTASK_CANCELED_COMPLETED, transferTask.toJson());
				verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_CANCELED_ACK), any());
				//assertEquals(transferTask.getUuid(), results, "Transfer task was not acknowledged in response uuid");
				//assertEquals(uuid, result, "result should have been uuid: " + uuid);

				ctx.completeNow();
			});
		});
	}

	//#########################################################################################################
	//# Nothing below here is implemented. Essentially all stub code.
	//#########################################################################################################
	@Test
	@DisplayName("TransferTaskCancelListenerTest - process cancel ack with parent should succeed and check parent")
	@Disabled
	public void processCancelAckWithParentTest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
		parentTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTaskCancelListener listener = getMockCancelAckListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from processCancelAck
		JsonObject expectedProcessCancelAck = parentTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateProcessCancelAck = getMockAsyncResult(expectedProcessCancelAck);
		doCallRealMethod().when(listener).processCancelAck( eq(parentTask.toJson()),any());

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = parentTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledIfNotCompletedNotHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(setTransferTaskCanceledIfNotCompletedNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledWhereNotCompleted(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted( any(), any(), any());

		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// now we run the actual test using our test transfer task data
		listener.processCancelAck(parentTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				//assertTrue(results.result(), "The async should return true");

				// verify the db service was called to update the task status
//				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
//						eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				//verify(listener)._doPublishEvent(TRANSFERTASK_CANCELLED, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, atLeastOnce()).processCancelAck(any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TransferTaskCancelListenerTest - process cancel ack with parent who has all other children failed should create cancel event on parent")
	@Disabled
	public void processCancelAckWithParentAllChildrenTest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
		parentTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTaskCancelListener listener = getMockCancelAckListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from processCancelAck
		JsonObject expectedProcessCancelAck = parentTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateProcessCancelAck = getMockAsyncResult(expectedProcessCancelAck);
		doCallRealMethod().when(listener).processCancelAck( eq(parentTask.toJson()),any());

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = parentTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledIfNotCompletedNotHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(setTransferTaskCanceledIfNotCompletedNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledWhereNotCompleted(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

//		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
//		AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.FALSE);
//		// mock the handler passed into processParentEvent
//		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
//			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(1, Handler.class))
//					.handle(processCancelRequest);
//			return null;
//		}).when(listener).processCancelAck( anyObject(), any());

		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// now we run the actual test using our test transfer task data
		listener.processCancelAck(parentTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				//assertTrue(results.result(), "The async should return true");

				// verify the db service was called to update the task status
//				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
//						eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				//verify(listener)._doPublishEvent(TRANSFERTASK_CANCELLED, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, atLeastOnce()).processCancelAck(any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("ransferTaskCancelListenerTest - process cancel ack with not empty parent id should check parent")
	@Disabled
	public void processCancelAckWithParentNotEmptyTest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
		parentTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTaskCancelListener listener = getMockCancelAckListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from processCancelAck
		JsonObject expectedProcessCancelAck = parentTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateProcessCancelAck = getMockAsyncResult(expectedProcessCancelAck);
		doCallRealMethod().when(listener).processCancelAck( eq(parentTask.toJson()),any());

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = parentTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());

		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
		AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.FALSE);
		// mock the handler passed into processParentEvent
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(processCancelRequest);
			return null;
		}).when(listener).processCancelAck( anyObject(), any());

		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// now we run the actual test using our test transfer task data
		listener.processCancelAck(parentTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				//assertTrue(results.result(), "The async should return true");

				// verify the db service was called to update the task status
//				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
//						eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				//verify(listener)._doPublishEvent(TRANSFERTASK_CANCELLED, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, atLeastOnce()).processCancelAck(any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC Ack Lstn with Parent - pCA w/Parent is not empty Failed")
	@Disabled
	public void processCancelAckWithParentIsNotEmptyTest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
		parentTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockCancelAckListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from processCancelAck
		JsonObject expectedProcessCancelAck = transferTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateProcessCancelAck = getMockAsyncResult(expectedProcessCancelAck);
		doCallRealMethod().when(listener).processCancelAck( eq(parentTask.toJson()),any());

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());



		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// now we run the actual test using our test transfer task data
		listener.processCancelAck(parentTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				//assertTrue(results.result(), "The async should return true");

				// verify the db service was called to update the task status
//				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
//						eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				//verify(listener)._doPublishEvent(TRANSFERTASK_CANCELLED, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, atLeastOnce()).processCancelAck(any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC parentParentAck - pPA w/Parent")
	@Disabled
	public void processParentAckWithParentTest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
		parentTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTaskCancelListener listener = getMockCancelAckListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from processCancelAck
		JsonObject expectedProcessCancelAck = parentTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateProcessCancelAck = getMockAsyncResult(expectedProcessCancelAck);
		doCallRealMethod().when(listener).processCancelAck( eq(parentTask.toJson()),any());

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = parentTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledIfNotCompletedNotHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(setTransferTaskCanceledIfNotCompletedNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledWhereNotCompleted(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted( any(), any(), any());

		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// now we run the actual test using our test transfer task data
		listener.processCancelAck(parentTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				//assertTrue(results.result(), "The async should return true");

				// verify the db service was called to update the task status
//				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
//						eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				//verify(listener)._doPublishEvent(TRANSFERTASK_CANCELLED, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, atLeastOnce()).processCancelAck(any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC processParentEvent - pPE w/Parent")
	@Disabled
	public void processParentEventTest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
//		parentTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
//		parentTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTaskCancelListener listener = getMockCancelAckListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

//		// mock a successful outcome with updated json transfer task result from processCancelAck
//		JsonObject expectedProcessParentEvent = parentTask.toJson();
////				.put("status", TransferStatusType.CANCELLED.name())
////				.put("endTime", Instant.now());
//		AsyncResult<JsonObject> updateProcessCancelAck = getMockAsyncResult(expectedProcessParentEvent);

		doCallRealMethod().when(listener).processParentEvent( eq(parentTask.getOwner()), eq(parentTask.getUuid()) ,any());


		// mock a successful outcome with updated json transfer task result from updateStatus
//		JsonObject expectedUdpatedJsonObject = parentTask.toJson()
//				.put("status", TransferStatusType.CANCELLED.name())
//				.put("endTime", Instant.now());
//		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
//		// mock the handler passed into updateStatus
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
//					.handle(updateStatusHandler);
//			return null;
//		}).when(dbService).updateStatus(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledGetByIdHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(setTransferTaskCanceledGetByIdHandler);
			return null;
		}).when(dbService).getById(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted( any(), any(), any());

		//doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// now we run the actual test using our test transfer task data
		listener.processParentEvent(parentTask.getTenantId(), parentTask.getParentTaskId(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				//assertTrue(results.result(), "The async should return true");

				// verify the db service was called to update the task status
//				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
//						eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				//verify(listener)._doPublishEvent(TRANSFERTASK_CANCELLED, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, atLeastOnce()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				ctx.completeNow();
			});
		});
	}
}
