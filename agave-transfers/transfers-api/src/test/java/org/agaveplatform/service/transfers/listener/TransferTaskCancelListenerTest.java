package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
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
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
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
	@DisplayName("TTC Lstn - pCR fail DB")
	@Disabled
	public void processCancelRequestFailDBTest(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(anyObject()) );

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
				// verify the db service was called to update the task status
				//verify(dbService).updateStatus(eq(transferTask.getTenantId()),
				//		eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be thrown
				// if the updateStatus result succeeds.
				verify(listener)._doPublishEvent(TRANSFERTASK_CANCELED_SYNC, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, never()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				//			Assertions.assertTrue(res.result(),
				//					"TransferTask response should be true indicating the task completed successfully.");

				//			Assertions.assertTrue(res.succeeded(), "TransferTask update should have succeeded");

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC Lstn - pCR w/no Parent/Root")
	//@Disabled
	public void processCancelRequestTest(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		//transferTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		//transferTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateGetById);
			return null;
		}).when(dbService).getById(any(), any(), any());

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
				// verify the db service was called to update the task status
				//verify(dbService).updateStatus(eq(transferTask.getTenantId()),
				//		eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be thrown
				// if the updateStatus result succeeds.
				verify(listener, times(1))._doPublishEvent(TRANSFERTASK_CANCELED_SYNC, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, never()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				//			Assertions.assertTrue(res.result(),
				//					"TransferTask response should be true indicating the task completed successfully.");

				//			Assertions.assertTrue(res.succeeded(), "TransferTask update should have succeeded");

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC Lstn - pCR w/ Parent/Root Txfr")
	@Disabled
	public void processCancelRequestParentTest(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		transferTask.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTask doneTransferTask = _createTestTransferTask();
		JsonObject body = new JsonObject()
				.put("cause","Cannot cancel non-root transfer tasks.")
				.put("message", "Cannot cancel non-root transfer tasks.")
				.mergeIn(transferTask.toJson());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getParentTaskId()), anyObject());

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);
		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
		//AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.TRUE);

		// mock the handler passed into processCancelRequest
		doCallRealMethod().when(listener).processCancelRequest(anyObject(), any() );


		// now we run the actual test using our test transfer task data
		listener.processCancelRequest(transferTask.toJson(), results -> {

			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				// verify the db service was called to update the task status
				//verify(dbService).updateStatus(eq(transferTask.getTenantId()),
				//		eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be thrown
				// if the updateStatus result succeeds.
				verify(listener,never())._doPublishEvent(TRANSFERTASK_CANCELED_SYNC, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, never()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_ERROR), body);
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				//			Assertions.assertTrue(res.result(),
				//					"TransferTask response should be true indicating the task completed successfully.");

				//			Assertions.assertTrue(res.succeeded(), "TransferTask update should have succeeded");

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC Lstn - pCR w/no Parent/Root Active")
	//@Disabled
	public void processCancelRequestActiveTest(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.CANCELLED);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), anyObject());

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
				// verify the db service was called to update the task status
				//verify(dbService).updateStatus(eq(transferTask.getTenantId()),
				//		eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be thrown
				// if the updateStatus result succeeds.
				verify(listener)._doPublishEvent(TRANSFERTASK_CANCELED_SYNC, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, never()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				//			Assertions.assertTrue(res.result(),
				//					"TransferTask response should be true indicating the task completed successfully.");

				//			Assertions.assertTrue(res.succeeded(), "TransferTask update should have succeeded");

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC Lstn - pCR w/no Parent/Root Cancelled")
	//@Disabled
	public void processCancelRequestActiveTransferingTest(Vertx vertx, VertxTestContext ctx) {
		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.CANCELLED);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
		doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), anyObject());

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
				// verify the db service was called to update the task status
				//verify(dbService).updateStatus(eq(transferTask.getTenantId()),
				//		eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be thrown
				// if the updateStatus result succeeds.
				verify(listener)._doPublishEvent(TRANSFERTASK_CANCELED_SYNC, transferTask.toJson());

				// make sure the parent was not processed when none existed for the transfer task
				verify(listener, never()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				//			Assertions.assertTrue(res.result(),
				//					"TransferTask response should be true indicating the task completed successfully.");

				//			Assertions.assertTrue(res.succeeded(), "TransferTask update should have succeeded");

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC Lstn - pCR w/no Parent/Root Transferring")
	@Disabled
	public void processCancelRequestActiveParentTest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
		//doCallRealMethod().when(listener).getTransferTask(anyString(), anyString(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson();
		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateGetById);
			return null;
		}).when(dbService).getById(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), anyObject());

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);
		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
		//AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.TRUE);

		// mock the handler passed into processCancelRequest
		doCallRealMethod().when(listener).processCancelRequest(anyObject(), any());

		// now we run the actual test using our test transfer task data
		listener.processCancelRequest(transferTask.toJson(), results -> {

			ctx.verify(() -> {
				assertFalse(results.failed(), "The call should fail.");
				// verify the db service was called to update the task status
				//verify(dbService).updateStatus(eq(transferTask.getTenantId()),
				//		eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be thrown
				// if the updateStatus result succeeds.
				verify(listener, never())._doPublishEvent(TRANSFERTASK_CANCELED_SYNC, any());

				// make sure the parent was not processed when none existed for the transfer task
				//verify(listener, never()).processParentEvent(any(), any(), any());

				// make sure no error event is ever thrown
				verify(listener)._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
				verify(listener, never())._doPublishEvent(eq(TRANSFERTASK_PARENT_ERROR), any());

				//			Assertions.assertTrue(res.result(),
				//					"TransferTask response should be true indicating the task completed successfully.");

				//			Assertions.assertTrue(res.succeeded(), "TransferTask update should have succeeded");

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC Lstn - pCA - TRANSFERTASK_CANCELED_COMPLETED")
	//@Disabled
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
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateAllChildHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledIfNotCompletedNotHandler = getMockAsyncResult(Boolean.TRUE);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(setTransferTaskCanceledIfNotCompletedNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledWhereNotCompleted(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any());


		doCallRealMethod().when(ttc).processCancelAck( eq(transferTask.toJson()),any());

		// mock a successful outcome with updated json transfer task result from getById
		JsonObject expectedgetByIdAck = transferTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateGetByIdAck = getMockAsyncResult(expectedgetByIdAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateGetByIdAck);
			return null;
		}).when(dbService).getById(any(), any(), any());

		// getTransferTask mock
//		AsyncResult<JsonObject> getTransferTaskMock = getMockAsyncResult(transferTask.toJson());

		// mock the processParenetEvent process
		AsyncResult<Boolean> setTransferTaskCanceledProcessParentAckHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(setTransferTaskCanceledProcessParentAckHandler);
			return null;
		}).when(ttc).processParentAck( eq(transferTask.getTenantId()), eq(transferTask.getParentTaskId()), any());

		// mock getTransferTask
		AsyncResult<TransferTask> transferTaskHandler = getMockAsyncResult(parentTask);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<TransferTask>>) arguments.getArgumentAt(2, Handler.class))
					.handle(transferTaskHandler);
			return null;
		}).when(ttc).getTransferTask(any(), any(), any());

//		doCallRealMethod().when(ttc).getTransferTask(anyString(), anyString(), any());

		ttc.processCancelAck(transferTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				assertTrue(results.result(), "The async should return true");

				verify(ttc)._doPublishEvent(TRANSFERTASK_CANCELED_COMPLETED, transferTask.toJson());
				verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_CANCELED_ACK), any());
				//assertEquals(transferTask.getUuid(), results, "Transfer task was not acknowledged in response uuid");
				//assertEquals(uuid, result, "result should have been uuid: " + uuid);

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("TTC Ack Lstn with Parent - pCA w/Parent")
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
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
					.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledIfNotCompletedNotHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(setTransferTaskCanceledIfNotCompletedNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledWhereNotCompleted(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(allChildrenCancelledOrCompletedHandler);
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
	@DisplayName("TTC Ack Lstn with Parent - pCA w/Parent AllChildren Failed")
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
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
					.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledIfNotCompletedNotHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(setTransferTaskCanceledIfNotCompletedNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledWhereNotCompleted(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(allChildrenCancelledOrCompletedHandler);
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
	@DisplayName("TTC Ack Lstn with Parent - pCA w/Parent is not empty")
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
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
					.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());

		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
		AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.FALSE);
		// mock the handler passed into processParentEvent
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(1, Handler.class))
					.handle(processCancelRequest);
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
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
					.handle(updateStatusHandler);
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
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
					.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> setTransferTaskCanceledIfNotCompletedNotHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(setTransferTaskCanceledIfNotCompletedNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledWhereNotCompleted(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(allChildrenCancelledOrCompletedHandler);
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
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(setTransferTaskCanceledGetByIdHandler);
			return null;
		}).when(dbService).getById(eq(parentTask.getTenantId()), eq(parentTask.getUuid()), any());

		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(allChildrenCancelledOrCompletedHandler);
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
