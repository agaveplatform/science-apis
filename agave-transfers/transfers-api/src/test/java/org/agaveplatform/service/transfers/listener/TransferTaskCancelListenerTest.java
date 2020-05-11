package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.time.Instant;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.mockito.Mockito.*;



@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Task Canceled test")
//@Disabled
class TransferTaskCancelListenerTest extends BaseTestCase {

	TransferTaskCancelListener getMockListenerInstance(Vertx vertx) {
		TransferTaskCancelListener listener = Mockito.mock(TransferTaskCancelListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_CANCELLED);
		when(listener.getVertx()).thenReturn(vertx);

		return listener;
	}

	TransferTaskCancelListener getMockCancelAckListenerInstance(Vertx vertx) {
		TransferTaskCancelListener listener = Mockito.mock(TransferTaskCancelListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_CANCELED_ACK);
		when(listener.getVertx()).thenReturn(vertx);

		return listener;
	}

	@Test
	@DisplayName("Transfer Task Cancel Listener - processCancelAck")
	@Disabled
	public void processCancelAckTest(Vertx vertx, VertxTestContext ctx){
	// Set up our transfertask for testing
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());
		transferTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		//VertxTestContext testContext = new VertxTestContext();

//		TransferTaskCancelListener ttc = Mockito.mock(TransferTaskCancelListener.class);
//		Mockito.when(ttc.getEventChannel()).thenReturn(TRANSFERTASK_CANCELLED);
//		Mockito.when(ttc.getVertx()).thenReturn(vertx);
		TransferTaskCancelListener ttc = getMockCancelAckListenerInstance(vertx);


		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from allChildrenCancelledOrCompleted
		JsonObject expectedAllChildJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.COMPLETED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateAllChildHandler = getMockAsyncResult(expectedAllChildJsonObject);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateAllChildHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());


		// mock a successful outcome with updated json transfer task result from setTransferTaskCanceledIfNotCompleted
		JsonObject expectedCanceledIfNotJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.COMPLETED.name())
				.put("endTime", Instant.now());

		AsyncResult<JsonObject> updateCanceledIfNotHandler = getMockAsyncResult(expectedCanceledIfNotJsonObject);
		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
					.handle(updateCanceledIfNotHandler);
			return null;
		}).when(dbService).setTransferTaskCanceledIfNotCompleted(any(), any(), any());

		// mock a successful outcome with updated json transfer task result from processCancelAck
		JsonObject expectedProcessCancelAck = transferTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateProcessCancelAck = getMockAsyncResult(expectedProcessCancelAck);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
					.handle(updateProcessCancelAck);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());


		when(ttc.processCancelAck( eq(transferTask.toJson()),any())).thenCallRealMethod();

		Mockito.when(ttc.getTransferTask( eq(transferTask.getOwner()), eq(transferTask.getParentTaskId()), any() )).thenReturn(transferTask);

		Future<Boolean> result = ttc.processCancelAck(transferTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				assertTrue(results.result(), "The async should return true");

				verify(ttc)._doPublishEvent(TRANSFERTASK_CANCELED_COMPLETED, transferTask.toJson());
				verify(ttc, never())._doPublishEvent(eq(TRANSFERTASK_CANCELED_ACK), any());
				assertEquals(transferTask.getUuid(), results, "Transfer task was not acknowledged in response uuid");
				//assertEquals(uuid, result, "result should have been uuid: " + uuid);

				ctx.completeNow();
			});
		});
	}

	@Test
	@DisplayName("Transfer Task Canceled Listener with parent - processCancelRequest")
	@Disabled
	public void processCancelAckWithParent(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
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

		// mock out the allChildrenCancelledOrCompleted method
//		Mockito.when(listener.allChildrenCancelledOrCompleted( eq(parentTask.getParentTaskId()), eq(any()))).thenReturn(( eq( any() )));
//
		// mock a successful outcome with updated json transfer task result from processCancelAck
		JsonObject expectedProcessCancelAck = transferTask.toJson()
				.put("status", TransferStatusType.CANCELLED.name())
				.put("endTime", Instant.now());
		AsyncResult<JsonObject> updateProcessCancelAck = getMockAsyncResult(expectedProcessCancelAck);
		when(listener.processCancelAck( eq(parentTask.toJson()),any())).thenCallRealMethod();

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



		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
		AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.FALSE);
		// mock the handler passed into processParentEvent
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(1, Handler.class))
					.handle(processCancelRequest);
			return null;
		}).when(listener).processCancelAck( anyObject(), any());

		// now we run the actual test using our test transfer task data
		Future<Boolean> result = listener.processCancelAck(parentTask.toJson(), results -> {
			ctx.verify(() -> {
				assertTrue(results.succeeded(), "The call should succeed.");
				//assertTrue(results.result(), "The async should return true");

				// verify the db service was called to update the task status
//				verify(dbService).updateStatus(eq(transferTask.getTenantId()),
//						eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

				// verify that the completed event was created. this should always be throws
				// if the updateStatus result succeeds.
				verify(listener)._doPublishEvent(TRANSFERTASK_CANCELLED, transferTask.toJson());

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
	@DisplayName("Transfer Task Cancel Listener - processCancelRequest")
	//@Disabled
	public void processCancelAckParentTest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.TRANSFERRING);
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));
//		parentTask.setRootTaskId(parentTask.getUuid());
//		parentTask.setParentTaskId(parentTask.getUuid());

		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());
//		transferTask.setRootTaskId(parentTask.getUuid());
//		transferTask.setParentTaskId(parentTask.getUuid());

		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
		when(listener.getTransferTask(eq(transferTask.getUuid()), any(), any() ) ).thenReturn(transferTask);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(listener.getDbService()).thenReturn(dbService);

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


		// mock a successful outcome with a puased result from processCancelRequest indicating the child has no active parents
		AsyncResult<Boolean> processCancelRequest = getMockAsyncResult(Boolean.FALSE);

		// mock the handler passed into processParentEvent
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(processCancelRequest);
			return null;
		}).when(listener).processParentEvent(any(), any(), any());

		// now we run the actual test using our test transfer task data
		Future<Boolean> res = listener.processCancelRequest(transferTask.toJson(), result -> {
			result.result();
		});

		ctx.verify(() -> {
			// verify the db service was called to update the task status
//			verify(dbService).updateStatus(eq(transferTask.getTenantId()),
//					eq(transferTask.getUuid()), eq(TransferStatusType.CANCELLED.name()), any());

			// verify that the completed event was created. this should always be throws
			// if the updateStatus result succeeds.
//			verify(listener)._doPublishEvent(TRANSFERTASK_CANCELLED, transferTask.toJson());

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
	}

//	@Test
//	@DisplayName("Get transfer task for uuid.")
//	public void getTransferTaskTest(Vertx vertx, VertxTestContext ctx){
//		TransferTaskCancelListener ttc = Mockito.mock(TransferTaskCancelListener.class);
//		TransferTask tt = _createTestTransferTask();
//		String tenantId = tt.getTenantId();
//		String uuid = tt.getUuid();
//
//		Mockito.when(ttc.getEventChannel()).thenReturn(TRANSFERTASK_CANCELLED);
//		Mockito.when(ttc.getVertx()).thenReturn(vertx);
//
//		TransferTaskCancelListener listener = getMockListenerInstance(vertx);
//
//		//doAnswer((Answer )).when(listener.getTransferTask(anyString(), anyString(), any()));
//		when(listener.getTransferTask(eq(tt.getParentTaskId()), eq(tt.getUuid()), any() ) ).thenReturn(tt);
//
//		// mock out the db service so we can can isolate method logic rather than db
//		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);
//
//		// mock the dbService getter in our mocked vertical so we don't need to use powermock
//		when(listener.getDbService()).thenReturn(dbService);
//
//
//
//		// mock a successful outcome with updated json transfer task result from updateStatus
//		JsonObject expectedUdpatedJsonObject = tt.toJson()
//				.put("status", TransferStatusType.CANCELLED.name())
//				.put("endTime", Instant.now());
//
//		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
//
//		// mock the handler passed into updateStatus
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
//					.handle(updateStatusHandler);
//			return null;
//		}).when(dbService).getById(any(), any(), any());
//
//		// mock a successful outcome with a puased result from processPauseRequest indicating the child has no active parents
//		AsyncResult<Boolean> processPauseRequest = getMockAsyncResult(Boolean.FALSE);
//
//		// mock the handler passed into processParentEvent
//		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
//			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
//					.handle(processPauseRequest);
//			return null;
//		}).when(listener).getTransferTask(any(), any(), any());
//
//
//		// now we run the actual test using our test transfer task data
//		Future<TransferTask> result = listener.getTransferTask(tt.getTenantId(), tt.getUuid(),   );
//
//		ctx.verify(() -> {
//
//
//			ctx.completeNow();
//		});
//	}

}