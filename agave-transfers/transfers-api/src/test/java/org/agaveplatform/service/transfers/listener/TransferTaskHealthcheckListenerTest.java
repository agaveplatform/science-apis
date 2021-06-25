package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.matchers.IsSameJsonTransferTask;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.messaging.Message;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.sql.SQLException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.COMPLETED;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Watch Listener Test")
class TransferTaskHealthcheckListenerTest extends BaseTestCase {

	private TransferTaskDatabaseService dbService;
	private Vertx vertx;
	private JWTAuth jwtAuth;

	protected TransferTask _createTestTransferTask() {
		TransferTask transferTask = new TransferTask(TENANT_ID, TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, null, null);
		transferTask.setStartTime(Instant.now().minus(30, ChronoUnit.MINUTES));
		transferTask.setLastUpdated(transferTask.getStartTime());
		transferTask.setStatus(TransferStatusType.TRANSFERRING);

		return transferTask;
	}

	TransferTaskHealthcheckListener getMockListenerInstance(Vertx vertx) throws Exception {
		TransferTaskHealthcheckListener listener = Mockito.mock(TransferTaskHealthcheckListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_HEALTHCHECK);
		when(listener.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(listener).processAllChildrenCanceledEvent(any(), any());
		when(listener.config()).thenReturn(config);
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doCallRealMethod().when(listener)._doPublishEvent(any(), any(), any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());

		NatsJetstreamMessageClient natsClient = mock(NatsJetstreamMessageClient.class);
		doNothing().when(natsClient).push(any(), any());
		when(listener.getMessageClient()).thenReturn(natsClient);
		doCallRealMethod().when(listener).handleMessage(any());

		return listener;
	}

	@Test
	@DisplayName("handleMessage Test")
	public void processHandleMessageTest(Vertx vertx, VertxTestContext ctx) throws Exception{
		// mock out the test class
		TransferTaskHealthcheckListener ta = getMockListenerInstance(vertx);
		// generate a fake transfer task
		TransferTask transferTask = _createTestTransferTask();
		JsonObject transferTaskJson = transferTask.toJson();

		Message msg = new Message(1, transferTask.toString());
		ta.handleMessage(msg);
		ctx.verify(() -> {
			verify(ta, atLeastOnce()).processAllChildrenCanceledEvent(eq(transferTaskJson), any());

			ctx.completeNow();
		});
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - no active children should mark task as complete")
	public void processEventNoActiveChildrenCompletesTask(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskHealthcheckListener transferTaskHealthcheckListener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		TransferTask transferTask = _createTestTransferTask();
		JsonObject json = transferTask.toJson();

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.COMPLETED.name())
				.put("endTime", Instant.now());

		// mock the handler passed into updateStatus
		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());

		// mock the handler passed into updateStatus
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		when(transferTaskHealthcheckListener.getDbService()).thenReturn(dbService);

		transferTaskHealthcheckListener.processAllChildrenCanceledEvent(json, ctx.succeeding( resp -> ctx.verify(() -> {
			verify(dbService).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any());
			verify(dbService).updateStatus(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(COMPLETED.name()), any());

			verify(transferTaskHealthcheckListener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any(), any());
			verify(transferTaskHealthcheckListener)._doPublishEvent(eq(TRANSFERTASK_FINISHED), any(), any());

			Assertions.assertTrue(resp, "All children completed should result in success response and result");

			ctx.completeNow();
		})));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - active children should leave task as is")
	public void processEventActiveChildrenLeavesTaskAsIs(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskHealthcheckListener transferTaskHealthcheckListener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		TransferTask transferTask = _createTestTransferTask();
		JsonObject json = transferTask.toJson();

		// mock the handler passed into updateStatus
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.FALSE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		when(transferTaskHealthcheckListener.getDbService()).thenReturn(dbService);

		transferTaskHealthcheckListener.processAllChildrenCanceledEvent(json, ctx.succeeding( resp -> ctx.verify(() -> {
			//verify(dbService).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any());

			//verify(dbService, never()).updateStatus(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(COMPLETED.name()), any());

			//verify(transferTaskHealthcheckListener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any(), any());
			//verify(transferTaskHealthcheckListener, never())._doPublishEvent(eq(TRANSFERTASK_FINISHED), any(), any());

			Assertions.assertTrue(resp, "All children completed should result in success response and result");

			ctx.completeNow();
		})));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - active children and over 1 month since startTime should error task")
	public void processEventWithActiveChildrenIsKilledAfterOneMonth(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskHealthcheckListener transferTaskHealthcheckListener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// create a transfer task that started over a month ago.
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStartTime(Instant.now().minus(30, ChronoUnit.DAYS).minusSeconds(1));
		transferTask.setLastUpdated(transferTask.getStartTime());

		JsonObject json = transferTask.toJson();

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.COMPLETED.name());

		// mock the handler passed into updateStatus
		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());

		// mock the handler passed into updateStatus
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.FALSE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		when(transferTaskHealthcheckListener.getDbService()).thenReturn(dbService);

		transferTaskHealthcheckListener.processAllChildrenCanceledEvent(json, ctx.succeeding( resp -> ctx.verify(() -> {
			verify(dbService).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any());
			//verify(transferTaskHealthcheckListener)._doPublishEvent(eq(TRANSFERTASK_ERROR), argThat(new IsSameJsonTransferTask(expectedUdpatedJsonObject)), any());

			//verify(dbService, never()).updateStatus(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(COMPLETED.name()), any());
			//verify(transferTaskHealthcheckListener, never())._doPublishEvent(eq(TRANSFERTASK_FINISHED), any(), any());

			//Assertions.assertTrue(resp, "All children completed should result in success response and result");

			ctx.completeNow();
		})));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - noactive children raises exception when update fails")
	public void processEventNoActiveChildrenRaisesErrorEventWhenUpdateFailsh(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskHealthcheckListener transferTaskHealthcheckListener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// create a transfer task that started over a month ago.
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStartTime(Instant.now().minus(30, ChronoUnit.DAYS).minusSeconds(1));
		transferTask.setLastUpdated(transferTask.getStartTime());

		JsonObject json = transferTask.toJson();
		SQLException expectedException = new SQLException("Update failed");

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("cause", expectedException.getClass().getName())
				.put("message", expectedException.getMessage());

		// mock the handler passed into updateStatus
		AsyncResult<JsonObject> updateErrorStatusHandler = getJsonObjectFailureAsyncResult(expectedException);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateErrorStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());

		// mock the handler passed into updateStatus
		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.TRUE);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		when(transferTaskHealthcheckListener.getDbService()).thenReturn(dbService);

		transferTaskHealthcheckListener.processAllChildrenCanceledEvent(json, ctx.failing( cause -> ctx.verify(() -> {
			verify(dbService).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any());

			verify(transferTaskHealthcheckListener, never())._doPublishEvent(eq(TRANSFERTASK_FINISHED), any(), any());
			verify(transferTaskHealthcheckListener)._doPublishEvent(eq(TRANSFERTASK_ERROR), argThat(new IsSameJsonTransferTask(expectedUdpatedJsonObject)), any());

			Assertions.assertTrue(cause instanceof SQLException, "All children completed should result in success response and result");

			verify(dbService).updateStatus(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), eq(COMPLETED.name()), any());
			ctx.completeNow();
		})));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - error to resolve children resolves false")
	public void processEventErrorQueryingChildrenResolvesFalse(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskHealthcheckListener transferTaskHealthcheckListener = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// create a transfer task that started over a month ago.
		TransferTask transferTask = _createTestTransferTask();
		JsonObject json = transferTask.toJson();

		SQLException expectedException = new SQLException("Update failed");

		// mock the handler passed into updateStatus
		AsyncResult<Boolean> allChildrenCancelledOrCompletedExceptionHandler = getBooleanFailureAsyncResult(expectedException);
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedExceptionHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		when(transferTaskHealthcheckListener.getDbService()).thenReturn(dbService);

		transferTaskHealthcheckListener.processAllChildrenCanceledEvent(json, ctx.failing( cause -> ctx.verify(() -> {
			verify(dbService).allChildrenCancelledOrCompleted(eq(transferTask.getTenantId()), eq(transferTask.getUuid()), any());
			Assertions.assertTrue(cause instanceof SQLException, "Error querying children should be resolved back up the chain");

			verify(dbService, never()).updateStatus(any(), any(), any(), any());
			verify(transferTaskHealthcheckListener, never())._doPublishEvent(eq(TRANSFERTASK_FINISHED), any(), any());
			verify(transferTaskHealthcheckListener, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any(), any());

			ctx.completeNow();
		})));
	}
}
