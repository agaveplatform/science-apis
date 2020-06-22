package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_RETRY;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("TransferErrorListener test")
//@Disabled
class TransferTaskErrorListenerTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskErrorListener.class);

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	protected TransferTaskErrorListener getMockTransferErrorListenerInstance(Vertx vertx) {
		TransferTaskErrorListener listener = mock(TransferTaskErrorListener.class );
		when(listener.config()).thenReturn(config);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_ERROR);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.config()).thenReturn(config);
		when(listener.getRecoverableExceptionsClassNames()).thenCallRealMethod();
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		when(listener.taskIsNotInterrupted(any())).thenReturn(true);
		when(listener.getRecoverableExceptionsClassNames()).thenCallRealMethod();
		doNothing().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
		doCallRealMethod().when(listener).processError(any(), any());

		return listener;
	}

	@Test
	@DisplayName("TransferErrorListener.processError RemoteDataException and Status= QUEUED test")
	protected void processErrorRDE_test(Vertx vertx, VertxTestContext ctx) {
		TransferTask tt = _createTestTransferTask();
		tt.setId(4L);
		tt.setStatus(QUEUED);

		JsonObject body = tt.toJson();
		body.put("cause", RemoteDataException.class.getName());
		body.put("message", "Error Message");

		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

//		// mock a successful outcome with updated json transfer task result from updateStatus
//		JsonObject expectedUdpatedJsonObject = tt.toJson()
//				.put("status", FAILED.name())
//				.put("endTime", Instant.now());

//		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );


		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "processError should succeed when in an active state");
			assertTrue(resp.result(), "processError should return false when the body has a recoverable cause and has a status of QUEUED");
			// active statuses should be moved to error state
			verify(dbService).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), eq(ERROR.name()), any());
			verify(txfrErrorListener)._doPublishEvent(TRANSFER_RETRY, tt.toJson().put("status", ERROR.name()));
			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError IOException and Status= QUEUED test")
	//@Disabled
	protected void processErrorIOE_test(Vertx vertx, VertxTestContext ctx) {
		TransferTask tt = _createTestTransferTask();
		tt.setId(3L);
		tt.setStatus(QUEUED);

		JsonObject body = tt.toJson();
		body.put("cause", IOException.class.getName());
		body.put("message", "Error Message");

		log.info("Cause: = {}", body.getString("cause"));
		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

//		// mock a successful outcome with updated json transfer task result from updateStatus
//		JsonObject expectedUdpatedJsonObject = tt.toJson()
//				.put("status", ERROR.name())
//				.put("endTime", Instant.now());
//
//		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			verify(txfrErrorListener).taskIsNotInterrupted(eq(tt));
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertTrue(resp.result(), "processError should succeed when the " +
					"TransferErrorListener.processError is called for an IOException. It is already in the QUEUE");
			// active statuses should be moved to error state
			verify(dbService).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), eq(ERROR.name()), any());
			verify(txfrErrorListener)._doPublishEvent(TRANSFER_RETRY, tt.toJson().put("status", ERROR.name()));
			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorListener.processError IOException and Status= COMPLETED test")
	//@Disabled
	protected void processErrorCOMPLETED_test(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = _createTestTransferTask();
		tt.setId(1L);
		tt.setStatus(COMPLETED);

		JsonObject body = tt.toJson();
		body.put("cause", IOException.class.getName());
		body.put("message", "Error Message");
		//body.put("status", "COMPLETED");

		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

//		// mock a successful outcome with updated json transfer task result from updateStatus
//		JsonObject expectedUdpatedJsonObject = tt.toJson()
//				.put("status", FAILED.name())
//				.put("endTime", Instant.now());
//
//		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertFalse(resp.result(), "processError should return FALSE when the TransferErrorListener.processError is called for an IOException and Status = COMPLETED");
			// no status update for tasks in done state
			verify(dbService, never()).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), any(), any());
			verify(txfrErrorListener, never())._doPublishEvent(any(), any());
			ctx.completeNow();
		}));
	}


	@Test
	@DisplayName("TransferErrorListener.processError InterruptedException and Status= FAILED test")
	//@Disabled
	protected void processErrorInterruptedException_FAILED_test(Vertx vertx, VertxTestContext ctx) {
		TransferTask tt = _createTestTransferTask();
		tt.setId(2L);
		tt.setStatus(FAILED);

		JsonObject body = tt.toJson();
		body.put("cause", "java.lang.InterruptedException");
		body.put("message", "Error Message");

		log.info("Cause: = {}", body.getString("cause"));
		TransferTaskErrorListener txfrErrorListener = getMockTransferErrorListenerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

//		// mock a successful outcome with updated json transfer task result from updateStatus
//		JsonObject expectedUdpatedJsonObject = tt.toJson()
//				.put("status", FAILED.name())
//				.put("endTime", Instant.now());
//
//		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorListener.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++


		txfrErrorListener.processError(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertFalse(resp.result(), "processError should return FALSE when the TransferErrorListener.processError is called for an IOException and Status = FAILED");
			// no status update for tasks in done state
			verify(dbService, never()).updateStatus(eq(tt.getTenantId()), eq(tt.getUuid()), any(), any());
			verify(txfrErrorListener, never())._doPublishEvent(any(), any());
			ctx.completeNow();
		}));
	}
}