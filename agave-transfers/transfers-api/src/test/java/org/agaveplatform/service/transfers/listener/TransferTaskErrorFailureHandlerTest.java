package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.exception.ObjectNotFoundException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;

import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_COMPLETED;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.FAILED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("TransferFailureHandler integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({ JDBCClient.class })
class TransferTaskErrorFailureHandlerTest extends BaseTestCase {
//	private static final Logger log = LoggerFactory.getLogger(TransferFailureHandlerTest.class);

	protected TransferTaskErrorFailureHandler getMockTransferFailureHandlerInstance(Vertx vertx) {
		TransferTaskErrorFailureHandler listener = mock(TransferTaskErrorFailureHandler.class );
		when(listener.getEventChannel()).thenReturn(TRANSFER_COMPLETED);
		when(listener.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(listener).processFailure(any(JsonObject.class), any());
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doNothing().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
		doCallRealMethod().when(listener).processBody(any(), any());
		return listener;
	}

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	@DisplayName("TransferFailureHandlerTest - testProcessFailure returns true on successful update")
	void testProcessFailure(Vertx vertx, VertxTestContext ctx) {
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setId(2L);

		JsonObject body = new JsonObject(transferTask.toJSON());
		body.put("cause", RemoteDataException.class.getName());
		body.put("message", "Error Message");

		TransferTaskErrorFailureHandler failureHandler = getMockTransferFailureHandlerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.FAILED.name())
				.put("endTime", Instant.now());

		AsyncResult<JsonObject> expectedUpdateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(expectedUpdateStatusHandler);
			return null;
		}).when(dbService).update(any(), any(), any(), any());

		// mock the handler passed into getById
		AsyncResult<JsonObject> expectedTransferTask = getMockAsyncResult(transferTask.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(expectedTransferTask);
			return null;
		}).when(dbService).getById(eq(transferTask.getId().toString()), any());

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(failureHandler.getDbService()).thenReturn(dbService);

		failureHandler.processFailure(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "FailureHandler update should have succeeded");
			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferFailureHandlerTest - testProcessFailure returns true on successful update")
	void testProcessFailureRecordsAndReturnsDBExceptions(Vertx vertx, VertxTestContext ctx) {
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setId(2L);

		JsonObject body = new JsonObject(transferTask.toJSON());
		body.put("cause", RemoteDataException.class.getName());
		body.put("message", "Error Message");

		TransferTaskErrorFailureHandler failureHandler = getMockTransferFailureHandlerInstance(vertx);
		doNothing().when(failureHandler)._doPublishEvent(any(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		AsyncResult<JsonObject> expectedFailureStatusHandler =
				getJsonObjectFailureAsyncResult(new ObjectNotFoundException("Should be passed up to the listener"));

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(expectedFailureStatusHandler);
			return null;
		}).when(dbService).update(any(), any(), any(), any());

		// mock the handler passed into getById
		AsyncResult<JsonObject> expectedTransferTask = getMockAsyncResult(transferTask.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(expectedTransferTask);
			return null;
		}).when(dbService).getById(eq(transferTask.getId().toString()), any());

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(failureHandler.getDbService()).thenReturn(dbService);

		failureHandler.processFailure(body, resp -> ctx.verify(() -> {
			assertFalse(resp.succeeded(), "Exception thrown from db update should result in failed response");
			assertEquals(resp.cause().getClass().getName(), ObjectNotFoundException.class.getName(),
					"Exception thrown from db update should be propagated back through handler to calling method");
			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorFailureHandler.processBody partial Transfer Task should return Transfer Task")
	protected void processBodyWithPartialTransferTaskTest(Vertx vertx, VertxTestContext ctx) {
		String parentId = new AgaveUUID(UUIDType.TRANSFER).toString();

		TransferTask tt = _createTestTransferTask();
		tt.setId(2L);
		tt.setStatus(FAILED);
		tt.setParentTaskId(parentId);
		tt.setRootTaskId(parentId);

		JsonObject body = new JsonObject()
				.put("id", tt.getId().toString())
				.put("cause", "java.lang.InterruptedException")
				.put("message", "Error Message");

		TransferTaskErrorFailureHandler txfrErrorFailureHandler = getMockTransferFailureHandlerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorFailureHandler.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		txfrErrorFailureHandler.processBody(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertNotNull(resp.result(), "processBody should return JsonObject for valid partial Transfer Task");
			assertEquals(tt.toJson(), resp.result(), "processBody should return the Transfer Task as JsonObject");

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferErrorFailureHandler.processBody Transfer Task should return Transfer Task")
	protected void processBodyWithTransferTaskTest(Vertx vertx, VertxTestContext ctx) {
		String parentId = new AgaveUUID(UUIDType.TRANSFER).toString();

		TransferTask tt = _createTestTransferTask();
		tt.setId(2L);
		tt.setStatus(FAILED);
		tt.setParentTaskId(parentId);
		tt.setRootTaskId(parentId);

		JsonObject body = tt.toJson();
		body.put("cause", "java.lang.InterruptedException");
		body.put("message", "Error Message");

		TransferTaskErrorFailureHandler txfrErrorFailureHandler = getMockTransferFailureHandlerInstance(vertx);

		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(getMockAsyncResult(tt.toJson().put("status", arguments.getArgumentAt(2, String.class))));
			return null;
		}).when(dbService).updateStatus( eq(tt.getTenantId()), eq(tt.getUuid()), any(), anyObject() );

		AsyncResult<JsonObject> getByIdResult = getMockAsyncResult(tt.toJson());
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(getByIdResult);
			return null;
		}).when(dbService).getById(eq(tt.getId().toString()), anyObject() );



		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(txfrErrorFailureHandler.getDbService()).thenReturn(dbService);
		// +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

		txfrErrorFailureHandler.processBody(body, resp -> ctx.verify(() -> {
			assertTrue(resp.succeeded(), "Error handler should succeed when an IOException is received.");
			assertNotNull(resp.result(), "processBody should return JsonObject for valid partial Transfer Task");
			assertEquals(tt.toJson(), resp.result(), "processBody should return the Transfer Task as JsonObject");
			ctx.completeNow();
		}));
	}
}