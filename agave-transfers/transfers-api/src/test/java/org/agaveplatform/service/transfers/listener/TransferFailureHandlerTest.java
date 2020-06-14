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
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("TransferFailureHandler integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({ JDBCClient.class })
class TransferFailureHandlerTest extends BaseTestCase {
//	private static final Logger log = LoggerFactory.getLogger(TransferFailureHandlerTest.class);

	protected TransferFailureHandler getMockTransferFailureHandlerInstance(Vertx vertx) {
		TransferFailureHandler ttc = mock(TransferFailureHandler.class );
		when(ttc.getEventChannel()).thenReturn(TRANSFER_COMPLETED);
		when(ttc.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(ttc).processFailure(any(JsonObject.class), any());
		return ttc;
	}

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	@DisplayName("TransferFailureHandlerTest - testProcessFailure returns true on successful update")
	void testProcessFailure(Vertx vertx, VertxTestContext ctx) {
		TransferTask transferTask = _createTestTransferTask();

		JsonObject body = new JsonObject(transferTask.toJSON());
		body.put("id", new AgaveUUID(UUIDType.TRANSFER).toString());
		body.put("cause", RemoteDataException.class.getName());
		body.put("message", "Error Message");

		TransferFailureHandler failureHandler = getMockTransferFailureHandlerInstance(vertx);
		doNothing().when(failureHandler)._doPublishEvent(any(), any());

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

		JsonObject body = new JsonObject(transferTask.toJSON());
		body.put("id", new AgaveUUID(UUIDType.TRANSFER).toString());
		body.put("cause", RemoteDataException.class.getName());
		body.put("message", "Error Message");

		TransferFailureHandler failureHandler = getMockTransferFailureHandlerInstance(vertx);
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

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(failureHandler.getDbService()).thenReturn(dbService);

		failureHandler.processFailure(body, resp -> ctx.verify(() -> {
			assertFalse(resp.succeeded(), "Exception thrown from db update should result in failed response");
			assertEquals(resp.cause().getClass().getName(), ObjectNotFoundException.class.getName(),
					"Exception thrown from db update should be propagated back through handler to calling method");
			ctx.completeNow();
		}));
	}
}