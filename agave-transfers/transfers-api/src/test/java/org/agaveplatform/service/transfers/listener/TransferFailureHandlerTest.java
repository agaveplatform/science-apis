package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("TransferFailureHandler integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({ JDBCClient.class })
class TransferFailureHandlerTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferFailureHandlerTest.class);

	protected TransferFailureHandler getMockTransferFailureHandlerInstance(Vertx vertx) {
		TransferFailureHandler ttc = mock(TransferFailureHandler.class );
		when(ttc.getEventChannel()).thenReturn(TRANSFER_COMPLETED);
		when(ttc.getVertx()).thenReturn(vertx);
		when(ttc.processFailure(anyObject())).thenCallRealMethod();
		return ttc;
	}

	private EventBus eventBus;
	private TransferTaskDatabaseService service;

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	@DisplayName("testProcessFailure Test")
	void testProcessFailure(Vertx vertx, VertxTestContext ctx) {
		TransferTask transferTask = _createTestTransferTask();

		JsonObject body = new JsonObject(transferTask.toJSON());
		body.put("id", new AgaveUUID(UUIDType.TRANSFER).toString());
		body.put("cause", "org.iplantc.service.transfer.exceptions.RemoteDataException");
		body.put("message", "Error Message");

		TransferFailureHandler failureHandler = getMockTransferFailureHandlerInstance(Vertx.vertx());
		doNothing().when(failureHandler)._doPublishEvent(any(), any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from updateStatus
		JsonObject expectedUdpatedJsonObject = transferTask.toJson()
				.put("status", TransferStatusType.COMPLETED.name())
				.put("endTime", Instant.now());

		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
					.handle(updateStatusHandler);
			return null;
		}).when(dbService).update(any(), any(), any(), any());

		// mock the dbService getter in our mocked vertical so we don't need to use powermock
		when(failureHandler.getDbService()).thenReturn(dbService);


		Future<Boolean> result = failureHandler.processFailure(body);

		ctx.verify(() -> {
			// make sure no error event is ever thrown
			//verify(failureHandler, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

//			Assertions.assertTrue(result.result(),
//					"TransferTask response should be true indicating the task completed successfully.");

			Assertions.assertTrue(result.succeeded(), "FailureHandler update should have succeeded");
			ctx.completeNow();
		});
	}
}