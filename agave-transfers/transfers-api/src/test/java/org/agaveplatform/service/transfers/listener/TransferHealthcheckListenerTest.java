package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.time.Instant;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Watch Listener Test")
class TransferHealthcheckListenerTest extends BaseTestCase {

	private TransferTaskDatabaseService dbService;
	private Vertx vertx;
	private JWTAuth jwtAuth;

	TransferHealthcheckListener getMockListenerInstance(Vertx vertx) {
		TransferHealthcheckListener thc = Mockito.mock(TransferHealthcheckListener.class);
		when(thc.getEventChannel()).thenReturn(TRANSFERTASK_HEALTHCHECK);
		when(thc.getVertx()).thenReturn(vertx);
		when(thc.processEvent(any())).thenCallRealMethod();
		when(thc.config()).thenReturn(config);
		doNothing().when(thc)._doPublishEvent(anyString(), any());
		return thc;
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - processEvent")
	public void processEvent(Vertx vertx, VertxTestContext ctx) {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferHealthcheckListener thc = getMockListenerInstance(vertx);

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		TransferTask transferTask = _createTestTransferTask();
		JsonObject json = transferTask.toJson();

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
		}).when(dbService).updateStatus(any(), any(), any(), any());

		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.FALSE);

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			((Handler<AsyncResult<Boolean>>) arguments.getArgumentAt(2, Handler.class))
					.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		when(thc.getDbService()).thenReturn(dbService);

		Future<Boolean> result = thc.processEvent(json);

		// empty list response from db mock should result in no healthcheck events being raised
		verify(thc, never())._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any());
		verify(thc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());

		Assertions.assertTrue(result.result(),
				"Empty list returned from db mock should result in a true response to the callback.");

		ctx.completeNow();
	}
}
