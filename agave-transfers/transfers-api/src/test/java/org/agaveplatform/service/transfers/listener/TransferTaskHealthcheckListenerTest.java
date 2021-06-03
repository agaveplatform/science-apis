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
import org.iplantc.service.common.exceptions.MessagingException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
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

	TransferTaskHealthcheckListener getMockListenerInstance(Vertx vertx) throws IOException, InterruptedException {
		TransferTaskHealthcheckListener listener = Mockito.mock(TransferTaskHealthcheckListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_HEALTHCHECK);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.processAllChildrenCanceledEvent(any())).thenCallRealMethod();
		when(listener.config()).thenReturn(config);
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doNothing().when(listener)._doPublishEvent(any(), any());
		doNothing().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
		return listener;
	}
//	NatsJetstreamMessageClient getMockNats() throws MessagingException {
//		NatsJetstreamMessageClient natsClient = Mockito.mock(NatsJetstreamMessageClient.class);
//		doNothing().when(natsClient).push(any(), any(), any());
//		return getMockNats();
//	}

	@Test
	@DisplayName("Transfers Watch Listener Test - processEvent")
	public void processEvent(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, TimeoutException, MessagingException {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskHealthcheckListener thc = getMockListenerInstance(vertx);
//		NatsJetstreamMessageClient nats = getMockNats();

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
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(3, Handler.class);
			handler.handle(updateStatusHandler);
			return null;
		}).when(dbService).updateStatus(any(), any(), any(), any());

		AsyncResult<Boolean> allChildrenCancelledOrCompletedHandler = getMockAsyncResult(Boolean.FALSE);

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(allChildrenCancelledOrCompletedHandler);
			return null;
		}).when(dbService).allChildrenCancelledOrCompleted(any(), any(), any());

		when(thc.getDbService()).thenReturn(dbService);

		Future<Boolean> result = thc.processAllChildrenCanceledEvent(json);

		// empty list response from db mock should result in no healthcheck events being raised
		//verify(thc, never())._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any());
		//verify(thc, never())._doPublishEvent(eq(TRANSFERTASK_ERROR), any());
//		verify(nats, never()).push(any(),eq(TRANSFERTASK_ERROR),any());
		Assertions.assertTrue(result.result(),
				"Empty list returned from db mock should result in a true response to the callback.");

		ctx.completeNow();
	}
}
