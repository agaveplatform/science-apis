package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.when;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Watch Listener Test")
class TransferWatchListenerTest extends BaseTestCase {

	private TransferTaskDatabaseService dbService;
	private Vertx vertx;
	private JWTAuth jwtAuth;


	TransferWatchListener getMockListenerInstance(Vertx vertx) {
		TransferWatchListener twc = Mockito.mock(TransferWatchListener.class);
		when(twc.getEventChannel()).thenReturn(TRANSFERTASK_HEALTHCHECK);
		when(twc.getVertx()).thenReturn(vertx);
		when(twc.processEvent()).thenCallRealMethod();
		when(twc.config()).thenReturn(config);
		doNothing().when(twc)._doPublishEvent(anyString(), any());
		return twc;
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - processEvent")
	public void processEvent(Vertx vertx, VertxTestContext ctx) {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferWatchListener twc = getMockListenerInstance(vertx);

		TransferTask transferTask = _createTestTransferTask();

		JsonObject json = transferTask.toJson();

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		AsyncResult<JsonArray> getActiveRootTaskIdsHandler = getMockAsyncResult(new JsonArray(new ArrayList()));

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {
			((Handler<AsyncResult<JsonArray>>) arguments.getArgumentAt(0, Handler.class))
					.handle(getActiveRootTaskIdsHandler);
			return null;
		}).when(dbService).getActiveRootTaskIds(any());

		when(twc.getDbService()).thenReturn(dbService);

		Future<Boolean> result = twc.processEvent();

		// empty list response from db mock should result in no healthcheck events being raised
		verify(twc, never())._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any());

		Assertions.assertTrue(result.result(),
				"Empty list returned from db mock should result in a true response to the callback.");

		ctx.completeNow();
	}
}
