package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Watch Listener Test")
class TransferTaskWatchListenerTest extends BaseTestCase {

	TransferTaskWatchListener getMockListenerInstance(Vertx vertx) throws IOException, InterruptedException, TimeoutException {
		TransferTaskWatchListener listener = Mockito.mock(TransferTaskWatchListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_HEALTHCHECK);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.config()).thenReturn(config);
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doNothing().when(listener)._doPublishEvent(any(), any());
		doNothing().when(listener)._doPublishNatsJSEvent( any(), any());
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
		return listener;
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - succeeds with no active transfer tasks")
	public void processEvent(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, TimeoutException {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener twc = getMockListenerInstance(vertx);
		doCallRealMethod().when(twc).processEvent(any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		AsyncResult<JsonArray> getActiveRootTaskIdsHandler = getMockAsyncResult(new JsonArray());

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonArray>> handler = arguments.getArgumentAt(0, Handler.class);
			handler.handle(getActiveRootTaskIdsHandler);
			return null;
		}).when(dbService).getActiveRootTaskIds(any());

		when(twc.getDbService()).thenReturn(dbService);

		twc.processEvent(resp -> ctx.verify(() -> {
			assertTrue(resp.result(),
					"Empty list returned from db mock should result in a true response to the callback.");

			// empty list response from db mock should result in no healthcheck events being raised
			verify(twc, never())._doPublishNatsJSEvent(eq(TRANSFERTASK_HEALTHCHECK), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - succeeds with a single active transfer tasks")
	public void processEventHandlesASingleActiveTask(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, TimeoutException {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener twc = getMockListenerInstance(vertx);
		doCallRealMethod().when(twc).processEvent(any());

		JsonArray activeTasks = new JsonArray();
		for (int i=1; i<10; i++) {
			TransferTask tt = _createTestTransferTask();
			activeTasks.add(new JsonObject().put("uuid", tt.getUuid()).put("tenantId", tt.getTenantId()));
		}

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		AsyncResult<JsonArray> getActiveRootTaskIdsHandler = getMockAsyncResult(activeTasks);

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonArray>> handler = arguments.getArgumentAt(0, Handler.class);
			handler.handle(getActiveRootTaskIdsHandler);
			return null;
		}).when(dbService).getActiveRootTaskIds(any());

		when(twc.getDbService()).thenReturn(dbService);

		twc.processEvent(resp -> ctx.verify(() -> {
			assertTrue(resp.result(),
					"Successful processing of a list of a single id should return true");

			// verify a call was made for each active task
			for (int i=0; i<activeTasks.size(); i++) {
				verify(twc, times(1))._doPublishNatsJSEvent(eq(TRANSFERTASK_HEALTHCHECK), eq(activeTasks.getJsonObject(i)));
			}

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - succeeds with multiple active transfer tasks")
	public void processEventHandlesMultipleActiveTasks(Vertx vertx, VertxTestContext ctx) throws IOException, InterruptedException, TimeoutException {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener twc = getMockListenerInstance(vertx);
		doCallRealMethod().when(twc).processEvent(any());

		JsonArray activeTasks = new JsonArray();
		for (int i=0; i<10; i++) {
			TransferTask tt = _createTestTransferTask();
			activeTasks.add(new JsonObject().put("uuid", tt.getUuid()).put("tenantId", tt.getTenantId()));
		}

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		AsyncResult<JsonArray> getActiveRootTaskIdsHandler = getMockAsyncResult(activeTasks);

		// mock the handler passed into updateStatus
		doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonArray>> handler = arguments.getArgumentAt(0, Handler.class);
			handler.handle(getActiveRootTaskIdsHandler);
			return null;
		}).when(dbService).getActiveRootTaskIds(any());

		when(twc.getDbService()).thenReturn(dbService);

		twc.processEvent(resp -> ctx.verify(() -> {
			assertTrue(resp.result(),
					"Successful processing of a list of ids should return true");

			// total call count should match active task count
//			verify(twc, times(activeTasks.size()))._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any());

			// verify a call was made for each active task
			for (int i=0; i<activeTasks.size(); i++) {
				verify(twc, times(1))._doPublishNatsJSEvent(eq(TRANSFERTASK_HEALTHCHECK), eq(activeTasks.getJsonObject(i)));
			}

			ctx.completeNow();
		}));
	}
}
