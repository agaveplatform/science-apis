package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Watch Listener Test")
class TransferTaskWatchListenerTest extends BaseTestCase {

	protected TransferTask _createTestTransferTask() {
		TransferTask transferTask = new TransferTask(TENANT_ID, TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, null, null);
		transferTask.setStartTime(Instant.now().minus(30, ChronoUnit.MINUTES));
		transferTask.setLastUpdated(transferTask.getStartTime());
		transferTask.setStatus(TransferStatusType.TRANSFERRING);

		return transferTask;
	}

	TransferTaskWatchListener getMockListenerInstance(Vertx vertx) throws Exception {
		TransferTaskWatchListener listener = Mockito.mock(TransferTaskWatchListener.class);
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_HEALTHCHECK);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.config()).thenReturn(config);
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doCallRealMethod().when(listener)._doPublishEvent(any(), any(), any());
		when(listener.createPushMessageSubject(any(), any(), any(), any())).thenCallRealMethod ();
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());

		NatsJetstreamMessageClient natsClient = mock(NatsJetstreamMessageClient.class);
		doNothing().when(natsClient).push(any(), any());
		when(listener.getMessageClient()).thenReturn(natsClient);

		return listener;
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - succeeds with no active transfer tasks")
	public void processEventSucceedsWhenNoTasks(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener transferTaskWatchListener = getMockListenerInstance(vertx);
		//NatsJetstreamMessageClient nats = getMockNats();

		doCallRealMethod().when(transferTaskWatchListener).processRootTaskEvent(any());

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock the handler passed into updateStatus
		AsyncResult<JsonArray> getActiveRootTaskIdsHandler = getMockAsyncResult(new JsonArray());
		doAnswer((Answer<AsyncResult<JsonArray>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonArray>> handler = arguments.getArgumentAt(0, Handler.class);
			handler.handle(getActiveRootTaskIdsHandler);
			return null;
		}).when(dbService).getActiveRootTaskIds(any());

		when(transferTaskWatchListener.getDbService()).thenReturn(dbService);

		transferTaskWatchListener.processRootTaskEvent(resp -> ctx.verify(() -> {
			verify(transferTaskWatchListener, never())._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any(), any());

			assertTrue(resp.result(),
					"Empty list returned from db mock should result in a true response to the callback.");

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - succeeds with a single active transfer tasks")
	public void processEventSucceedsWithASingleActiveTask(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener transferTaskWatchListener = getMockListenerInstance(vertx);
		doCallRealMethod().when(transferTaskWatchListener).processRootTaskEvent(any());

		JsonArray activeTasks = new JsonArray();
		TransferTask tt = _createTestTransferTask();
		activeTasks.add(new JsonObject()
				.put("uuid", tt.getUuid())
				.put("tenantId", tt.getTenantId())
				.put("last_updated", tt.getLastUpdated()));

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

		when(transferTaskWatchListener.getDbService()).thenReturn(dbService);

		transferTaskWatchListener.processRootTaskEvent(resp -> ctx.verify(() -> {
			assertTrue(resp.result(),
					"Successful processing of a list of a single transfer task should return true");

			// verify a call was made for each active task
			verify(transferTaskWatchListener)._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any(), any());
			verify(transferTaskWatchListener)._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), eq(activeTasks.getJsonObject(0)), any());

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - fails the doPublishEvent(TRANSFERTASK_HEALTHCHECK succeeds with a single active transfer tasks")
	public void processEventFailsWithASingleActiveTask(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener transferTaskWatchListener = getMockListenerInstance(vertx);
		doCallRealMethod().when(transferTaskWatchListener).processRootTaskEvent(any());

		JsonArray activeTasks = new JsonArray();
		TransferTask tt = _createTestTransferTask();
		activeTasks.add(new JsonObject()
				.put("uuid", tt.getUuid())
				.put("tenantId", tt.getTenantId())
				.put("last_updated", tt.getLastUpdated()));

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

		when(transferTaskWatchListener.getDbService()).thenReturn(dbService);

		AsyncResult<Boolean> eventHandler = getBooleanFailureAsyncResult(new Exception("Testing _doPublishEvent failure"));
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(eventHandler);
			return null;
		}).when(transferTaskWatchListener)._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), eq(activeTasks.getJsonObject(0)), any());


		transferTaskWatchListener.processRootTaskEvent(resp -> ctx.verify(() -> {
			assertTrue(resp.failed(),
					"Should fail processing of a list of a single transfer task should return true");

			// verify a call was made for each active task
			verify(transferTaskWatchListener)._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any(), any());

			ctx.completeNow();
		}));
	}


	@Test
	@DisplayName("Transfers Watch Listener Test - succeeds with multiple active transfer tasks")
	public void processEventHandlesMultipleActiveTasks(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener transferTaskWatchListener = getMockListenerInstance(vertx);
	//	NatsJetstreamMessageClient nats = getMockNats();

		doCallRealMethod().when(transferTaskWatchListener).processRootTaskEvent(any());

		JsonArray activeTasks = new JsonArray();
		for (int i=0; i<10; i++) {
			TransferTask tt = _createTestTransferTask();
			activeTasks.add(new JsonObject()
					.put("uuid", tt.getUuid())
					.put("tenantId", tt.getTenantId())
					.put("last_updated", tt.getLastUpdated()));
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

		when(transferTaskWatchListener.getDbService()).thenReturn(dbService);

		transferTaskWatchListener.processRootTaskEvent(resp -> ctx.verify(() -> {
			assertTrue(resp.result(),
					"Successful processing of a list of ids should return true");

			verify(transferTaskWatchListener, times(activeTasks.size()))._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), any(), any());

			for (int i=0; i<10; i++) {
				verify(transferTaskWatchListener)._doPublishEvent(eq(TRANSFERTASK_HEALTHCHECK), eq(activeTasks.getJsonObject(i)), any());
			}

			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("Transfers Watch Listener Test - fails the getActiveRootTaskIds with a single active transfer tasks")
	public void getActiveRootTaskIdsFailsWithASingleActiveTask(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener transferTaskWatchListener = getMockListenerInstance(vertx);
		doCallRealMethod().when(transferTaskWatchListener).processRootTaskEvent(any());

		JsonArray activeTasks = new JsonArray();
		TransferTask tt = _createTestTransferTask();
		activeTasks.add(new JsonObject()
				.put("uuid", tt.getUuid())
				.put("tenantId", tt.getTenantId())
				.put("last_updated", tt.getLastUpdated()));

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

		when(transferTaskWatchListener.getDbService()).thenReturn(dbService);

		AsyncResult<Boolean> eventHandler = getBooleanFailureAsyncResult(new Exception("Testing getActiveRootTaskIds failure"));
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(0, Handler.class);
			handler.handle(eventHandler);
			return null;
		}).when(dbService).getActiveRootTaskIds(any());


		transferTaskWatchListener.processRootTaskEvent(resp -> ctx.verify(() -> {
			assertTrue(resp.failed(),
					"Should fail processing of a list of a single transfer task should return true");

			// verify a call was made for each active task
			verify(transferTaskWatchListener,never())._doPublishEvent(any(), any(), any());

			ctx.completeNow();
		}));
	}


	//"no null address accepted"
	@Test
	@DisplayName("Transfers Watch Listener Test - fails the getActiveRootTaskIds with a single active transfer tasks")
	//@Disabled
	public void processRootTaskEventFailsWithASingleActiveTask(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener transferTaskWatchListener = getMockListenerInstance(vertx);
		doCallRealMethod().when(transferTaskWatchListener).processRootTaskEvent(any());

		JsonArray activeTasks = new JsonArray();
		TransferTask tt = _createTestTransferTask();
		activeTasks.add(new JsonObject()
				.put("uuid", tt.getUuid())
				.put("tenantId", tt.getTenantId())
				.put("last_updated", tt.getLastUpdated()));

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);
		when(transferTaskWatchListener.getDbService()).thenReturn(dbService);

		when(dbService.getActiveRootTaskIds(any())).thenThrow(new RuntimeException("no null address accepted"));

		transferTaskWatchListener.processRootTaskEvent(resp -> ctx.verify(() -> {
			assertFalse(resp.failed(),
					"Should fail a general error from the transfer task should return true");

			// verify a call was made for each active task
			//verify(transferTaskWatchListener,never())._doPublishEvent(any(), any(), any());

			ctx.completeNow();
		}));
	}

    //"Error with TransferTaskWatchListener processEvent"
	@Test
	@DisplayName("Transfers Watch Listener Test - fails the getActiveRootTaskIds")
	//@Disabled
	public void processRootTaskEventFailsWithAGeneralError(Vertx vertx, VertxTestContext ctx) throws Exception {

		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskWatchListener transferTaskWatchListener = getMockListenerInstance(vertx);
		doCallRealMethod().when(transferTaskWatchListener).processRootTaskEvent(any());

		JsonArray activeTasks = new JsonArray();
		TransferTask tt = _createTestTransferTask();
		activeTasks.add(new JsonObject()
				.put("uuid", tt.getUuid())
				.put("tenantId", tt.getTenantId())
				.put("last_updated", tt.getLastUpdated()));

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);
		when(transferTaskWatchListener.getDbService()).thenReturn(dbService);

		when(dbService.getActiveRootTaskIds(any())).thenThrow(new RuntimeException("Error with TransferTaskWatchListener processEvent"));

		transferTaskWatchListener.processRootTaskEvent(resp -> ctx.verify(() -> {
			assertTrue(resp.failed(),
					"Should fail a general error from the transfer task should return true");

			// verify a call was made for each active task
			//verify(transferTaskWatchListener,never())._doPublishEvent(any(), any(), any());

			ctx.completeNow();
		}));
	}
}
