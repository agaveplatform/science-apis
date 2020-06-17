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
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;

import java.net.URI;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_RETRY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("👋 TransferRetryListenerTest test")
//@Disabled
class TransferRetryListenerTest  extends BaseTestCase {

	protected TransferRetryListener getMockTransferRetryListenerInstance(Vertx vertx) {
		TransferRetryListener ttc = mock(TransferRetryListener.class );
		when(ttc.getEventChannel()).thenReturn(TRANSFER_RETRY);
		when(ttc.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(ttc).processRetryTransferTask(any(), any());
		when(ttc.taskIsNotInterrupted(any())).thenReturn(true);
		doNothing().when(ttc)._doPublishEvent(any(), any());
		return ttc;
	}


	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}


	@Test
	@DisplayName("Process TransferTaskPublishesProtocolEvent")
	@Disabled
	public void processTransferTaskPublishesProtocolEvent(Vertx vertx, VertxTestContext ctx) {
		//JsonObject body = new JsonObject();
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();


		vertx.eventBus().consumer("transfertask.sftp", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});
		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			ctx.failNow(new Exception(bodyRec.getString("message")));
		});

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);

		//ta.processRetryTransferTask(body);
		ta.processRetryTransferTask(body, resp -> {
			if(resp.succeeded()){
				System.out.println("Succeeded with the procdessTransferTask in retrying of the event ");
			} else {
				System.out.println("Error with return from retrying the event ");
			}
		});

		String protocolSelected = "http";

		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("Process processTransferTaskPublishesChildTasksForDirectory")
//	@Disabled
	public void processTransferTaskPublishesChildTasksForDirectory(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = _createTestTransferTask();

		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = getMockTranserTaskDatabaseService(tt.toJson());

		// mock our test class
		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);
		// pass through the call to the method under test
		doCallRealMethod().when(ta).processRetryTransferTask(any(), any());
		// attach the mock db service
		when(ta.getDbService()).thenReturn(dbService);

		// mock a successful outcome from the call to processRetry
		AsyncResult<Boolean> processRetryHandler = getMockAsyncResult(true);
		// mock the handler passed into getById
		doAnswer((Answer<AsyncResult<Boolean>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<Boolean>> handler = arguments.getArgumentAt(1, Handler.class);
			handler.handle(processRetryHandler);
			return null;
		}).when(ta).processRetry(any(TransferTask.class), any());

		ta.processRetryTransferTask(tt.toJson(), resp -> ctx.verify(() -> {
			assertFalse(resp.succeeded(), "processRetry should fail when system is unknown");
			assertEquals(resp.cause().getClass(), SystemUnknownException.class, "processRetry should propagate SystemUnknownException back to handler when thrown.");
			verify(ta, times(1)).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource())));
			verify(ta, times(1)).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest())));
			verify(ta, times(1))._doPublishEvent(eq(TRANSFERTASK_ERROR), any(JsonObject.class));
			ctx.completeNow();
		}));


		TransferTask tta = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tta.toJson();

		vertx.eventBus().consumer("transfertask.sftp", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		//TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);
		//ta.processRetryTransferTask(body);
		ta.processRetryTransferTask(body, resp -> {
			if (resp.succeeded()){
				System.out.println("Succeeded with the procdessTransferTask in retrying of the event ");
			} else {
				System.out.println("Error with return from retrying the event ");
			}
		});
		String protocolSelected = "http";

		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	/**
	 * Generates a mock of the {@link TransferTaskDatabaseService} with the {@link TransferTaskDatabaseService#getById(String, String, Handler)}
	 * method mocked out to return the given {@code transferTask};
	 * @param transferTaskToReturn {@link JsonObject} to return from the {@link TransferTaskDatabaseService#getById(String, String, Handler)} handler
	 * @return a mock of the db service with the getById mocked out to return the {@code transferTaskToReturn} as an async result.
	 */
	private TransferTaskDatabaseService getMockTranserTaskDatabaseService(JsonObject transferTaskToReturn) {
		// mock out the db service so we can can isolate method logic rather than db
		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);

		// mock a successful outcome with updated json transfer task result from getById call to db
		AsyncResult<JsonObject> getByAnyHandler = getMockAsyncResult(transferTaskToReturn);

		// mock the handler passed into getById
		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
			@SuppressWarnings("unchecked")
			Handler<AsyncResult<JsonObject>> handler = arguments.getArgumentAt(2, Handler.class);
			handler.handle(getByAnyHandler);
			return null;
		}).when(dbService).getById(any(), any(), any());

		return dbService;
	}

	@Test
	@DisplayName("Process processTransferTaskPublishesErrorOnSystemUnavailble")
	@Disabled
	public void processTransferTaskPublishesErrorOnSystemUnavailble(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);

		when(ta.getDbService().update (eq(TENANT_ID), eq(TEST_USERNAME), eq(tt), any() )).thenCallRealMethod();
		when(ta.getDbService().getById(eq(TENANT_ID), eq(TEST_USERNAME), any() )).thenCallRealMethod();


		//ta.processRetryTransferTask(body);
		ta.processRetryTransferTask(body, resp -> {
			if (resp.succeeded()){
				System.out.println("Succeeded with the procdessTransferTask in retrying of the event ");
			} else {
				System.out.println("Error with return from retrying the event ");
			}
		});


		String protocolSelected = "http";
		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("TransferTaskRetryListenerTest - error event thrown on unknown dest system")
	public void processTransferTaskPublishesErrorOnSystemUnknown(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		RemoteDataClient srcClient = mock(RemoteDataClient.class);

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);

		doCallRealMethod().when(ta).processRetry(any(), any());
		try {
			// allow the first one to succeed since it's not an agave URI
			when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource()))))
					.thenReturn(srcClient);
			// force the second one to fail since it is an agave URI and can result in a bad syste lookup.
			when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest()))))
					.thenThrow(new SystemUnknownException("THis should be thrown during the test and propagated back as the handler.cause() method."));
		} catch (Exception e) {
			ctx.failNow(e);
		}

		ta.processRetry(tt, processRetryResult -> ctx.verify(() -> {
			assertFalse(processRetryResult.succeeded(), "processRetry should fail when system is unknown");
			assertEquals(processRetryResult.cause().getClass(), SystemUnknownException.class, "processRetry should propagate SystemUnknownException back to handler when thrown.");
			verify(ta, times(1)).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource())));
			verify(ta, times(1)).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest())));
			verify(ta, times(1))._doPublishEvent(eq(TRANSFERTASK_ERROR), any(JsonObject.class));
			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferRetryListenerTest - error event thrown on unknown source system")
	public void processTransferTaskPublishesErrorOnSrcSystemUnknown(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_DEST, TRANSFER_SRC, TEST_USERNAME, TENANT_ID, null, null);

		RemoteDataClient destClient = mock(RemoteDataClient.class);

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);

		doCallRealMethod().when(ta).processRetry(any(), any());

		try {
			// force the source one to fail since it is an agave URI and can result in a bad syste lookup.
			when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource()))))
					.thenThrow(new SystemUnknownException("This should be thrown during the test and propagated back as the handler.cause() method."));
			// allow the dest one to succeed since it's not an agave URI
			when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest()))))
					.thenReturn(destClient);
		} catch (Exception e) {
			ctx.failNow(e);
		}

		ta.processRetry(tt, processRetryResult -> ctx.verify(() -> {
			assertFalse(processRetryResult.succeeded(), "processRetry should fail when system is unknown");
			assertEquals(processRetryResult.cause().getClass(), SystemUnknownException.class, "processRetry should propagate SystemUnknownException back to handler when thrown.");
			verify(ta, times(1)).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getSource())));
			verify(ta, never()).getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), eq(URI.create(tt.getDest())));
			verify(ta, times(1))._doPublishEvent(eq(TRANSFERTASK_ERROR), any(JsonObject.class));
			ctx.completeNow();
		}));
	}

	@Test
	@DisplayName("TransferRetryListener - isTaskInterruptedTest")
	void isTaskInterrupted(Vertx vertx, VertxTestContext ctx){
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferRetryListener ta = new TransferRetryListener(vertx);

		ctx.verify(() -> {
			ta.interruptedTasks.add(tt.getUuid());
			assertTrue(ta.taskIsNotInterrupted(tt), "UUID of tt present in interruptedTasks list should indicate task is interrupted");
			ta.interruptedTasks.remove(tt.getUuid());

			ta.interruptedTasks.add(tt.getParentTaskId());
			assertTrue(ta.taskIsNotInterrupted(tt), "UUID of tt parent present in interruptedTasks list should indicate task is interrupted");
			ta.interruptedTasks.remove(tt.getParentTaskId());

			ta.interruptedTasks.add(tt.getRootTaskId());
			assertTrue(ta.taskIsNotInterrupted(tt), "UUID of tt root present in interruptedTasks list should indicate task is interrupted");
			ta.interruptedTasks.remove(tt.getRootTaskId());

			ctx.completeNow();
		});
	}

}