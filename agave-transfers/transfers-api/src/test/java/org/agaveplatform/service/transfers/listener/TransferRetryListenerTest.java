package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.stubbing.Answer;

import java.io.FileNotFoundException;
import java.net.URI;
import java.time.Instant;

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

	private EventBus eventBus;
	private TransferTaskDatabaseService service;

	protected TransferRetryListener getMockTransferRetryListenerInstance(Vertx vertx) {
		TransferRetryListener ttc = mock(TransferRetryListener.class );
		when(ttc.getEventChannel()).thenReturn(TRANSFER_RETRY);
		when(ttc.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(ttc).processRetryTransferTask(any(), any());
		when(ttc.taskIsNotInterrupted(any())).thenCallRealMethod();
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
	@DisplayName("Process processTransferTaskPublishesChildTasksForDirectory")
	@Disabled
	public void processTransferTaskPublishesChildTasksForDirectory(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.sftp", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);
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
	@DisplayName("Process processTransferTaskPublishesErrorOnSystemUnknown")
	//@Disabled
	public void processTransferTaskPublishesErrorOnSystemUnknown(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);
		doCallRealMethod().when(ta).processRetry( any(), any() );
		try {
			//when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), URI.create(tt.getSource()))).thenCallRealMethod();
			when(ta.getRemoteDataClient(eq(tt.getTenantId()), eq(tt.getOwner()), URI.create(tt.getSource())))
					.thenThrow(new SystemUnknownException("THis should be thrown during the test and propagated back as the handler.cause() method."));
		} catch (Exception e) {
			ctx.failNow(e);
		}

		ta.processRetry(tt, processRetryResult -> {
			ctx.verify(() -> {
				assertFalse(processRetryResult.succeeded(), "processRetry should fail when system is unknown");
				assertEquals(processRetryResult.cause().getClass(), SystemUnknownException.class, "processRetry should propagate SystemUnknownException back to handler when thrown.");
				ctx.completeNow();
			});
		});
//		TransferTaskDatabaseService dbService = mock(TransferTaskDatabaseService.class);
//
//		// mock a successful outcome with updated json transfer task result from getById
//		JsonObject expectedgetByIdAck = tt.toJson();
//		AsyncResult<JsonObject> updateGetById = getMockAsyncResult(expectedgetByIdAck);
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(2, Handler.class))
//					.handle(updateGetById);
//			return null;
//		}).when(dbService).getById(eq(tt.getTenantId()), eq(tt.getUuid()), anyObject() );
//
//		// mock a successful outcome with updated json transfer task result from updateStatus
//		JsonObject expectedUdpatedJsonObject = tt.toJson()
//				.put("status", TransferStatusType.COMPLETED.name())
//				.put("endTime", Instant.now());
//		AsyncResult<JsonObject> updateStatusHandler = getMockAsyncResult(expectedUdpatedJsonObject);
//		// mock the handler passed into updateStatus
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
//					.handle(updateStatusHandler);
//			return null;
//		}).when(dbService).updateStatus(any(), any(), any(), any());
//
//
//		// mock a successful outcome with updated json transfer task result from updateStatus
//		AsyncResult<JsonObject> updateHandler = getMockAsyncResult(expectedUdpatedJsonObject);
//		// mock the handler passed into updateStatus
//		doAnswer((Answer<AsyncResult<JsonObject>>) arguments -> {
//			((Handler<AsyncResult<JsonObject>>) arguments.getArgumentAt(3, Handler.class))
//					.handle(updateHandler);
//			return null;
//		}).when(dbService).update(any(), any(), any(), any());


	}

	@Test
	@DisplayName("Process isTaskInterrupted")
	@Disabled
	void isTaskInterrupted(Vertx vertx, VertxTestContext ctx){
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferRetryListener ta = new TransferRetryListener(Vertx.vertx(), "transfertask.assigned");
		ta.processInterrupt("add", tt.toJson());
		//ta.interruptedTasks.add(tt.getUuid());

		assertFalse(ta.taskIsNotInterrupted(tt), "UUID of tt present in interruptedTasks list should return true");

		ta.processInterrupt("remove", tt.toJson());
		//ta.interruptedTasks.remove(tt.getUuid());

		//ta.interruptedTasks.add(tt.getParentTaskId());
		ta.processInterrupt("add", tt.toJson());

		assertTrue(ta.taskIsNotInterrupted(tt), "UUID of tt parent present in interruptedTasks list should return true");

		//ta.interruptedTasks.remove(tt.getParentTaskId());
		ta.processInterrupt("remove", tt.toJson());

		//ta.interruptedTasks.add(tt.getRootTaskId());
		ta.processInterrupt("add", tt.toJson());
		assertFalse(ta.taskIsNotInterrupted(tt), "UUID of tt root present in interruptedTasks list should return true");

		//ta.interruptedTasks.remove(tt.getRootTaskId());
		ta.processInterrupt("remove", tt.toJson());

		ctx.completeNow();
	}

}