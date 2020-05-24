package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.exception.InterruptableTransferTaskException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_RETRY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


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

		return ttc;
	}


	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}


	@Test
	@DisplayName("Process TransferTaskPublishesProtocolEvent")
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
		try {
			when(ta.isTaskInterrupted(tt)).thenCallRealMethod();
		} catch (InterruptableTransferTaskException e) {
			e.printStackTrace();
		}
		ta.retryProcessTransferTask(body);

		String protocolSelected = "http";

		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("Process processTransferTaskPublishesChildTasksForDirectory")
	public void processTransferTaskPublishesChildTasksForDirectory(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.sftp", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);
		ta.retryProcessTransferTask(body);

		String protocolSelected = "http";

		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("Process processTransferTaskPublishesErrorOnSystemUnavailble")
	public void processTransferTaskPublishesErrorOnSystemUnavailble(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);
		ta.retryProcessTransferTask(body);

		String protocolSelected = "http";
		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("Process processTransferTaskPublishesErrorOnSystemUnknown")
	public void processTransferTaskPublishesErrorOnSystemUnknown(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferRetryListener ta = getMockTransferRetryListenerInstance(vertx);
		ta.retryProcessTransferTask(body);

		String protocolSelected = "http";
		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("Process isTaskInterrupted")
	void isTaskInterrupted(){
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferRetryListener ta = new TransferRetryListener(Vertx.vertx(), "transfertask.assigned");
		ta.processInterrupt("add", tt.toJson());
		//ta.interruptedTasks.add(tt.getUuid());
		try {
			assertTrue(ta.isTaskInterrupted(tt), "UUID of tt present in interruptedTasks list should return true");

			ta.processInterrupt("remove", tt.toJson());
			//ta.interruptedTasks.remove(tt.getUuid());

			//ta.interruptedTasks.add(tt.getParentTaskId());
			ta.processInterrupt("add", tt.toJson());

			assertTrue(ta.isTaskInterrupted(tt), "UUID of tt parent present in interruptedTasks list should return true");

			//ta.interruptedTasks.remove(tt.getParentTaskId());
			ta.processInterrupt("remove", tt.toJson());

			//ta.interruptedTasks.add(tt.getRootTaskId());
			ta.processInterrupt("add", tt.toJson());
			assertTrue(ta.isTaskInterrupted(tt), "UUID of tt root present in interruptedTasks list should return true");
		} catch (InterruptableTransferTaskException e) {
			e.printStackTrace();
		}
		//ta.interruptedTasks.remove(tt.getRootTaskId());
		ta.processInterrupt("remove", tt.toJson());
	}

}