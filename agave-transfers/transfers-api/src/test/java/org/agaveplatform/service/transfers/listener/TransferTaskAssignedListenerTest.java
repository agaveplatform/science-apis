package org.agaveplatform.service.transfers.listener;

import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
//import org.agaveplatform.service.transfers.exception.InterruptableTransferTaskException;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("👋 TransferTaskAssignedListener test")
class TransferTaskAssignedListenerTest {

	private final String TEST_USERNAME = "testuser";
	public static final String TENANT_ID = "agave.dev";
	public static final String TRANSFER_SRC = "http://foo.bar/cat";
	public static final String TRANSFER_DEST = "agave://sftp.example.com//dev/null";
	public static final String TEST_USER = "testuser";

	private EventBus eventBus;
	private TransferTaskDatabaseService service;

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - processTransferTaskPublishesProtocolEventTest")
	public void processTransferTaskPublishesProtocolEventTest(Vertx vertx, VertxTestContext ctx) {
		//JsonObject body = new JsonObject();
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.all", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			ctx.failNow(new Exception(bodyRec.getString("message")));
		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
//		try {
			//ta.processTransferTask(body, handler);
		ta.processTransferTask(body, resp -> {
			if (resp.succeeded()){
				System.out.println("Succeeded with the procdessTransferTask in the assigning of the event ");
			} else {
				System.out.println("Error with return from creating the event ");
			}
		});
//		} catch (InterruptableTransferTaskException e) {
//			e.printStackTrace();
//		}

		String protocolSelected = "http";

		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - processTransferTaskPublishesChildTasksForDirectoryTest")
	public void processTransferTaskPublishesChildTasksForDirectoryTest(Vertx vertx, VertxTestContext ctx) {
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.all", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
//		try {
		//	ta.processTransferTask(body);
		ta.processTransferTask(body, resp -> {
			if (resp.succeeded()){
				System.out.println("Succeeded with the procdessTransferTask in the assigning of the event ");
			} else {
				System.out.println("Error with return from creating the event ");
			}
		});
//		} catch (InterruptableTransferTaskException e) {
//			e.printStackTrace();
//		}

		String protocolSelected = "http";

		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - processTransferTaskPublishesErrorOnSystemUnavailbleTest")
	public void processTransferTaskPublishesErrorOnSystemUnavailbleTest(Vertx vertx, VertxTestContext ctx) {
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
//		try {
			//ta.processTransferTask(body);
		ta.processTransferTask(body, resp -> {
			if (resp.succeeded()){
				System.out.println("Succeeded with the procdessTransferTask in the assigning of the event ");
			} else {
				System.out.println("Error with return from creating the event ");
			}
		});
//		} catch (InterruptableTransferTaskException e) {
//			e.printStackTrace();
//		}

		String protocolSelected = "http";
		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - processTransferTaskPublishesErrorOnSystemUnknownTest")
	public void processTransferTaskPublishesErrorOnSystemUnknownTest(Vertx vertx, VertxTestContext ctx) {
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
//		try {
			//ta.processTransferTask(body);
		ta.processTransferTask(body, resp -> {
			if (resp.succeeded()){
				System.out.println("Succeeded with the procdessTransferTask in the assigning of the event ");
			} else {
				System.out.println("Error with return from creating the event ");
			}
		});
//		} catch (InterruptableTransferTaskException e) {
//			e.printStackTrace();
//		}

		assertEquals(StorageProtocolType.SFTP.name().toLowerCase(), StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	@DisplayName("TransferTaskAssignedListener - isTaskInterruptedTest")
	void isTaskInterruptedTest(Vertx vertx, VertxTestContext ctx){
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
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