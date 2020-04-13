package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers processPausedRequest tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
class TransferTaskPausedListenerTest extends BaseTestCase {


//	@BeforeAll
//	public void prepare(Vertx vertx, VertxTestContext ctx) throws InterruptedException, IOException {
//		Path configPath = Paths.get(TransferServiceVerticalTest.class.getClassLoader().getResource("config.json").getPath());
//		String json = new String(Files.readAllBytes(configPath));
//		JsonObject conf = new JsonObject(json);
//
////		vertx.deployVerticle(new TransferTaskDatabaseVerticle(),
////				new DeploymentOptions().setConfig(conf).setWorker(true).setMaxWorkerExecuteTime(3600),
////				ctx.succeeding(id -> {
////					service = TransferTaskDatabaseService.createProxy(vertx, conf.getString(CONFIG_TRANSFERTASK_DB_QUEUE));
////					ctx.completeNow();
////				}));
//		vertx.deployVerticle(new TransferTaskPausedListener(vertx));
//		vertx.deployVerticle(new TransferTaskCancelListener(vertx));
//	}

	TransferTaskPausedListener getMockListenerInstance(Vertx vertx) {
		TransferTaskPausedListener listener = Mockito.mock(TransferTaskPausedListener.class);
		when(listener.getEventChannel()).thenReturn(MessageType.TRANSFERTASK_PAUSED);
		when(listener.getVertx()).thenReturn(vertx);
		doCallRealMethod().when(listener).processPauseRequest(any());

		return listener;
	}

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	@DisplayName("Transfer Task Paused Listener - processCancelRequest")
	public void processPauseRequest(Vertx vertx, VertxTestContext ctx) {

		// Set up our transfertask for testing
		TransferTask parentTask = _createTestTransferTask();
		parentTask.setStatus(TransferStatusType.COMPLETED);
		parentTask.setStartTime(Instant.now());
		parentTask.setEndTime(Instant.now());
		parentTask.setSource(TRANSFER_SRC.substring(0, TRANSFER_SRC.lastIndexOf("/") - 1));

		TransferTask transferTask = _createTestTransferTask();
		transferTask.setStatus(TransferStatusType.TRANSFERRING);
		transferTask.setStartTime(Instant.now());
		transferTask.setEndTime(Instant.now());
		transferTask.setRootTaskId(parentTask.getUuid());
		transferTask.setParentTaskId(parentTask.getUuid());

		TransferTaskPausedListener listener = getMockListenerInstance(vertx);
		when(listener.getTransferTask(transferTask.getUuid())).thenReturn(transferTask);

//
//
//		vertx.eventBus().consumer("transfertask.paused.sync", msg -> {
//			JsonObject bodyRec = (JsonObject) msg.body();
//			assertEquals(tt.getUuid(), bodyRec.getString("uuid"), "uuid should match and didn't");
//			ctx.completeNow();
//		});
//
//		vertx.eventBus().consumer("transfertask.error", msg -> {
//			JsonObject bodyErr = (JsonObject) msg.body();
//			fail(bodyErr.getString("cause"));
//			ctx.failNow(new Exception(bodyErr.getString("message")));
//		});

		listener.processPauseRequest(transferTask.toJson());
		Mockito.verify(listener)._doPublishEvent(MessageType.TRANSFERTASK_PAUSED_SYNC, transferTask.toJson());
		ctx.completeNow();
	}


}