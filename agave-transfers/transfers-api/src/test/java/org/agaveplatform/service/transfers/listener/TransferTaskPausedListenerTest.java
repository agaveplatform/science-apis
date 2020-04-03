package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers processPausedRequest tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
class TransferTaskPausedListenerTest {

	private final String TEST_USERNAME = "testuser";
	public static final String TENANT_ID = "agave.dev";
	public static final String TRANSFER_SRC = "http://foo.bar/cat";
	public static final String TRANSFER_DEST = "agave://sftp.example.com//dev/null";

	protected Vertx vertx;
	private TransferTaskDatabaseService service;

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

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	@DisplayName("Transfer Task Puased Listener - processCancelRequest")
	public void processPauseRequest(Vertx vertx, VertxTestContext ctx) {

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		JsonObject body = tt.toJson();

		TransferTaskPausedListener tpl = new TransferTaskPausedListener(vertx,"transfertask.paused");

		vertx.eventBus().consumer("transfertask.paused.sync", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"), "uuid should match and didn't");
			ctx.completeNow();
		});

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyErr = (JsonObject) msg.body();
			fail(bodyErr.getString("cause"));
			ctx.failNow(new Exception(bodyErr.getString("message")));
		});

		tpl.processPauseRequest(body);
	}


}