package org.agaveplatform.service.transfers.listener;

import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.junit5.*;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.resources.TransferServiceVerticalTest;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers processPausedRequest tests")
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
class TransferTaskPausedListenerTest {

	protected Vertx vertx;
	private TransferTaskDatabaseService service;

	@BeforeAll
	public void prepare(Vertx vertx, VertxTestContext ctx) throws InterruptedException, IOException {
		Path configPath = Paths.get(TransferServiceVerticalTest.class.getClassLoader().getResource("config.json").getPath());
		String json = new String(Files.readAllBytes(configPath));
		JsonObject conf = new JsonObject(json);

//		vertx.deployVerticle(new TransferTaskDatabaseVerticle(),
//				new DeploymentOptions().setConfig(conf).setWorker(true).setMaxWorkerExecuteTime(3600),
//				ctx.succeeding(id -> {
//					service = TransferTaskDatabaseService.createProxy(vertx, conf.getString(CONFIG_TRANSFERTASK_DB_QUEUE));
//					ctx.completeNow();
//				}));
		vertx.deployVerticle(new TransferTaskPausedListener(vertx));
		vertx.deployVerticle(new TransferTaskCancelListener(vertx));
	}

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	public void processCancelRequest() {
		String uuid = new AgaveUUID(UUIDType.TRANSFER).toString();
		JsonObject body = new JsonObject();
		body.put("uuid", uuid);  // uuid
		body.put("owner", "dooley");
		body.put("tenantId", "agave.dev");
		body.put("protocol","sftp");
		body.put("source", "");

		TransferTaskPausedListener tpl = new TransferTaskPausedListener(vertx);
		vertx.eventBus().consumer("transfertask.cancel.sync", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(uuid, bodyRec.getString("uuid"));
		});

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			fail(bodyRec.getString("cause"));
		});

		tpl.processCancelRequest(body);
	}


}