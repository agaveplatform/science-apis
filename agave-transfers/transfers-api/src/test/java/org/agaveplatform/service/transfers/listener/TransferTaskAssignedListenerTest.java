package org.agaveplatform.service.transfers.listener;

import io.vertx.core.DeploymentOptions;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.agaveplatform.service.transfers.listener.TransferTaskAssignedListener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.protocol.TransferSftpVertical;
import org.agaveplatform.service.transfers.resources.FileTransferCreateServiceImpl;
import org.agaveplatform.service.transfers.resources.TransferServiceUIVertical;
import org.agaveplatform.service.transfers.resources.TransferServiceVerticalTest;
import org.agaveplatform.service.transfers.resources.TransferTaskUnaryImpl;
import org.agaveplatform.service.transfers.streaming.StreamingFileTaskImpl;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.junit.jupiter.api.Assertions.*;


//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("👋 TransferTaskAssignedListener test")
//@Disabled
class TransferTaskAssignedListenerTest {

	private EventBus eventBus;
	private TransferTaskDatabaseService service;

	@BeforeAll
	public void prepare(Vertx vertx, VertxTestContext ctx) throws InterruptedException, IOException {
		Path configPath = Paths.get(TransferServiceVerticalTest.class.getClassLoader().getResource("config.json").getPath());
		String json = new String(Files.readAllBytes(configPath));
		JsonObject conf = new JsonObject(json);

		vertx.deployVerticle(new TransferTaskDatabaseVerticle(),
				new DeploymentOptions().setConfig(conf).setWorker(true).setMaxWorkerExecuteTime(3600),
				ctx.succeeding(id -> {
					service = TransferTaskDatabaseService.createProxy(vertx, conf.getString(CONFIG_TRANSFERTASK_DB_QUEUE));
					ctx.completeNow();
				}));

		vertx.deployVerticle(new TransferTaskCreatedListener(vertx,"transfertask.created"));
		vertx.deployVerticle(new TransferTaskAssignedListener(vertx));
		vertx.deployVerticle(new TransferTaskCancelListener(vertx));
		vertx.deployVerticle(new TransferSftpVertical(vertx));
		vertx.deployVerticle(new TransferCompleteTaskListenerImpl(vertx));
		vertx.deployVerticle(new ErrorTaskListener());
		vertx.deployVerticle(new InteruptEventListener(vertx));
		vertx.deployVerticle(new NotificationListener());
		vertx.deployVerticle(new TransferTaskPausedListener(vertx));
		vertx.deployVerticle(new FileTransferCreateServiceImpl(vertx));
		vertx.deployVerticle(new TransferServiceUIVertical());
		vertx.deployVerticle(new TransferTaskUnaryImpl(vertx));
		vertx.deployVerticle(new StreamingFileTaskImpl(vertx));

	}

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}
	// end::finish[]


	@Test
	public void processTransferTask(Vertx vertx, VertxTestContext ctx) {
		//JsonObject body = new JsonObject();
		TransferTask tt = new TransferTask();
		tt.setUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setOwner("dooley");
		tt.setTenantId("agave.dev");
		tt.setProtocol("sftp");
		tt.setSource("");

		JsonObject body = new JsonObject(tt.toJSON());
		body.put("uuid", new AgaveUUID(UUIDType.TRANSFER).toString());  // uuid
		body.put("owner", "dooley");
		body.put("tenantId", "agave.dev");
		body.put("protocol","sftp");
		body.put("source", "");

		vertx.eventBus().consumer("transfertask.created", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals("1", bodyRec.getString("id"));
			ctx.completeNow();
		});

//		vertx.eventBus().consumer("transfertask.cancel.ack", msg -> {
//			JsonObject bodyRec = (JsonObject) msg.body();
//			assertEquals("1", bodyRec.getString("uuid"));
//		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
		try {
			String ret = ta.processTransferTask(body);
			System.out.println(ret);
		} catch (Exception e){
			fail();
		}
	}

	@Test
	void isTaskInterrupted(){
		TransferTask tt = new TransferTask();
		tt.setUuid(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setOwner("dooley");
		tt.setTenantId("agave.dev");
		tt.setProtocol("sftp");
		tt.setSource("");

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(Vertx.vertx(), "transfertask.assigned");
		boolean result = ta.isTaskInterrupted(tt);
		assertTrue(result);
	}

}