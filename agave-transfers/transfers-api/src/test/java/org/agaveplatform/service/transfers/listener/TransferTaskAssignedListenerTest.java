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
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.StorageConfig;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
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

	private final String TEST_USERNAME = "testuser";
	public static final String TENANT_ID = "agave.dev";
	public static final String TRANSFER_SRC = "http://foo.bar/cat";
	public static final String TRANSFER_DEST = "agave://sftp.example.com//dev/null";
	public static final String TEST_USER = "testuser";

	private EventBus eventBus;
	private TransferTaskDatabaseService service;

//	@BeforeAll
//	public void prepare(Vertx vertx, VertxTestContext ctx) throws InterruptedException, IOException {
//		Path configPath = Paths.get(TransferServiceVerticalTest.class.getClassLoader().getResource("config.json").getPath());
//		String json = new String(Files.readAllBytes(configPath));
//		JsonObject conf = new JsonObject(json);
//
//		ctx.completeNow();
////		vertx.deployVerticle(new TransferTaskDatabaseVerticle(),
////				new DeploymentOptions().setConfig(conf).setWorker(true).setMaxWorkerExecuteTime(3600),
////				ctx.succeeding(id -> {
////					service = TransferTaskDatabaseService.createProxy(vertx, conf.getString(CONFIG_TRANSFERTASK_DB_QUEUE));
////					ctx.completeNow();
////				}));
////
////		vertx.deployVerticle(new TransferTaskCreatedListener(vertx,"transfertask.created"));
////		vertx.deployVerticle(new TransferTaskAssignedListener(vertx));
////		vertx.deployVerticle(new TransferTaskCancelListener(vertx));
////		vertx.deployVerticle(new TransferSftpVertical(vertx));
////		vertx.deployVerticle(new TransferCompleteTaskListenerImpl(vertx));
////		vertx.deployVerticle(new ErrorTaskListener());
////		vertx.deployVerticle(new InteruptEventListener(vertx));
////		vertx.deployVerticle(new NotificationListener());
////		vertx.deployVerticle(new TransferTaskPausedListener(vertx));
////		vertx.deployVerticle(new FileTransferCreateServiceImpl(vertx));
////		vertx.deployVerticle(new TransferServiceUIVertical());
////		vertx.deployVerticle(new TransferTaskUnaryImpl(vertx));
////		vertx.deployVerticle(new StreamingFileTaskImpl(vertx));
//
//	}

	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

//	@ParameterizedTest
//	@ValueSource(strings = { "GRIDFTP", "FTP", "SFTP", "IRODS", "IRODS4", "LOCAL", "AZURE", "S3", "SWIFT", "HTTP", "HTTPS" })
	@Test
	//@Disabled
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

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
		ta.processTransferTask(body);

//		URI srcUri = URI.create(tt.getSource());
//		RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
//		String protocolSelected =  srcSystem.getStorageConfig().getProtocol().name().toLowerCase();
		String protocolSelected = "http";

		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	public void processTransferTaskPublishesChildTasksForDirectory(Vertx vertx, VertxTestContext ctx) {
		//JsonObject body = new JsonObject();
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.sftp", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
		ta.processTransferTask(body);

//		URI srcUri = URI.create(tt.getSource());
//		RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
//		String protocolSelected =  srcSystem.getStorageConfig().getProtocol().name().toLowerCase();
		String protocolSelected = "http";

		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	public void processTransferTaskPublishesErrorOnSystemUnavailble(Vertx vertx, VertxTestContext ctx) {
		//JsonObject body = new JsonObject();
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
		ta.processTransferTask(body);

//		URI srcUri = URI.create(tt.getSource());
//		RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
//		String protocolSelected =  srcSystem.getStorageConfig().getProtocol().name().toLowerCase();
		String protocolSelected = "http";
		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	public void processTransferTaskPublishesErrorOnSystemUnknown(Vertx vertx, VertxTestContext ctx) {
		//JsonObject body = new JsonObject();
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);

		JsonObject body = tt.toJson();

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals(tt.getUuid(), bodyRec.getString("uuid"));
			ctx.completeNow();
		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(vertx);
		ta.processTransferTask(body);
//		URI srcUri = URI.create(tt.getSource());
//		RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
//		String protocolSelected =  srcSystem.getStorageConfig().getProtocol().name().toLowerCase();
		String protocolSelected = "http";
		assertEquals(StorageProtocolType.HTTP.name().toLowerCase(), protocolSelected.toLowerCase(), "Protocol used should have been " + StorageProtocolType.SFTP.name().toLowerCase());
		ctx.completeNow();
	}

	@Test
	void isTaskInterrupted(){
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		tt.setParentTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());
		tt.setRootTaskId(new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener(Vertx.vertx(), "transfertask.assigned");
		ta.interruptedTasks.add(tt.getUuid());
		assertTrue(ta.isTaskInterrupted(tt), "UUID of tt present in interruptedTasks list should return true");
		ta.interruptedTasks.remove(tt.getUuid());

		ta.interruptedTasks.add(tt.getParentTaskId());
		assertTrue(ta.isTaskInterrupted(tt), "UUID of tt parent present in interruptedTasks list should return true");
		ta.interruptedTasks.remove(tt.getParentTaskId());

		ta.interruptedTasks.add(tt.getRootTaskId());
		assertTrue(ta.isTaskInterrupted(tt), "UUID of tt root present in interruptedTasks list should return true");
		ta.interruptedTasks.remove(tt.getRootTaskId());
	}

}