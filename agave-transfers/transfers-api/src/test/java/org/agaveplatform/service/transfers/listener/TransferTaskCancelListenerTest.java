package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
//import static org.mockito.Matchers.anyString;
//import static org.mockito.ArgumentMatchers.*;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Task Canceled test")
//@Disabled
class TransferTaskCancelListenerTest {

	private EventBus eventBus;
	private final String TEST_USERNAME = "testuser";
	public static final String TENANT_ID = "agave.dev";
	public static final String TRANSFER_SRC = "http://foo.bar/cat";
	public static final String TRANSFER_DEST = "agave://sftp.example.com//dev/null";
	public static final String HOST = "foo.bar";
	public static final String PROTOCOL = "http";


	@Test
	@DisplayName("Transfer Task Cancel Listener - processCancelAck")
	public void processCancelAck(Vertx vertx, VertxTestContext ctx){
		eventBus = vertx.eventBus();

		TransferTaskCancelListener ttc = Mockito.mock(TransferTaskCancelListener.class);
		Mockito.when(ttc.getEventChannel()).thenReturn("transfertask.cancelled");
		Mockito.when(ttc.getVertx()).thenReturn(vertx);
		Mockito.when(ttc.processCancelAck(Mockito.any())).thenCallRealMethod();

		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		JsonObject body = tt.toJson();
		String uuid = body.getString("uuid");

		String result = ttc.processCancelAck(body);

//		assertEquals(uuid, result, "result should have been uuid: " + uuid);
		assertEquals(body.getString("parentTaskId"), result, "Parent result was not verified");

//		verify(ttc)._doPublish("transfertask.cancel.ack", body);
		ctx.completeNow();

//		vertx.deployVerticle(new TransferTaskAssignedListener(), ctx.succeeding(id -> {
//
//			eventBus.consumer("transfertask.cancel.ack", msg -> {
//				JsonObject bodyRec = (JsonObject) msg.body();
//				assertEquals("1", bodyRec.getString("id"));
//				assertEquals("dooley", bodyRec.getString("owner"));
//				assertEquals("agave.dev", bodyRec.getString("tenantId"));
//				assertEquals("sftp", bodyRec.getString("protocol"));
//			});
//
//			eventBus.consumer("transfertask.cancel.sync", msg -> {
//				JsonObject bodySync = (JsonObject) msg.body();
//				String uuid = bodySync.getString("id");
//				assertEquals("1", bodySync.getString("id"));
//			});
//
//			eventBus.consumer("transfertask.cancel.ack", msg -> {
//				JsonObject bodyAck = (JsonObject) msg.body();
//				String uuid = bodyAck.getString("id");
//				assertEquals("1", bodyAck.getString("id"));
//			});
//
//			eventBus.publish("transfertask.cancelled", body);
//		}));

	}
}