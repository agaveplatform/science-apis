package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import static org.junit.jupiter.api.Assertions.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Task Canceled test")
//@Disabled
class TransferTaskCancelListenerTest {

	private EventBus eventBus;

	@Test
	public void taskAssigned(Vertx vertx, VertxTestContext ctx){
		eventBus = vertx.eventBus();

		vertx.deployVerticle(new TransferTaskAssignedListener(), ctx.succeeding(id -> {
			JsonObject body = new JsonObject();
			body.put("id", "1");  // uuid
			body.put("owner", "dooley");
			body.put("tenantId", "agave.dev");
			body.put("protocol", "sftp");

			eventBus.consumer("transfertask.cancel.ack", msg -> {
				JsonObject bodyRec = (JsonObject) msg.body();
				assertEquals("1", bodyRec.getString("id"));
				assertEquals("dooley", bodyRec.getString("owner"));
				assertEquals("agave.dev", bodyRec.getString("tenantId"));
				assertEquals("sftp", bodyRec.getString("protocol"));
			});

			eventBus.consumer("transfertask.cancel.sync", msg -> {
				JsonObject bodySync = (JsonObject) msg.body();
				String uuid = bodySync.getString("id");
				assertEquals("1", bodySync.getString("id"));
			});

			eventBus.consumer("transfertask.cancel.ack", msg -> {
				JsonObject bodyAck = (JsonObject) msg.body();
				String uuid = bodyAck.getString("id");
				assertEquals("1", bodyAck.getString("id"));
			});

			eventBus.publish("transfertask.cancelled", body);
		}));

	}
}