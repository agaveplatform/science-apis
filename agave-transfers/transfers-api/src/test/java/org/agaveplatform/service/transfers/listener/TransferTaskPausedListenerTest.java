package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.junit5.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers processPausedRequest tests")
//@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Disabled
class TransferTaskPausedListenerTest {

	Vertx vertx;

	@Test
	public void processCancelRequest() {
		JsonObject body = new JsonObject();
		body.put("id", "1");  // uuid
		body.put("owner", "dooley");
		body.put("tenantId", "agave.dev");
		body.put("protocol","sftp");
		body.put("source", "");

		TransferTaskPausedListener tpl = new TransferTaskPausedListener(vertx);
		vertx.eventBus().consumer("transfertask.cancel.sync", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals("1", bodyRec.getString("id"));
		});

		vertx.eventBus().consumer("transfertask.error", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			fail(bodyRec.getString("cause"));
		});

		tpl.processCancelRequest(body);
	}

	@Test
	public void getTransferTaskTree() {
	}

	@Test
	public void getTransferTask() {
	}
}