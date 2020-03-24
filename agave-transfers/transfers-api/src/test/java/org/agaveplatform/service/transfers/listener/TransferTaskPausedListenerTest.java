package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TransferTaskPausedListenerTest {

	Vertx vertx;

	@Test
	void processCancelRequest() {
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
	void getTransferTaskTree() {
	}

	@Test
	void getTransferTask() {
	}
}