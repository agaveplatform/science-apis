package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.junit.jupiter.api.Test;

import static io.vertx.core.Vertx.vertx;
import static org.junit.jupiter.api.Assertions.*;

class TransferTaskCreatedListenerTest {

	Vertx vertx;

	@Test
	void assignTransferTask() {
		JsonObject body = new JsonObject();
		body.put("id", "1");  // uuid
		body.put("owner", "dooley");
		body.put("tenantId", "agave.dev");
		body.put("protocol","sftp");
		body.put("source", "");

		TransferTaskCreatedListener ttc = new TransferTaskCreatedListener(vertx);

		String assignmentChannel = "transfertask.assigned." +
				"agave.dev" +
				"." + "sftp" +
				"." + "" +
				"." + "dooley";
		vertx().eventBus().consumer(assignmentChannel, msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals("1", bodyRec.getString("id"));
		});

		try {
			String result = ttc.assignTransferTask(body);
			assertEquals(result, "sftp");
		} catch (Exception e) {
			fail(e.toString());
		}

	}
}