package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TransferCompleteTaskListenerImplTest {

	Vertx vertx;
	@Test
	void start() {
		String address = "*.transfer.complete";
		TransferCompleteTaskListenerImpl ttl = new TransferCompleteTaskListenerImpl(vertx, address);


		vertx.eventBus().consumer("transfertask.nofication", msg -> {
			JsonObject body = (JsonObject) msg.body();
			body.put("id", "1");  // uuid
			String uuid = body.getString("id");

		});
		ttl.start();
	}
}