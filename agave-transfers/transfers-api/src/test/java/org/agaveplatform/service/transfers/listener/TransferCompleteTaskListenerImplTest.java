package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.junit.jupiter.api.Assertions.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("Transfers Task transfertask.nofication test")
class TransferCompleteTaskListenerImplTest {

	Vertx vertx;
	@Test
	public void start() {
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

