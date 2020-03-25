package org.agaveplatform.service.transfers.listener;

import org.agaveplatform.service.transfers.listener.TransferTaskAssignedListener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testng.Assert;

import javax.validation.constraints.AssertFalse;

import static org.junit.jupiter.api.Assertions.*;


@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@ExtendWith(VertxExtension.class)
@DisplayName("👋 TransferTaskAssignedListener test")
class TransferTaskAssignedListenerTest {

	private EventBus eventBus;

//	@Test
//	void taskAssigned(Vertx vertx, VertxTestContext ctx){
//		eventBus = vertx.eventBus();
//		vertx.deployVerticle(new TransferTaskAssignedListener(), ctx.succeeding(id -> {
//			JsonObject body = new JsonObject();
//			body.put("id", "1");  // uuid
//			body.put("owner", "dooley");
//			body.put("tenantId", "agave.dev");
//			body.put("protocol","sftp");
//
//			eventBus.consumer("transfertask.created", msg -> {
//				JsonObject bodyRec = (JsonObject) msg.body();
//				assertEquals("1", bodyRec.getString("id"));
//				assertEquals("dooley", bodyRec.getString("owner"));
//				assertEquals("agave.dev", bodyRec.getString("tenantId"));
//				assertEquals("sftp", bodyRec.getString("protocol"));
//
//			});
//
//			eventBus.publish("transfertask.assigned.agave.dev.sftp.*.*", body );
//
//		}));
//	}

	@Test
	public void processTransferTask(Vertx vertx, VertxTestContext ctx) {
		JsonObject body = new JsonObject();
		body.put("id", "1");  // uuid
		body.put("owner", "dooley");
		body.put("tenantId", "agave.dev");
		body.put("protocol","sftp");
		body.put("source", "");

		vertx.eventBus().consumer("transfertask.created", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals("1", bodyRec.getString("id"));
		});

		vertx.eventBus().consumer("transfertask.cancel.ack", msg -> {
			JsonObject bodyRec = (JsonObject) msg.body();
			assertEquals("1", bodyRec.getString("id"));
		});

		TransferTaskAssignedListener ta = new TransferTaskAssignedListener();
		try {
			String ret = ta.processTransferTask(body);
		} catch (Exception e){
			fail(e.toString());
		}
	}

	@Test
	void isTaskInterrupted(){
		TransferTask tt = new TransferTask();
		tt.setId(1L); // uuid
		TransferTaskAssignedListener ta = new TransferTaskAssignedListener();
		boolean result = ta.isTaskInterrupted(tt);
		assertFalse(result);

	}

}