package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.exception.InterruptableTransferTaskException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers assignTransferTask tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TransferTaskCreatedListenerTest extends BaseTestCase {

	private static final Logger log = LoggerFactory.getLogger(TransferTaskCreatedListenerTest.class);

	TransferTaskCreatedListener getMockListenerInstance(Vertx vertx) {
		TransferTaskCreatedListener ttc = Mockito.mock(TransferTaskCreatedListener.class);
		when(ttc.getEventChannel()).thenReturn(TRANSFERTASK_CREATED);
		when(ttc.getVertx()).thenReturn(vertx);

		return ttc;
	}

	@Test
	@DisplayName("Transfer Task Created Listener - assignTransferTask")
	public void assignTransferTask(Vertx vertx, VertxTestContext ctx) throws InterruptableTransferTaskException {

		// get the JsonObject to pass back and forth between verticles
		TransferTask transferTask = _createTestTransferTask();
		JsonObject json = transferTask.toJson();
		//TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		//JsonObject body = tt.toJson().put("tenantId", tt.getTenantId());
		log.info("TransferTaskCreateListenerTest");
		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);
		doNothing().when(ttc)._doPublishEvent(any(), any());
		when(ttc.assignTransferTask(eq(json))).thenCallRealMethod();

		String result = ttc.assignTransferTask(json);

		assertEquals("http", result, "result should have been http");
		verify(ttc)._doPublishEvent(TRANSFERTASK_ASSIGNED, json);

		ctx.completeNow();
	}

	@Test
	@DisplayName("Transfer Task Created Listener - assignTransferTask with no source")
	public void assignTransferTaskFailSrcTest(Vertx vertx, VertxTestContext ctx) throws InterruptableTransferTaskException {

		// get the JsonObject to pass back and forth between verticles
		TransferTask transferTask = _createTestTransferTask();
		transferTask.setSource("htt://");
		JsonObject json = transferTask.toJson();
		log.info("TransferTaskCreateListenerTest with no source");
		// mock out the verticle we're testing so we can observe that its methods were called as expected
		TransferTaskCreatedListener ttc = getMockListenerInstance(vertx);
		when(ttc.assignTransferTask(transferTask.toJson())).thenCallRealMethod();
		when(ttc.checkTaskInterrupted(transferTask)).thenCallRealMethod();

		String result = ttc.assignTransferTask(json);
		JsonObject newJsonObject = new JsonObject(result);

		assertEquals("org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException",newJsonObject.getString("cause")  , "result should have been RemoteDataSyntaxException");

		ctx.completeNow();

	}

}