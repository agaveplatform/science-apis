package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.rxjava.ext.unit.Async;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.StorageProtocolType;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;

import java.net.URI;

import static io.vertx.core.Vertx.vertx;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Transfers assignTransferTask tests")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
//@Disabled
class TransferTaskCreatedListenerTest {

	private final String TEST_USERNAME = "testuser";
	public static final String TENANT_ID = "agave.dev";
	public static final String TRANSFER_SRC = "http://foo.bar/cat";
	public static final String TRANSFER_DEST = "agave://sftp.example.com//dev/null";
	public static final String HOST = "foo.bar";
	public static final String PROTOCOL = "http";
	Vertx vertx;


	@AfterAll
	public void finish(Vertx vertx, VertxTestContext ctx) {
		vertx.close(ctx.completing());
	}

	@Test
	@DisplayName("Transfer Task Created Listener - assignTransferTask")
	public void assignTransferTask(Vertx vertx, VertxTestContext ctx) {
		TransferTask tt = new TransferTask(TRANSFER_SRC, TRANSFER_DEST, TEST_USERNAME, TENANT_ID, null, null);
		JsonObject body = tt.toJson().put("tenantId", tt.getTenantId());

		TransferTaskCreatedListener ttc = Mockito.mock(TransferTaskCreatedListener.class);
		Mockito.when(ttc.getEventChannel()).thenReturn("transfertask.created");
		Mockito.when(ttc.getVertx()).thenReturn(vertx);
		Mockito.when(ttc.assignTransferTask(Mockito.any())).thenCallRealMethod();

		String assignmentChannel = "transfertask.assigned." +
				TENANT_ID +
				"." + PROTOCOL +
				"." + HOST +
				"." + TEST_USERNAME;

		String result = ttc.assignTransferTask(body);
		assertEquals("http", result, "result should have been http");
		verify(ttc)._doPublish(assignmentChannel, body);
		ctx.completeNow();


	}
}