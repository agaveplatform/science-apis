package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_NOTIFICATION;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(VertxExtension.class)
@DisplayName("Notification listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({ JDBCClient.class })
//@Disabled
class TransferTaskTransferTaskNotificationListenerTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskTransferTaskNotificationListenerTest.class);

	protected TransferTaskNotificationListener getMockNotificationListenerInstance(Vertx vertx) {
		TransferTaskNotificationListener ttc = mock(TransferTaskNotificationListener.class );
		when(ttc.getEventChannel()).thenReturn(TRANSFERTASK_NOTIFICATION);
		when(ttc.getVertx()).thenReturn(vertx);

		return ttc;
	}

	@Test
	@DisplayName("notificationEventProcess Test")
	void notificationEventProcess(Vertx vertx, VertxTestContext ctx) {
		log.info("Starting process of notificationEventProcess.");
		JsonObject body = new JsonObject(_createTestTransferTask().toJSON());
		body.put("id", new AgaveUUID(UUIDType.TRANSFER).toString());

		TransferTaskNotificationListener txfrTransferTaskNotificationListener = getMockNotificationListenerInstance(vertx);
		when(txfrTransferTaskNotificationListener.notificationEventProcess(any())).thenReturn(true);

		boolean result = txfrTransferTaskNotificationListener.notificationEventProcess(body);

		assertTrue(result, "notificationEventProcess should return true when the notificationEventProcess returned");
		ctx.completeNow();
	}
}