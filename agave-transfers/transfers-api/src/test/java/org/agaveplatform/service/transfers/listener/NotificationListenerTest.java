package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical;
import org.iplantc.service.common.uuid.AgaveUUID;
import org.iplantc.service.common.uuid.UUIDType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_COMPLETED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.*;
import static org.mockito.Mockito.never;

@ExtendWith(VertxExtension.class)
@DisplayName("Notification listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({ JDBCClient.class })
@Disabled
class NotificationListenerTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(NotificationListenerTest.class);

	protected NotificationListener getMockNotificationListenerInstance(Vertx vertx) {
		NotificationListener ttc = mock(NotificationListener.class );
		when(ttc.getEventChannel()).thenReturn(TRANSFER_COMPLETED);
		when(ttc.getVertx()).thenReturn(vertx);

		return ttc;
	}

	@Test
	@DisplayName("notificationEventProcess Test")
	void notificationEventProcess(Vertx vertx, VertxTestContext ctx) {
		log.info("Starting process of notificationEventProcess.");
		JsonObject body = new JsonObject(_createTestTransferTask().toJSON());
		body.put("id", new AgaveUUID(UUIDType.TRANSFER).toString());

		NotificationListener txfrNotificationListener = getMockNotificationListenerInstance(vertx);
		when(txfrNotificationListener.notificationEventProcess(any())).thenCallRealMethod();

		boolean result = txfrNotificationListener.notificationEventProcess(body);


		assertTrue(result, "notificationEventProcess should return true when the notificationEventProcess returned");
	}
}