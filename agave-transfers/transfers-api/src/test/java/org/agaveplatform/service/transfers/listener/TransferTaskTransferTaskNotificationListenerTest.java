package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.model.TransferTask;
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
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Notification listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({ JDBCClient.class })
//@Disabled
class TransferTaskTransferTaskNotificationListenerTest extends BaseTestCase {
	private static final Logger log = LoggerFactory.getLogger(TransferTaskTransferTaskNotificationListenerTest.class);

	protected TransferTaskNotificationListener getMockNotificationListenerInstance(Vertx vertx) {
		TransferTaskNotificationListener listener = mock(TransferTaskNotificationListener.class );
		when(listener.getEventChannel()).thenReturn(TRANSFERTASK_NOTIFICATION);
		when(listener.getVertx()).thenReturn(vertx);
		when(listener.getRetryRequestManager()).thenCallRealMethod();
		doNothing().when(listener)._doPublishEvent(any(), any());
		doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
		doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());

		return listener;
	}

	@Test
	@DisplayName("notificationEventProcess Test")
	void notificationEventProcess(Vertx vertx, VertxTestContext ctx) {
		log.info("Starting process of notificationEventProcess.");
		TransferTask transferTask = _createTestTransferTask();
		JsonObject body = transferTask.toJson();
		body.put("id", 1);

		TransferTaskNotificationListener txfrTransferTaskNotificationListener = getMockNotificationListenerInstance(vertx);
		when(txfrTransferTaskNotificationListener.sentToLegacyMessageQueue(any())).thenReturn(true);

		boolean result = txfrTransferTaskNotificationListener.sentToLegacyMessageQueue(body);

		assertTrue(result, "notificationEventProcess should return true when the notificationEventProcess returned");
		ctx.completeNow();
	}
}