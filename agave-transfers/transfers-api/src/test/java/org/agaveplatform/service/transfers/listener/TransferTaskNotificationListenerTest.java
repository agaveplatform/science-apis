package org.agaveplatform.service.transfers.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.notification.queue.messaging.NotificationMessageBody;
import org.iplantc.service.notification.queue.messaging.NotificationMessageContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.iplantc.service.common.Settings.FILES_STAGING_QUEUE;
import static org.iplantc.service.common.Settings.FILES_STAGING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(VertxExtension.class)
@DisplayName("Notification listener unit tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({JDBCClient.class})
class TransferTaskNotificationListenerTest extends BaseTestCase {
    private static final Logger log = LoggerFactory.getLogger(TransferTaskNotificationListenerTest.class);
    private static final List<String> NOTIFICATION_EVENTS = List.of(
            TRANSFERTASK_CREATED,
            TRANSFERTASK_UPDATED,
            TRANSFERTASK_FINISHED,
            TRANSFERTASK_FAILED,
            TRANSFERTASK_PAUSED_COMPLETED,
            TRANSFERTASK_CANCELED_COMPLETED);

    protected TransferTaskNotificationListener getMockNotificationListenerInstance(Vertx vertx) throws Exception {
        TransferTaskNotificationListener listener = mock(TransferTaskNotificationListener.class);
        when(listener.getEventChannel()).thenReturn(TRANSFERTASK_NOTIFICATION);
        when(listener.getVertx()).thenReturn(vertx);
        when(listener.createPushMessageSubject(any(), any(), any(), any())).thenCallRealMethod();
        doNothing().when(listener)._doPublishEvent(any(), any(), any());
        doCallRealMethod().when(listener).doHandleError(any(),any(),any(),any());
        doCallRealMethod().when(listener).doHandleFailure(any(),any(),any(),any());
        doCallRealMethod().when(listener).sentToLegacyMessageQueue(any(NotificationMessageBody.class));
        when(listener.processForNotificationMessageBody(anyString(), any(TransferTask.class))).thenCallRealMethod();

        NatsJetstreamMessageClient natsClient = mock(NatsJetstreamMessageClient.class);
        doNothing().when(natsClient).push(any(), any());
        when(listener.getMessageClient()).thenReturn(natsClient);

        return listener;
    }

    protected MessageQueueClient getMockLegacyMessageQueue() throws MessagingException {
        MessageQueueClient mockMessageQueue = mock(MessageQueueClient.class);
        doNothing().when(mockMessageQueue).push(anyString(), anyString(), anyString());

        return mockMessageQueue;
    }

    protected NotificationMessageBody getNotificationMessageBody(String uuid, String messageType, JsonObject body) throws JsonProcessingException {
        NotificationMessageContext messageBodyContext = new NotificationMessageContext(
                messageType, body.encode(), uuid);

        NotificationMessageBody messageBody = new NotificationMessageBody(
                uuid, body.getString("owner"), body.getString("tenant_id"),
                messageBodyContext);

        return messageBody;
    }

    @Test
    @DisplayName("notificationEventProcess Test")
    void notificationEventProcessTest(Vertx vertx, VertxTestContext ctx) throws Exception {
        log.info("Starting process of notificationEventProcess.");
        TransferTask transferTask = _createTestTransferTask();
        JsonObject body = transferTask.toJson();
        body.put("id", 1);

        TransferTaskNotificationListener txfrTransferTaskNotificationListener = getMockNotificationListenerInstance(vertx);

        NOTIFICATION_EVENTS.forEach(event -> {
            NotificationMessageBody notificationMessageBody = txfrTransferTaskNotificationListener.processForNotificationMessageBody(event, transferTask);
            assertNotNull(notificationMessageBody, "Valid JsonObject of TransferTask event should return corresponding JsonObject of NotificationMessageBody");
        });

        ctx.completeNow();
    }

    @Test
    @DisplayName("Process notification to legacy queue test")
    void sentToLegacyQueueTest(Vertx vertx, VertxTestContext ctx) throws Exception {
        TransferTask tt = _createTestTransferTask();
        JsonObject body = tt.toJson();
        body.put("id", 1);

        TransferTaskNotificationListener txfrTransferTaskNotificationListener = getMockNotificationListenerInstance(vertx);
        MessageQueueClient mockMessageQueue = getMockLegacyMessageQueue();
        when(txfrTransferTaskNotificationListener.getLegacyMessageClient()).thenReturn(mockMessageQueue);


        NOTIFICATION_EVENTS.forEach(event -> {
        	try {
                NotificationMessageBody notificationBody = getNotificationMessageBody(event, tt.getUuid(), body);
				txfrTransferTaskNotificationListener.sentToLegacyMessageQueue(notificationBody);
				verify(mockMessageQueue, times(1)).push(FILES_STAGING_TOPIC, FILES_STAGING_QUEUE, notificationBody.toJSON());
			} catch (Exception e) {
        		fail("Sending valid notification message to queue should not throw exception" , e);
        	}
		});
		ctx.completeNow();
    }

    @Test
    @DisplayName("Send to legacy queue should swallows exceptions")
    void processNotificationToLegacyQueueHandlesExceptionTest(Vertx vertx, VertxTestContext ctx) throws Exception {
        TransferTask tt = _createTestTransferTask();
        JsonObject body = tt.toJson();
        body.put("id", 1);

        TransferTaskNotificationListener txfrTransferTaskNotificationListener = getMockNotificationListenerInstance(vertx);

        when(txfrTransferTaskNotificationListener.getLegacyMessageClient()).thenThrow(MessagingException.class);

		NOTIFICATION_EVENTS.forEach(event -> {
			try {
                NotificationMessageBody notificationBody = getNotificationMessageBody(event, tt.getUuid(), body);
				txfrTransferTaskNotificationListener.sentToLegacyMessageQueue(notificationBody);
			} catch (Exception e) {
				fail("Exception should be swallowed", e);
			}
		});

        ctx.completeNow();
    }

}