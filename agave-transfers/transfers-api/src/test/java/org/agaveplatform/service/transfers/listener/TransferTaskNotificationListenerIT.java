package org.agaveplatform.service.transfers.listener;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.agaveplatform.service.transfers.BaseTestCase;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.BeanstalkClient;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.notification.queue.messaging.NotificationMessageBody;
import org.iplantc.service.notification.queue.messaging.NotificationMessageContext;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.powermock.core.classloader.annotations.PrepareForTest;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CREATED;
import static org.iplantc.service.common.Settings.FILES_STAGING_QUEUE;
import static org.iplantc.service.common.Settings.FILES_STAGING_TOPIC;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;


@ExtendWith(VertxExtension.class)
@DisplayName("Notification listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({JDBCClient.class})
class TransferTaskNotificationListenerIT extends BaseTestCase {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void testProcessNotificatinMessageAndSendToLegacyQueue(Vertx vertx, VertxTestContext ctx) throws MessagingException {
        TransferTask tt = _createTestTransferTask();

        Message<JsonObject> msg = mock(Message.class);
        when(msg.address()).thenReturn(MessageType.TRANSFERTASK_CREATED);
        when(msg.body()).thenReturn(tt.toJson());

        TransferTaskNotificationListener listener = mock(TransferTaskNotificationListener.class);
        MessageQueueClient client = MessageClientFactory.getMessageClient();
        doCallRealMethod().when(listener).handleNotificationMessage(any(Message.class));
        when(listener.processForNotificationMessageBody(anyString(), any(JsonObject.class))).thenCallRealMethod();
        doCallRealMethod().when(listener).sentToLegacyMessageQueue(any(JsonObject.class));
        when(listener.getMessageClient()).thenReturn(client);

        for (int x = 0; x < 10; x++) {
            listener.handleNotificationMessage(msg);
        }


        ctx.verify(() -> {
            try {

                for (int x = 0; x < 10; x++) {
                    org.iplantc.service.common.messaging.Message notificationmsg = client.pop(FILES_STAGING_TOPIC, FILES_STAGING_QUEUE);
                    JsonNode jsonBody = objectMapper.readTree(notificationmsg.getMessage());
                    assertNotNull(jsonBody, "Notification sent onto queue should be valid json");
                }
            } catch (Exception e) {
                fail("Notification messages sent onto the queue should be retrieved without throwing an exception");
            }
            ctx.completeNow();
        });

    }

    @Test
    void legacyMessageCanBeDecodedByLegacyCode(Vertx vertx, VertxTestContext ctx) throws Exception {
        TransferTask tt = _createTestTransferTask();
        JsonObject body = tt.toJson();
        body.put("id", 1);

        TransferTaskNotificationListener txfrTransferTaskNotificationListener = mock(TransferTaskNotificationListener.class);
        when(txfrTransferTaskNotificationListener.getMessageClient()).thenCallRealMethod();
        doCallRealMethod().when(txfrTransferTaskNotificationListener).sentToLegacyMessageQueue(any());


        NotificationMessageContext messageBodyContext = new NotificationMessageContext(
                TRANSFERTASK_CREATED, body.encode(), tt.getUuid());

        NotificationMessageBody messageBody = new NotificationMessageBody(
                tt.getUuid(), body.getString("owner"), body.getString("tenant_id"),
                messageBodyContext);

        JsonObject notificationBody = new JsonObject(messageBody.toJSON());
        txfrTransferTaskNotificationListener.sentToLegacyMessageQueue(notificationBody);

        BeanstalkClient client = new BeanstalkClient();
        org.iplantc.service.common.messaging.Message message = client.pop(Settings.FILES_STAGING_TOPIC, Settings.FILES_STAGING_QUEUE);
        ctx.verify(() -> {
            assertNotNull(message, "message should be in queue");
            try {
                JsonNode decodedMessage = objectMapper.readTree(message.getMessage());

                assertNotNull(decodedMessage, "decoded object should not be null");

            } catch (Exception e) {
                fail("Decoding valid notification message body should not throw exception", e);
            }
            ctx.completeNow();
        });
    }

}