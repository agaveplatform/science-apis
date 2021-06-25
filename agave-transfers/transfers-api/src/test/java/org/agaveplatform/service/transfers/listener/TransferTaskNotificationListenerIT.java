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
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.powermock.core.classloader.annotations.PrepareForTest;

import static org.iplantc.service.common.Settings.FILES_STAGING_QUEUE;
import static org.iplantc.service.common.Settings.FILES_STAGING_TOPIC;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;


@ExtendWith(VertxExtension.class)
@DisplayName("Notification listener integration tests")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@PrepareForTest({JDBCClient.class})
class TransferTaskNotificationListenerIT extends BaseTestCase {
    private ObjectMapper mapper = new ObjectMapper();

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
                    JsonNode jsonBody = mapper.readTree(notificationmsg.getMessage());
                    assertNotNull(jsonBody, "Notification sent onto queue should be valid json");
                }
            } catch (Exception e) {
                fail("Notification messages sent onto the queue should be retrieved without throwing an exception");
            }
            ctx.completeNow();
        });

    }

}