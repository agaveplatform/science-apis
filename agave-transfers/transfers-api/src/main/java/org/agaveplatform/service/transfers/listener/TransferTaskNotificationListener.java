package org.agaveplatform.service.transfers.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.iplantc.service.notification.managers.NotificationManager;
import org.iplantc.service.notification.queue.messaging.NotificationMessageBody;
import org.iplantc.service.notification.queue.messaging.NotificationMessageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;


public class TransferTaskNotificationListener extends AbstractTransferTaskListener {
	private static final Logger logger = LoggerFactory.getLogger(TransferTaskNotificationListener.class);
	protected static final String EVENT_CHANNEL = TRANSFERTASK_NOTIFICATION ;

	protected String eventChannel;

	public TransferTaskNotificationListener() { super(); }
	public TransferTaskNotificationListener(Vertx vertx) {
		super(vertx);
	}
	public TransferTaskNotificationListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {

        List<String> notificationEvents = List.of(
                TRANSFERTASK_CREATED,
                TRANSFERTASK_UPDATED,
                TRANSFERTASK_FINISHED,
                TRANSFERTASK_FAILED,
                TRANSFERTASK_PAUSED_COMPLETED,
                TRANSFERTASK_CANCELED_COMPLETED);

        EventBus bus = vertx.eventBus();

        notificationEvents.stream().forEach(event -> {
            bus.consumer(event, this::handleNotificationMessage);
        });
    }

    /**
     * Parses messages as they come in from the event bus, serializing to {@link JsonObject} and sending to the
     * legacy message queue.
     * @param msg the incoming message from the {@link EventBus}
     */
    protected void handleNotificationMessage(Message<JsonObject> msg) {
        msg.reply(TransferTaskNotificationListener.class.getName() + " received.");

        JsonObject body = msg.body();
        JsonObject notificationMessageBody = processForNotificationMessageBody(msg.address(), body);
        sentToLegacyMessageQueue(notificationMessageBody);
    }


    /**
     * Process the {@link JsonObject} we recieve from the {@link EventBus} to a {@link NotificationMessageBody} for \
     * compatibility with our legacy message queue.
     *
     * @param messageType {@link MessageType} for the Transfer Task notification event
     * @param body        {@link JsonObject} of the Transfer Task
     * @return {@link NotificationMessageBody}
     */
    protected JsonObject processForNotificationMessageBody(String messageType, JsonObject body) {
        try {
            String uuid = body.getString("uuid");
            String tenantId = body.getString("tenant_id");

            logger.debug("tenantId = {}", tenantId);
            if (uuid == null) {
                logger.error("Transfer task uuid cannot be null.");
            } else {
                NotificationMessageContext messageBodyContext = new NotificationMessageContext(
                        messageType, body.encode(), uuid);

                NotificationMessageBody messageBody = new NotificationMessageBody(
                        uuid, body.getString("owner"), body.getString("tenant_id"),
                        messageBodyContext);

                if (body.getString("event") == null)
                    body.put("event", body.getString("status"));
                if (body.getString("type") == null) {
                    body.put("type", messageType);
                }

                logger.info("{} notification event raised for {} {}: {}",
                        body.getString("event"),
                        body.getString("type"),
                        body.getString("uuid"),
                        body.encodePrettily());

                return new JsonObject(messageBody.toJSON());
            }
        } catch (JsonProcessingException e) {
            logger.error("Failed to serialize notification for transfer task {} to legacy message format. {}",
                    body.getString("uuid"), e.getMessage());
        }
        return null;
    }

    /**
     * Writes the notification tot he legacy notification queue. Returns if at least one message was written to the queue.
     *
     * @param body the message body to send
     * @return true if a message was written
     */
    protected boolean sentToLegacyMessageQueue(JsonObject body) {
        logger.info("Sending legacy notification message for transfer task {}", body.getString("uuid"));
        logger.debug("tenantId = {}", body.getString("tenant_id"));
        org.iplantc.service.common.Settings.NOTIFICATION_QUEUE = org.iplantc.service.common.Settings.FILES_STAGING_QUEUE;
        org.iplantc.service.common.Settings.NOTIFICATION_TOPIC = org.iplantc.service.common.Settings.FILES_STAGING_TOPIC;

        return NotificationManager.process(body.getString("uuid"), body.encode(), body.getString("owner")) > 0;
    }

}
