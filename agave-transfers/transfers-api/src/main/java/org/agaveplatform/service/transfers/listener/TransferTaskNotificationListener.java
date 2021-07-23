package org.agaveplatform.service.transfers.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.model.TransferTaskNotificationMessage;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.uuid.UUIDType;
import org.iplantc.service.notification.queue.messaging.NotificationMessageBody;
import org.iplantc.service.notification.queue.messaging.NotificationMessageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.Objects;

public class TransferTaskNotificationListener extends AbstractNatsListener {
	private static final Logger logger = LoggerFactory.getLogger(TransferTaskNotificationListener.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_NOTIFICATION;

//    List<String> notificationEvents = List.of(
//            TRANSFERTASK_CREATED,
//            TRANSFERTASK_UPDATED,
//            TRANSFERTASK_FINISHED,
//            TRANSFERTASK_FAILED,
//            TRANSFERTASK_PAUSED_COMPLETED,
//            TRANSFERTASK_CANCELED_COMPLETED);

    protected String eventChannel;
    MessageQueueClient legacyMessageClient;

	public TransferTaskNotificationListener() throws IOException, InterruptedException {
	    super();
	}

	public TransferTaskNotificationListener(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
    }

	public TransferTaskNotificationListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super(vertx, eventChannel);
    }

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

    public void setLegacyMessageClient(MessageQueueClient legacyMessageClient) {
        this.legacyMessageClient = legacyMessageClient;
    }

    /**
     * Mockable helper method to retrieve {@link MessageQueueClient} to push notifications onto
     */
    protected MessageQueueClient getLegacyMessageClient() throws MessagingException {
        if (this.legacyMessageClient == null){
            this.legacyMessageClient = MessageClientFactory.getMessageClient();
        }
        return this.legacyMessageClient;
    }

    @Override
	public void start(Promise<Void> startPromise) {

        try {
            //group subscription so each message only processed by this vertical type once
            subscribeToSubjectGroup(EVENT_CHANNEL, this::handleMessage);
        } catch (Exception e) {
            logger.error("TRANSFER_ALL - Exception {}", e.getMessage());
            startPromise.tryFail(e);
        }
    }

    /**
     * Processes the message pulled off the subscription listener, transforms it, and writes a new message to the
     * legacy message queue.
     * @param message the message received from the stream
     */
    protected void handleMessage(Message message) {
        try {
            // parse raw body
            JsonObject body = new JsonObject(message.getMessage());

            TransferTaskNotificationMessage internalMessage = new TransferTaskNotificationMessage(body);
            TransferTask transferTask = internalMessage.getTransferTask();

            // process and transform into serialized legacy message body
            NotificationMessageBody legacyMessageBody =
                    processForNotificationMessageBody(internalMessage.getSourceMessageType(), transferTask);

            sentToLegacyMessageQueue(legacyMessageBody);
        } catch (NullPointerException|DecodeException e) {
            logger.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            logger.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }
    /**
     * Process the {@link JsonObject} we recieve from the message listener to a {@link NotificationMessageBody} for
     * compatibility with our legacy message queue.
     *
     * @param messageType the {@link MessageType} constant value that caused this notification
     * @param transferTask the transferTask on which the event happened
     * @return legacy notification message body
     * @throws NullPointerException when an invalid parameter is provided
     */
    protected NotificationMessageBody processForNotificationMessageBody(@NotNull String messageType, @NotNull TransferTask transferTask) {

        Objects.requireNonNull(messageType);
        Objects.requireNonNull(transferTask);

        NotificationMessageBody notificationMessageBody = null;

        NotificationMessageContext messageBodyContext = new NotificationMessageContext(
                messageType, transferTask.toJson().encode(), transferTask.getUuid());

        notificationMessageBody = new NotificationMessageBody(
                transferTask.getUuid(), transferTask.getOwner(), transferTask.getTenantId(), messageBodyContext);

        logger.info("Legacy {} notification event raised for {} {}",
                messageType,
                UUIDType.TRANSFER.name(),
                transferTask.getUuid());

        return notificationMessageBody;
    }

    /**
     * Writes the notification tot he legacy notification queue. Returns if at least one message was written to the queue.
     *
     * @param legacyMessageBody the message body to send
     */
    protected void sentToLegacyMessageQueue(NotificationMessageBody legacyMessageBody) {
        try {
            MessageQueueClient queue = getLegacyMessageClient();
            queue.push(Settings.FILES_STAGING_TOPIC, Settings.FILES_STAGING_QUEUE, legacyMessageBody.toJSON());
        } catch (MessagingException e){
            logger.error("Failed to push message to legacy queue: {}", e.getMessage());
        } catch (JsonProcessingException e){
            logger.error("Failed to serialize the legacy message body to json. Message was not pushed to legacy queue: {}",
                    e.getMessage());
        } catch (Throwable t){
            logger.error("Failed to process legacy notification message. Message was not pushed to legacy queue: {}",
                    t.getMessage());
        }

    }

    /**
     * If your verticle has simple synchronous clean-up tasks to complete then override this method and put your clean-up
     * code in here.
     *
     * @throws Exception when parent is unable to stop.
     */
    @Override
    public void stop() throws Exception {

        try {
            getLegacyMessageClient().stop();
        } catch (Exception e) {
            logger.error("Failed to shutdown legacy message queue client on shutdown: {}", e.getMessage());
        }

        super.stop();
    }
}
