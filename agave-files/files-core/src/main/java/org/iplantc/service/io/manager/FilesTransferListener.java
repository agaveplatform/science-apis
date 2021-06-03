package org.iplantc.service.io.manager;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.messaging.MessageQueueListener;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.notification.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to listen and handle events sent from agave-transfers and update the corresponding LogicalFile accordingly.
 */
public class FilesTransferListener extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(FilesTransferListener.class);
    protected static final String COMPLETED_EVENT_CHANNEL = "transfer.completed";
    protected static final String FAILED_EVENT_CHANNEL = "transfertask.failed";

    protected String eventChannel;
    private MessageQueueClient messageClient;

    public FilesTransferListener() { super(); }
//    public FilesTransferListener(Vertx vertx) {
//        super(vertx);
//    }
//    public FilesTransferListener(Vertx vertx, String eventChannel) {
//        super(vertx, eventChannel);
//    }

    public String getFailedEventChannel() {
        return FAILED_EVENT_CHANNEL;
    }
    public MessageQueueClient getMessageClient() throws MessagingException {
        if (messageClient == null) {
            messageClient = MessageClientFactory.getMessageClient();
        }
        return messageClient;
    }

    public void setMessageClient(org.iplantc.service.common.messaging.MessageQueueClient messageClient) {
        this.messageClient = messageClient;
    }

    public String getCompletedEventChannel() {
        return COMPLETED_EVENT_CHANNEL;
    }


    public void start(){
        EventBus bus = vertx.eventBus();

        do {


            Message message = null;
            try
            {
                message = getMessageClient().listen(StagingTaskStatus.STAGING_COMPLETED, StagingTaskStatus.STAGING_COMPLETED, new MessageQueueListener() {
                    @Override
                    public void processMessage(String body) throws MessageProcessingException {
                        logger.debug("Received completed notification from transfertask event");
                        processTransferNotification(StagingTaskStatus.STAGING_COMPLETED, body, FilesTransferListener::handler);

                    }

                    @Override
                    public void stop() {
                        try {
                            getMessageClient().stop();
                        } catch (MessagingException e) {
                            e.printStackTrace();
                        }
                    }
                });

                message = getMessageClient().listen(StagingTaskStatus.STAGING_FAILED, StagingTaskStatus.STAGING_FAILED, new MessageQueueListener() {
                    @Override
                    public void processMessage(String body) throws MessageProcessingException {
                        logger.debug("Received failed notification from transfertask event");
                        processTransferNotification(StagingTaskStatus.STAGING_COMPLETED, body, result -> {
                            if (result.succeeded()){
                                //logical file updated
                                try {
                                    getMessageClient().delete(Settings.NOTIFICATION_TOPIC, Settings.NOTIFICATION_QUEUE, body.getId());
                                } catch (MessagingException e) {
                                    e.printStackTrace();
                                }
                            } else {
                                try {
                                    getMessageClient().reject(Settings.NOTIFICATION_TOPIC, Settings.NOTIFICATION_QUEUE, body.getId(), body.getMessage());
                                } catch (MessagingException e) {
                                    e.printStackTrace();
                                }
                            }
                        });
                    }

                    @Override
                    public void stop() {
                        try {
                            getMessageClient().stop();
                        } catch (MessagingException e) {
                            e.printStackTrace();
                        }
                    }
                });


            }
            catch (MessagingException|MessageProcessingException e) {

            }

        } while (true);

    }

    protected static void handler(AsyncResult<Boolean> booleanAsyncResult) {

        if (booleanAsyncResult.succeeded()){
            //logical file updated
            try {
                getMessageClient().delete(Settings.NOTIFICATION_TOPIC, Settings.NOTIFICATION_QUEUE, "foo");
            } catch (MessagingException e) {
                e.printStackTrace();
            }
        } else {
            try {
                getMessageClient().reject(Settings.NOTIFICATION_TOPIC, Settings.NOTIFICATION_QUEUE, body.getId(), body.getMessage());
            } catch (MessagingException e) {
                e.printStackTrace();
            }
        }

    }

    public void processTransferNotification(StagingTaskStatus status, JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        // Parse data
        String srcUrl = body.getString("source");
        String createdBy = body.getString("owner");
        String transferUuid = body.getString("uuid");

        // Retrieve current logical file
        LogicalFile file = LogicalFileDao.findByTransferUuid(transferUuid);
        if (file != null) {
            try {
                // Update logical file
                LogicalFileDao.updateTransferStatus(file, status, createdBy);
                handler.handle(Future.succeededFuture(true));
            } catch (Exception e) {
                logger.debug("Unable to update transfer status");
                handler.handle(Future.failedFuture(e));
            }
        } else {
            //
            handler.handle(Future.failedFuture("No existing file found matching transfer " + transferUuid));
        }
    }
}
