package org.iplantc.service.io.manager;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.messaging.MessageQueueListener;
import org.iplantc.service.io.Settings;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class to listen and handle events sent from agave-transfers and update the corresponding LogicalFile accordingly.
 */
public class FilesTransferListener extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(FilesTransferListener.class);
    private static MessageQueueClient messageClient;

    public FilesTransferListener() {
        super();
    }

    public static MessageQueueClient getMessageClient() throws MessagingException {
        if (messageClient == null) {
            messageClient = MessageClientFactory.getMessageClient();
        }
        return messageClient;
    }

    public void setMessageClient(org.iplantc.service.common.messaging.MessageQueueClient messageClient) {
        this.messageClient = messageClient;
    }


    public void start() {
        do {

            Message message = null;
            try {
                getMessageClient().listen(StagingTaskStatus.STAGING_COMPLETED.name(), StagingTaskStatus.STAGING_COMPLETED.name(), new MessageQueueListener() {
                    @Override
                    public void processMessage(String body) {
                        logger.debug("Received completed notification from transfertask event");
                        processTransferNotification(StagingTaskStatus.STAGING_COMPLETED, body, FilesTransferListener::handler);
                    }

                    @Override
                    public void stop() {
                        try {
                            getMessageClient().stop();
                        } catch (MessagingException e) {
                            logger.error("Failed to stop message client:" + e.getMessage());
                        }
                    }
                });

                getMessageClient().listen(StagingTaskStatus.STAGING_FAILED.name(), StagingTaskStatus.STAGING_FAILED.name(), new MessageQueueListener() {
                    @Override
                    public void processMessage(String body) {
                        logger.debug("Received failed notification from transfertask event");
                        processTransferNotification(StagingTaskStatus.STAGING_FAILED, body, FilesTransferListener::handler);
                    }

                    @Override
                    public void stop() {
                        try {
                            getMessageClient().stop();
                        } catch (MessagingException e) {
                            logger.error("Failed to stop message client:" + e.getMessage());
                        }
                    }
                });

                getMessageClient().listen(StagingTaskStatus.STAGING.name(), StagingTaskStatus.STAGING.name(), new MessageQueueListener() {
                    @Override
                    public void processMessage(String body) {
                        logger.debug("Received staging notification from transfertask event");
                        processTransferNotification(StagingTaskStatus.STAGING, body, FilesTransferListener::handler);
                    }

                    @Override
                    public void stop() {
                        try {
                            getMessageClient().stop();
                        } catch (MessagingException e) {
                            logger.error("Failed to stop message client:" + e.getMessage());
                        }
                    }
                });

                getMessageClient().listen(StagingTaskStatus.STAGING_QUEUED.name(), StagingTaskStatus.STAGING_QUEUED.name(), new MessageQueueListener() {
                    @Override
                    public void processMessage(String body) {
                        logger.debug("Received staging queued notification from transfertask event");
                        processTransferNotification(StagingTaskStatus.STAGING_QUEUED, body, FilesTransferListener::handler);
                    }

                    @Override
                    public void stop() {
                        try {
                            getMessageClient().stop();
                        } catch (MessagingException e) {
                            logger.error("Failed to stop message client:" + e.getMessage());
                        }
                    }
                });


            } catch (MessagingException | MessageProcessingException e) {
                logger.error("Unable to process message: " + e.getMessage());
            }

        } while (true);

    }

    protected static void handler(AsyncResult<JsonNode> booleanAsyncResult) {

        if (booleanAsyncResult.succeeded()) {
            //logical file updated
            try {
                getMessageClient().delete(Settings.TRANSFER_NOTIFICATION_SUBJECT, Settings.TRANSFER_NOTIFICATION_QUEUE,
                        booleanAsyncResult.result().get("id"));
            } catch (MessagingException e) {
                logger.error("Failed to remove processed messages from queue: " + e.getMessage());
            }
        } else {
            try {
                getMessageClient().reject(Settings.TRANSFER_NOTIFICATION_SUBJECT, Settings.TRANSFER_NOTIFICATION_QUEUE,
                        booleanAsyncResult.result().get("id"), booleanAsyncResult.result().get("message").textValue());
            } catch (MessagingException e) {
                logger.error("Failed to process messages from queue: " + e.getMessage());
            }
        }

    }

    public void processTransferNotification(StagingTaskStatus status, String body, Handler<AsyncResult<JsonNode>> handler) {
        // Parse data
        try {
            JsonNode jsonBody = new ObjectMapper().readTree(body);

            String srcUrl = jsonBody.get("source").textValue();
            String createdBy = jsonBody.get("owner").textValue();
            String transferUuid = jsonBody.get("uuid").textValue();
            String transferStatus = jsonBody.get("status").textValue();


            // Retrieve current logical file
            LogicalFile file = LogicalFileDao.findBySourceUrl(srcUrl);
            if (file != null) {
                try {
                    // Update logical file
                    if (transferStatus.equals("transfertask.assigned")) {
                        LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_QUEUED, createdBy);

                    } else if (transferStatus.equals("transfertask.created")) {
                        LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING, createdBy);

                    } else if (transferStatus.equals("transfertask.completed")) {
                        LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_COMPLETED, createdBy);

                    } else if (transferStatus.equals("transfertask.failed")) {
                        LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_FAILED, createdBy);

                    } else if (transferStatus.equals("transfer.completed")) {
                        LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_COMPLETED, createdBy);

                        LogicalFile destFile = LogicalFileDao.findBySystemAndPath(file.getSystem(), file.getPath());
                        if (destFile != null) {
                            destFile.setStatus(FileEventType.OVERWRITTEN.name());
                            LogicalFileDao.persist(file);
                        } else {
                            destFile = new LogicalFile(file.getOwner(), file.getSystem(), file.getPath());
                            LogicalFileDao.persist(destFile);
                        }
                    }
                    handler.handle(Future.succeededFuture(jsonBody));
                } catch (Exception e) {
                    logger.debug("Unable to update transfer status");
                    handler.handle(Future.failedFuture(e));
                }
            } else {
                handler.handle(Future.failedFuture("No existing file found matching transfer " + transferUuid));
            }
        } catch (IOException ioException) {
            logger.error("Transfer notification processing failed: " + ioException.getMessage());
            handler.handle(Future.failedFuture(ioException));
        }
    }
}
