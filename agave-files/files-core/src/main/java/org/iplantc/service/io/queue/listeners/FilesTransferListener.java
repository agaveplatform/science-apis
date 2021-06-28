package org.iplantc.service.io.queue.listeners;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.messaging.MessageQueueListener;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.exceptions.LogicalFileException;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.List;

import static org.iplantc.service.common.Settings.FILES_STAGING_QUEUE;
import static org.iplantc.service.common.Settings.FILES_STAGING_TOPIC;
import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.*;

/**
 * Class to listen and handle events sent from agave-transfers and update the corresponding LogicalFile accordingly.
 */
public class FilesTransferListener implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FilesTransferListener.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static MessageQueueClient messageClient;
//    private final HashMap<StagingTaskStatus, FilesTransferListener.FileTransferMessageQueueListener> queueListeners = new HashMap();

    public FilesTransferListener() {}

    public MessageQueueClient getMessageClient() throws MessagingException {
        if (messageClient == null) {
            messageClient = MessageClientFactory.getMessageClient();
        }
        return messageClient;
    }

    public void setMessageClient(org.iplantc.service.common.messaging.MessageQueueClient messageClient) {
        FilesTransferListener.messageClient = messageClient;
    }

    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        try {
            while (true) {
                MessageQueueClient messageQueueClient = null;
                Message msg = null;
                try {
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException();
                    }
                    messageQueueClient = getMessageClient();
                }
                catch (MessagingException e) {
                    logger.error("Unable to create messaging client", e);
                    try {
                        Thread.sleep(5000);} catch (Exception ignored){}
                    continue;
                }

                try {
                    // dynamically register all the status listeners we want to take action on
                    List<StagingTaskStatus> stagingTaskStatuses = List.of(STAGING_COMPLETED, STAGING_FAILED, STAGING, STAGING_QUEUED);

                    msg = messageClient.pop(org.iplantc.service.common.Settings.FILES_STAGING_TOPIC,
                            org.iplantc.service.common.Settings.FILES_STAGING_QUEUE);
                    try {
                        logger.debug("Messaged received from transfer service ");
                        JsonNode jsonBody = objectMapper.readTree(msg.getMessage());

                        processTransferNotification(jsonBody);
                        messageClient.delete(org.iplantc.service.common.Settings.FILES_STAGING_TOPIC,
                                org.iplantc.service.common.Settings.FILES_STAGING_QUEUE,
                                msg.getId());
                    } catch (IOException|MessageProcessingException e) {
                        logger.error("Unable to parse message body, {}: {}", msg.getMessage(), e.getMessage());
                        try {
                            messageClient.reject(org.iplantc.service.common.Settings.FILES_STAGING_TOPIC,
                                    org.iplantc.service.common.Settings.FILES_STAGING_QUEUE, msg.getId(), msg.getMessage());
                        } catch (MessagingException e1) {
                            logger.error("Failed to release message back to the queue. This message will timeout and return on its own.");
                            throw e1;
                        }
                    }

                    // check for thread interrupt
                    if (Thread.currentThread().isInterrupted()) {
                        throw new InterruptedException();
                    }

                    //            // iterate over each status, creating a new listener for each
                    //            for (StagingTaskStatus status : stagingTaskStatuses) {
                    //                FileTransferMessageQueueListener listener = new FileTransferMessageQueueListener(status);
                    //                // this will run in the background, calling the listener#processMessage(String) method every time a
                    //                // message comes in.
                    //
                    //                        status.name(),
                    //                        listener);
                    //
                    //                // keep a reference to each listener so we can manually stop them during shutdown.
                    //                queueListeners.put(status, listener);
                    //            }
                    //
                    //            // we simply wait for an interrupt to the parent thread (which we know is controlled by an
                    //            // ExecutorService) to interrrupt us. When that happens, we stop the listeners and propagate
                    //            // the interrupt exception to stop the client and exit.
                    //            while (true) {
                    //                if (Thread.currentThread().isInterrupted()) {
                    //                    queueListeners.forEach((key, listener) -> {
                    //                        // call stop on each listener to unsubscribe from the queue/topic/subject it was listening to
                    //                        listener.stop();
                    //                    });
                    //                    throw new InterruptedException();
                    //                }
                    //            }
                } catch (MessagingException e) {
                    logger.error("Failure communicating with message queue", e);
                    if (messageQueueClient != null) {
                        messageQueueClient.stop();
                    }
                    messageQueueClient = null;
                    setMessageClient(null);
                } catch (InterruptedException e) {
                    throw e;
                } catch (Throwable t) {
                    logger.error("Unexpected exception caught in transfer listener. Ignoring...", t);
                }
            }
        } catch (InterruptedException e) {
            try {
                getMessageClient().stop();
            } catch (MessagingException e1) {
                logger.error("Failed to stop message client gracefully", e1);
            }
        }
        finally {
            logger.info("File transfer listener worker completed.");
        }
    }


    /**
     * Process the {@link JsonNode} notification from the transfers service to match the transfer status to the legacy
     * staging status for a logical file.
     * @param jsonBody [@link JsonNode} of the notification to process
     * @throws MessageProcessingException if unknown transfer status or no logical file matching the source of the transfer
     * @throws IOException if unable to parse notification
     */
    public void processTransferNotification(JsonNode jsonBody) throws MessageProcessingException, IOException {
        try {
            JsonNode jsonTransferTask = objectMapper.readTree(jsonBody.at("/context/customData").textValue());

            // Parse data
            String srcUrl = jsonTransferTask.get("source").textValue();
            String destUrl = jsonTransferTask.get("dest").textValue();
            String createdBy = jsonTransferTask.get("owner").textValue();
            String transferUuid = jsonTransferTask.get("uuid").textValue();
            String transferStatus = jsonBody.at("/context/event").textValue();
            String tenantId = jsonTransferTask.get("tenant_id").textValue();

            logger.debug("Retrieved transfer " + transferUuid + " with status " + transferStatus + " for transfer from " + srcUrl + " to " + destUrl);

            // Retrieve current logical file
            LogicalFile sourceLogicalFile = lookupLogicalFileByUrl(srcUrl, tenantId);
            if (sourceLogicalFile != null) {
                // Update logical file
                if (transferStatus.equals("transfertask.created")){
                    updateTransferStatus(sourceLogicalFile, STAGING, createdBy);
                } else if (transferStatus.equals("transfertask.assigned")) {
                    updateTransferStatus(sourceLogicalFile, STAGING_QUEUED, createdBy);
                } else if (transferStatus.equals("transfertask.failed") || transferStatus.equals("transfer.failed")) {
                    updateTransferStatus(sourceLogicalFile, StagingTaskStatus.STAGING_FAILED, createdBy);
                } else if (transferStatus.equals("transfer.completed") || transferStatus.equals("transfertask.finished")) {
                    logger.debug("Creating logical file for the destination location");
                    updateTransferStatus(sourceLogicalFile, StagingTaskStatus.STAGING_COMPLETED, createdBy);

                    // now we update the destination if it exists and is warranted
                    updateDestinationLogicalFile(sourceLogicalFile, destUrl, createdBy);

                } else if (List.of("transfertask.notification", "transfertask.updated").contains(transferStatus)){
                    //these known event types don't have an equivalent staging task status
                }else {
                    logger.error("Failed to process notification due to unknown status " + transferStatus);
                    throw new MessageProcessingException("Unable to process notification due to unknown status " + transferStatus);
                }
            } else {
                logger.debug("Unable to find logical file for source " + srcUrl + " with tenant " + tenantId);
                throw new MessageProcessingException("No existing file found matching transfer " + transferUuid);
            }
        } catch (MessageProcessingException e) {
            throw e;
        } catch (Exception e) {
            throw new MessageProcessingException("Unable to update transfer status", e);
        }
    }

    /**
     * Mockable method to update destination {@link LogicalFile}. If the destination system does not exist, we skip the
     * logical file creation and event updates. Otherwise, we create or update the logical file and send the appropriate
     * update/overwritten event as appropriate.
     * @param sourceLogicalFile source logical file of the transfer task
     * @param dest the destination url from the transfertask message body
     * @param username the owner of the transfertask message
     * @return the updated destination logicalfile
     * @throws LogicalFileException if unable to lookup the logical file
     */
    protected LogicalFile updateDestinationLogicalFile(LogicalFile sourceLogicalFile, String dest, String username) throws LogicalFileException, RemoteDataException, RemoteCredentialException, IOException {
        LogicalFile destFile = null;
        destFile = lookupLogicalFileByUrl(dest, sourceLogicalFile.getTenantId());
        if (destFile != null) {
            destFile.addContentEvent(FileEventType.OVERWRITTEN, username);
            persistLogicalFile(destFile);
        } else {
            URI destUri = URI.create(dest);
            RemoteSystem destSystem = getSystemById(destUri.getHost(), username, sourceLogicalFile.getTenantId());
            if (destSystem == null && !sourceLogicalFile.getSystem().getRemoteDataClient().doesExist(dest)){
                logger.debug("No matching system found for the destination of the transfer task. Skipping logical file update and events.");
            } else {
                destFile = new LogicalFile(username, destSystem, destUri.getPath());
                destFile.setSourceUri(sourceLogicalFile.getPath());
//                destFile.setSourceUri(String.format("agave://%s/%s", sourceLogicalFile.getSystem(), sourceLogicalFile.getPath()));
                destFile.setStatus(FileEventType.CREATED.name());

                persistLogicalFile(destFile);
            }
        }
        return destFile;
    }


    /**
     * Mockable wrapper to fetch transfer task destination system by id, tenant, and username. The owner of the transfer
     * task will have already been vetted to make the transfer, so they clearly have access to the destination system.
     * We simply add that to the query to ensure that we don't run into race conditions on access privileges, and to
     * speed up the query a bit.
     *
     * @param systemId the {@link RemoteSystem#getSystemId()} to use as the lookup id
     * @param username the principal who should have access to the system (this is included in the lookup)
     * @param tenantId the tenant in which the systme should reside.
     * @return the matching system or null if no match is found.
     */
    protected RemoteSystem getSystemById(String systemId, String username, String tenantId) {
        return new SystemDao().findUserSystemBySystemIdAndTenant(username, systemId, tenantId);
    }


    /**
     * Mockable helper method to wrap the static call to {@link LogicalFileDao#persist(LogicalFile}}.
     * @param file the file to add or update
     */
    protected void persistLogicalFile(LogicalFile file){
        LogicalFileDao.persist(file);
    }

    /**
     * Mockable helper method to wrap the static call to  {@link LogicalFileDao#findBySourceUrl(String)}.
     * @param srcUrl the url for which to lookup the logical file
     * @param tenantId the id of the tenant for which to lookup the url
     * @return the logical file for the given url or null if not found
     * @see LogicalFileDao#findBySourceUrl(String)
     */
    protected LogicalFile lookupLogicalFileByUrl(String srcUrl, String tenantId) throws LogicalFileException {
        return LogicalFileDao.findBySourceUrlAndTenant(srcUrl, tenantId);
    }

    /**
     * Updates the logical file status and sends an event of the status change. This is a helper for the call to
     * {@link LogicalFileDao#updateTransferStatus(LogicalFile, StagingTaskStatus, String)} to make testing much easier.
     *
     * @param file the logical file to update
     * @param stagingTaskStatus the new status
     * @param username the username to whom the update event will be attributed
     * @see LogicalFileDao#updateTransferStatus(LogicalFile, StagingTaskStatus, String)
     */
    protected void updateTransferStatus(LogicalFile file, StagingTaskStatus stagingTaskStatus, String username) {
        LogicalFileDao.updateTransferStatus(file, stagingTaskStatus, username);
    }

//    class FileTransferMessageQueueListener implements MessageQueueListener {
//
//        private StagingTaskStatus messageStatus;
//
//        public FileTransferMessageQueueListener(StagingTaskStatus messageStatus) {
//            this.messageStatus = messageStatus;
//        }
//
//        @Override
//        public void processMessage(String body) throws MessageProcessingException {
//            logger.debug("Received completed notification from transfertask event");
//            try {
//                JsonNode jsonBody = objectMapper.readTree(body);
//                if (jsonBody.get("status").textValue().equalsIgnoreCase(getMessageStatus().name())) {
//                    processTransferNotification(jsonBody);
//                } else {
//                    throw new MessageProcessingException();
//                }
//            } catch (MessageProcessingException e) {
//                throw e;
//            } catch (IOException e) {
//                logger.error("Unable to parse message body: {}", e.getMessage());
//            }
//        }
//
//
//        @Override
//        public void stop() {
//            try {
//                logger.debug("Stopped called on the " + getMessageStatus().name() + " listener." );
//                getMessageClient().stop();
//            } catch (MessagingException e) {
//                logger.error("Failed to stop " + getMessageStatus().name() + " listener:" + e.getMessage());
//            }
//        }
//
//        public StagingTaskStatus getMessageStatus() {
//            return messageStatus;
//        }
//
//        public void setMessageStatus(StagingTaskStatus messageStatus) {
//            this.messageStatus = messageStatus;
//        }
//    }


}
