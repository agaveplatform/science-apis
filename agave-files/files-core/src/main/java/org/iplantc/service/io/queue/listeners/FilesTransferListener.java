package org.iplantc.service.io.queue.listeners;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.log4j.Logger;
import org.iplantc.service.common.exceptions.MessageProcessingException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.messaging.MessageClientFactory;
import org.iplantc.service.common.messaging.MessageQueueClient;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.exceptions.LogicalFileException;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.FileEventType;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.exceptions.RemoteDataException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import static org.iplantc.service.common.Settings.FILES_STAGING_QUEUE;
import static org.iplantc.service.common.Settings.FILES_STAGING_TOPIC;
import static org.iplantc.service.io.model.enumerations.StagingTaskStatus.*;

/**
 * Class to listen and handle events sent from agave-transfers and update the corresponding LogicalFile accordingly.
 */
public class FilesTransferListener implements Runnable {
    private static final Logger logger = Logger.getLogger(FilesTransferListener.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();
    private MessageQueueClient messageClient;

    public enum TransferTaskEventType {
        TRANSFERTASK_CREATED("transfertask.created", false, STAGING_QUEUED),
        TRANSFERTASK_ASSIGNED("transfertask.assigned", false, STAGING),
        TRANSFER_RETRY("transfer.retry", false, STAGING_QUEUED),
//        TRANSFERTASK_UPDATED("transfertask.updated", false, STAGING),

        TRANSFERTASK_FAILED("transfertask.failed", true, STAGING_FAILED),
        TRANSFERTASK_FINISHED("transfertask.finished", true, STAGING_COMPLETED),
        TRANSFERTASK_CANCELED("transfertask.canceled", true, STAGING_FAILED);

        private final String eventName;
        private final boolean terminal;
        private final StagingTaskStatus status;

        TransferTaskEventType(String eventName, boolean terminal, StagingTaskStatus status) {
            this.eventName = eventName;
            this.terminal = terminal;
            this.status = status;
        }

        public String getEventName() {
            return this.eventName;
        }

        public StagingTaskStatus getStagingTaskStatus() {
            return this.status;
        }

        public boolean isTerminal() {
            return this.terminal;
        }

        /**
         * Returns the supported {@link TransferTaskEventType} corresponding to the given string value. If no match is
         * found, the value is unknown.
         * @param eventName the transfer event name to lookup
         * @return the {@link TransferTaskEventType} with a matching status, or null if no match
         */
        public static TransferTaskEventType valueOfEventName(String eventName) {
            if (eventName == null) return null;
            for (TransferTaskEventType val: values()) {
                if (val.getEventName().equals(eventName)) {
                    return val;
                }
            }
            return null;
        }
    }

//    private final HashMap<StagingTaskStatus, FilesTransferListener.FileTransferMessageQueueListener> queueListeners = new HashMap();

    public FilesTransferListener() {}

    public MessageQueueClient getMessageClient() throws MessagingException {
        if (messageClient == null) {
            messageClient = MessageClientFactory.getMessageClient();
        }
        return messageClient;
    }

    public void setMessageClient(MessageQueueClient messageClient) {
        this.messageClient = messageClient;
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
            while (! isThreadInterrupted()) {
//                MessageQueueClient messageQueueClient = null;
                Message msg = null;
                try {
                    getMessageClient();
                }
                catch (MessagingException e) {
                    logger.error("Unable to create messaging client", e);
                    try {
                        Thread.sleep(5000);} catch (Exception ignored){}
                    continue;
                }

                try {
                    // dynamically register all the status listeners we want to take action on
//                    List<StagingTaskStatus> stagingTaskStatuses = List.of(STAGING_COMPLETED, STAGING_FAILED, STAGING, STAGING_QUEUED);

                    msg = getMessageClient().pop(FILES_STAGING_TOPIC, FILES_STAGING_QUEUE);
                    try {
                        logger.debug("Messaged received from transfer service ");
                        JsonNode jsonBody = objectMapper.readTree(msg.getMessage());

                        processTransferNotification(jsonBody);

//                        getMessageClient().delete(FILES_STAGING_TOPIC, FILES_STAGING_QUEUE, msg.getId());
                    }
                    catch (JsonProcessingException e) {
                        String message = String.format("Invalid json found in transfer task message body, %s: %s",
                            msg.getMessage(), e.getMessage());
                        logger.error(message);
                    }
                    catch (MessageProcessingException e) {
                        String message = String.format("Unable to parse message body, %s: %s",
                                msg.getMessage(), e.getMessage());
                        logger.error(message);
                    }
                    finally {
                        getMessageClient().delete(FILES_STAGING_TOPIC, FILES_STAGING_QUEUE, msg.getId());
                    }
                }
                catch (MessagingException e) {
                    logger.error("Failure communicating with message queue: " + e.getMessage());
                    getMessageClient().stop();
                    setMessageClient(null);
                } catch (Throwable t) {
                    logger.error("Unexpected exception caught in file transfer listener. Ignoring...", t);
                }
            }
        } catch (Throwable e) {
            logger.debug("Listener thread was interrupted");
        }
        finally {
            try {
                getMessageClient().stop();
                setMessageClient(null);
            } catch (MessagingException e1) {
                logger.error("Failed to stop file transfer listener message client gracefully", e1);
            }
            logger.info("File transfer listener worker completed.");
        }
    }

    /**
     * Mockable method to check for current thread iterruption
     * @return true if current thread is interrupted
     */
    protected boolean isThreadInterrupted() {
        return Thread.currentThread().isInterrupted();
    }

    /**
     * Process the {@link JsonNode} notification from the transfers service to match the transfer status to the legacy
     * staging status for a logical file.
     * @param jsonBody [@link JsonNode} of the notification to process
     * @throws MessageProcessingException if unknown transfer status or no logical file matching the source of the transfer
     */
    public void processTransferNotification(JsonNode jsonBody) throws MessageProcessingException {
        try {
            JsonNode jsonTransferTask = objectMapper.readTree(jsonBody.at("/context/customData").textValue());

            // Parse data
            String transferTaskEventName = jsonBody.at("/context/event").textValue();
            String srcUrl = jsonTransferTask.get("source").textValue();
            String destUrl = jsonTransferTask.get("dest").textValue();
            String createdBy = jsonTransferTask.get("owner").textValue();
            String transferUuid = jsonTransferTask.get("uuid").textValue();
            String tenantId = jsonTransferTask.get("tenant_id").textValue();

            // refactored the transfer task event type to an enum and we validate that we recognize the value in
            // the notification here so we can skip several db queries and potential typos. In production, there will
            // be thousands of events coming through a minute per instance, so this really starts to add up as we scale.
            TransferTaskEventType transferTaskEvent = TransferTaskEventType.valueOfEventName(transferTaskEventName);

            if (transferTaskEvent == null) {
                logger.debug(String.format("Ignoring unknown %s event for transfer task %s on behalf of %s in tenant %s",
                        transferTaskEventName, transferUuid, createdBy, tenantId));
            }
            else {
                logger.debug(String.format("Start processing %s event for transfer task %s on behalf of %s in tenant %s",
                        transferTaskEventName, transferUuid, createdBy, tenantId));

                updateSourceLogicalFile(srcUrl, createdBy, transferTaskEvent, tenantId);

                updateDestinationLogicalFile(destUrl, createdBy, transferTaskEvent, tenantId);

                logger.info(String.format("Completed processing %s event for transfer task %s on behalf of %s in tenant %s",
                        transferTaskEventName, transferUuid, createdBy, tenantId));
            }
        }
        catch (IOException e) {
            throw new MessageProcessingException("Invalid json found in transfer task message customData field", e);
        }
    }

    /**
     * Updates the status of the logical file corresponding to the {@code srcUrl}. This may not exist, and may never
     * have existed, so we fail gracefully if it's not found. All exceptions are swallowed as this is a one-shot
     * delivery attempt and ordering matters.
     *
     * @param srcUrl the source url from the transfer task message body
     * @param username the owner of the transfer task message
     * @param transferTaskEventType the transfer task event in the message
     * @param tenantId the tenant code of the transfer and logical file
     */
    protected void updateSourceLogicalFile(String srcUrl, String username, TransferTaskEventType transferTaskEventType, String tenantId) {
        try {
            // Retrieve current logical file
            LogicalFile sourceLogicalFile = lookupSourceLogicalFileByUrl(srcUrl, tenantId);
            // if the logical file exists, this was a transfer from a known agave system via a files-import request.
            // we want to update the source logical file status for backward compatibility.
            if (sourceLogicalFile != null) {
                // Update logical file
                updateTransferStatus(sourceLogicalFile, transferTaskEventType.getStagingTaskStatus(), username);

                logger.debug(String.format("Published %s event on logical file %s for transfer task source %s in tenant %s",
                        transferTaskEventType.getStagingTaskStatus().name(), sourceLogicalFile.getUuid(),
                        srcUrl, tenantId));
            } else {
                // if the logical file is null, then it still may have existed, but been deleted before this notification
                // was processed. In this situation, the notification would fail to resolve anyway, so there is nothing
                // further to do with respect to the sourceUrl. We do still need to check for a destination logical file
                // if the task is in a terminal state so we can create the proper content change notifications.
                logger.debug("Unable to find logical file for transfer task source " + srcUrl + " in tenant " +
                        tenantId + ". Skipping " + transferTaskEventType.getEventName() +
                        " notifications for this target.");
            }
        } catch (RuntimeException e) {
            // this might cause a broken db connection to result in this worker draining the queue and throwing away all
            // the messages with no trace.
            logger.debug("Failed to process " + transferTaskEventType.getEventName() + " event for source target " +
                    srcUrl + " in tenant " + tenantId + ". This notification will be ignored");
        } catch (LogicalFileException e){
            logger.debug("Unable to retrieve logical file for with source target " + srcUrl + " in tenant " +
                    tenantId + ". This notification will be ignored");
        }
    }

    /**
     * Handles terminal events on destination {@link LogicalFile} of the transfer task. If the destination system does,
     * not exist we skip the logical file creation and event updates. Otherwise, we update the logical file and send
     * the appropriate update/overwritten event as appropriate. All exceptions are swallowed as this is a one-shot
     * delivery attempt and ordering matters.
     *
     * @param destUrl the destination url from the transfertask message body
     * @param username the owner of the transfertask message
     * @param transferTaskEventType the transfer task event in the message
     * @param tenantId the tenant code of the transfer and logical file
     */
    protected void updateDestinationLogicalFile(String destUrl, String username, TransferTaskEventType transferTaskEventType, String tenantId) {
        try {
            // if the logical file is null, then it still may have existed, but been deleted before this notification
            // was processed. In this situation, the notification would fail to resolve anyway, so there is nothing
            // further to do with respect to the sourceUrl. We still need to check for a destination logical file
            // if the task is in a terminal state so we can create the proper content change notifications.
            if (transferTaskEventType.isTerminal()) {
                LogicalFile destLogicalFile = lookupLogicalFileByUrl(destUrl, tenantId);
                if (destLogicalFile != null) {
                    destLogicalFile.addContentEvent(FileEventType.OVERWRITTEN, username);
                    persistLogicalFile(destLogicalFile);
                    logger.debug(String.format("Published %s event on logical file %s for transfer task source %s in tenant %s",
                            FileEventType.OVERWRITTEN.name(), destLogicalFile.getUuid(),
                            transferTaskEventType.getEventName(), tenantId));
                } else {
                    logger.debug("Unable to find logical file for transfer task destination " + destUrl +
                            " in tenant " + tenantId + ". Skipping " + transferTaskEventType.getEventName() +
                            " notifications for this target.");
                }
            }
        } catch (RuntimeException e) {
            // this might cause a broken db connection to result in this worker draining the queue and throwing away all
            // the messages with no trace.
            logger.debug("Failed to process " + transferTaskEventType.getEventName() + " event for source target " + destUrl + " in tenant " +
                    tenantId + ". This notification will be ignored");
        } catch (RemoteCredentialException | FileNotFoundException | RemoteDataException e) {
            logger.debug("Unable to retrieve logical file for with source target " + destUrl + " in tenant " +
                    tenantId + ". This notification will be ignored");
        }
    }

    /**
     * Mockable wrapper to fetch transfer task destination system by id and tenant. This check does not perform permission
     * check on the user's system access because we are processing events from things already completed. If they had
     * permission to do them at the time, then they should recieve the confirmation that it was done.
     *
     * @param systemId the {@link RemoteSystem#getSystemId()} to use as the lookup id
     * @param tenantId the tenant in which the systme should reside.
     * @return the matching system or null if no match is found.
     */
    protected RemoteSystem getSystemById(String systemId, String tenantId) {
        return new SystemDao().findBySystemIdAndTenant(systemId, tenantId);
    }

    /**
     * Mockable helper method to wrap the static call to {@link LogicalFileDao#persist(LogicalFile}}.
     * @param logicalFile the file to add or update
     */
    protected void persistLogicalFile(LogicalFile logicalFile){
        LogicalFileDao.persist(logicalFile);
    }

    /**
     * Mockable helper method to wrap the static call to  {@link LogicalFileDao#findBySourceUrl(String)}.
     * @param target a serialized URI representing a file item at the source or dest of a transfer
     * @param tenantId the id of the tenant for which to lookup the url
     * @return the logical file for the given url or null if not found
     * @see LogicalFileDao#findBySourceUrl(String)
     */
    protected LogicalFile lookupLogicalFileByUrl(String target, String tenantId) throws RemoteDataException, RemoteCredentialException, FileNotFoundException {
        TenancyHelper.setCurrentTenantId(tenantId);

        URI srcURI = URI.create(target);

        // if the src URI is a local file, then we are handling an event for an internal transfer and do not need
        // to create a notification as there would be no mechanism for a user to subscribe.
        if (srcURI.getScheme() == null || srcURI.getScheme().equalsIgnoreCase("file")) {
            logger.debug("Ignoring internal file " + target + "...");
            return null;
        } else {
            logger.debug("Finding remote system for logical file " + target + "...");
            RemoteSystem remoteSystem = getSystemById(srcURI.getHost(), tenantId);
            if (remoteSystem == null) {
                logger.debug("No matching system found for the destination of the transfer task. Skipping logical file update and events.");
                return null;
            } else {
                String resolvedPath = remoteSystem.getRemoteDataClient().resolvePath(srcURI.getPath());
                logger.debug("Finding logical file on system " + remoteSystem.getName() + " with path " + resolvedPath);
                return LogicalFileDao.findBySystemAndPath(remoteSystem, resolvedPath);
            }
        }
    }

    /**
     * Mockable helper method to wrap the static call to  {@link LogicalFileDao#findBySourceUrlAndTenant(String, String)}.
     * @param target a serialized URI representing a file item at the source or dest of a transfer
     * @param tenantId the id of the tenant for which to lookup the url
     * @return the logical file for the given url or null if not found
     * @see LogicalFileDao#findBySourceUrl(String)
     */
    protected LogicalFile lookupSourceLogicalFileByUrl(String target, String tenantId) throws LogicalFileException {
        TenancyHelper.setCurrentTenantId(tenantId);

        URI srcURI = URI.create(target);

        // if the src URI is a local file, then we are handling an event for an internal transfer and do not need
        // to create a notification as there would be no mechanism for a user to subscribe.
        if (srcURI.getScheme() == null || srcURI.getScheme().equalsIgnoreCase("file")) {
            logger.debug("Ignoring internal source file " + target + "...");
            return null;
        } else {
            logger.debug("Finding remote system for source logical file " + target + "...");
            RemoteSystem remoteSystem = getSystemById(srcURI.getHost(), tenantId);
            if (remoteSystem == null) {
                logger.debug("No matching system found for the source of the transfer task. Skipping logical file update and events.");
                return null;
            } else {
                logger.debug("Finding source logical file on system " + remoteSystem.getName() + " with path " + target);
                return LogicalFileDao.findBySourceUrlAndTenant(target, tenantId);
            }
        }
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

}
