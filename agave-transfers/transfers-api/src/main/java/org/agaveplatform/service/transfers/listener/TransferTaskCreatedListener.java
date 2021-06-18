package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.RemoteSystemAO;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemRoleException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

public class TransferTaskCreatedListener extends AbstractNatsListener {
    private static final Logger log = LoggerFactory.getLogger(TransferTaskCreatedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_CREATED;
    private TransferTaskDatabaseService dbService;

    public TransferTaskCreatedListener() throws IOException, InterruptedException {
        super(null, null);
    }
    public TransferTaskCreatedListener(Vertx vertx) throws IOException, InterruptedException {
        super(vertx, null);
    }
    public TransferTaskCreatedListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() throws IOException, InterruptedException, TimeoutException {

        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);


        try {
            //group subscription so each message only processed by this vertical type once
            subscribeToSubjectGroup(EVENT_CHANNEL, this::handleCreatedMessage);
        } catch (Exception e) {
            log.error("TRANSFERTASK_CREATED - Exception {}", e.getMessage());
        }

        try {
            // broadcast subscription so each message gets to every verticle to cancel the task where ever it may be
            subscribeToSubjectGroup(TRANSFERTASK_CANCELED_SYNC, this::handleCanceledAckMessage);
        } catch (Exception e) {
            log.error("TRANSFERTASK_CANCELED_SYNC - Exception {}", e.getMessage());
        }

//        try {
//            // broadcast subscription so each message gets to every verticle to pause the task where ever it may be
//            subscribeToSubject(TRANSFERTASK_PAUSED_SYNC, this::handlePausedSyncMessage);
//        } catch (Exception e) {
//            log.error("TRANSFERTASK_CANCELED_SYNC - Exception {}", e.getMessage());
//        }
//
//        try {
//            // broadcast subscription so each message gets to every verticle to remove the task from list of cancelled tasks
//            subscribeToSubject(TRANSFERTASK_CANCELED_COMPLETED, this::handleCanceledCompletedMessage);
//        } catch (Exception e) {
//            log.error("TRANSFERTASK_CANCELED_COMPLETED - Exception {}", e.getMessage());
//        }
//
//        try {
//            // broadcast subscription so each message gets to every verticle to remove the task from list of paused tasks
//            subscribeToSubject(TRANSFERTASK_PAUSED_COMPLETED, this::handlePausedCompletedMessage);
//        } catch (Exception e) {
//            log.error("TRANSFERTASK_PAUSED_COMPLETED - Exception {}", e.getMessage());
//        }
    }

    /**
     * Wrapper to process paused completed message body and call {@link #processEvent(JsonObject, Handler)} to handle the pausing complete of the message.
     * @param message
     */
    protected void handlePausedCompletedMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);

        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Wrapper to process canceled completed message body and call {@link #processEvent(JsonObject, Handler)} to handle the canceled complete of the message.
     * @param message
     */
    protected void handleCanceledCompletedMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);

        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Wrapper to process canceled message body and call {@link #processEvent(JsonObject, Handler)} to handle the cancelation of the message.
     * @param message
     */
    protected void handleCanceledAckMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);

        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Wrapper to process paused message body and call {@link #processEvent(JsonObject, Handler)} to handle the pausing of the message.
     * @param message
     */
    protected void handlePausedSyncMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);

        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Wrapper to process message body and call {@link #processEvent(JsonObject, Handler)} to handle the intended behavior.
     * @param message
     */
    protected void handleCreatedMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);

            processEvent(body, resp -> {
                if (resp.succeeded()) {
                    log.debug("Succeeded with the processTransferTask in the assigning of the event {}", uuid);
                    // TODO: codify our notification behavior here. Do we rewrap? How do we ensure ordering? Do we just
                    //   throw it over the fence to Camel and forget about it? Boy, that would make things easier,
                    //   thought not likely faster.
                    // TODO: This seems like the correct pattern. Handler sent to the processing function, then
                    //   only send the notification on success. We can add a failure and error notification to the
                    //   respective listeners in the same way.
                    body.put("event", this.getClass().getName());
                    body.put("type", getEventChannel());
                    try {
                        Handler<AsyncResult<Boolean>> handle = null;
                        _doPublishEvent(MessageType.TRANSFERTASK_NOTIFICATION, body, handle);
                    } catch (Exception e) {
                        log.debug(e.getMessage());
                    }
                } else {
                    log.error(resp.cause().getMessage());
                }
            });
        } catch (InterruptedException|IOException e) {
            log.error(e.getMessage());
        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Validateas the source and dest in the transfer task and forwards the task to teh assigned event queue.
     * @param body the transfer task event body
     * @param handler callback to recieve a boolean response with the result of the assigned event processing.
     */
    public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException {
        String uuid = body.getString("uuid");
        String source = body.getString("source");
        String dest = body.getString("dest");
        String username = body.getString("owner");
        String tenantId = (body.getString("tenant_id"));

        TransferTask createdTransferTask = new TransferTask(body);

        try {
            URI srcUri;
            try {
                srcUri = URI.create(source);
            } catch (Exception e) {
                String msg = String.format("Unable to parse source uri %s for transfer task %s: %s",
                        source, uuid, e.getMessage());
                throw new RemoteDataSyntaxException(msg, e);
            }

            URI destUri;
            try {
                destUri = URI.create(dest);
            } catch (Exception e) {
                String msg = String.format("Unable to parse dest uri %s for transfer task %s: %s",
                        source, uuid, e.getMessage());
                throw new RemoteDataSyntaxException(msg, e);
            }

            // ensure we can make the transfer based on protocol
            if (uriSchemeIsNotSupported(srcUri)) {
                log.info("uriSchemeIsNotSupported {}", srcUri);
                throw new RemoteDataSyntaxException(String.format("Unknown source schema %s for the transfer task %s",
                        destUri.getScheme(), uuid));
            } else {
                // look up the system to check permissions
                log.info("got into the look up the system to check permissions");
                if (srcUri.getScheme().equalsIgnoreCase("agave")) {
                    // TODO: ensure user has access to the system. We may need to look up file permissions her as well.
                    //  though it's a lot easier to say this is an internal service and permissions will be checked and
                    //  invalidated from another process and propagated here via events.
                    if (!userHasMinimumRoleOnSystem(tenantId, username, srcUri.getHost(), RoleType.GUEST)) {
                        String message = String.format("User does not have sufficient permissions to access the " +
                                "source file item %s for transfer task %s.", source, uuid);
                        log.info(message);
                        throw new PermissionException(message);
                    }
                }
            }

            if (uriSchemeIsNotSupported(destUri)) {
                log.info("uri is not supported. {}", destUri);
                throw new RemoteDataSyntaxException(String.format("Unknown destination schema %s for the transfer task %s",
                        destUri.getScheme(), uuid));
            } else {
                log.info("uri is supported. {}", destUri);
                SystemDao systemDao = new SystemDao();
                if (destUri.getScheme().equalsIgnoreCase("agave")) {
                    // TODO: ensure user has access to the system. We may need to look up file permissions her as well.
                    //  though it's a lot easier to say this is an internal service and permissions will be checked and
                    //  invalidated from another process and propagated here via events.
                    if (!userHasMinimumRoleOnSystem(tenantId, username, destUri.getHost(), RoleType.USER)) {
                        String message = String.format("User does not have sufficient permissions to access the " +
                                "dest file item %s for transfer task %s.", dest, uuid);
                        throw new PermissionException(message);
                    }
                }
            }

            // check to be sure that the root task or parent task are not null first
            //if (createdTransferTask.getRootTaskId() != null && createdTransferTask.getParentTaskId() != null) {

            // if there are values for root task and parent task then do the following
            if (taskIsNotInterrupted(createdTransferTask)) {
                // update dt DB status here
                getDbService().updateStatus(tenantId, uuid, TransferStatusType.ASSIGNED.toString(), updateResult -> {
                    if (updateResult.succeeded()) {
                        // continue assigning the task and return
                        try {

                            String owner = updateResult.result().getString("owner");
                            String host = srcUri.getHost();
                            String subject = createPushMessageSubject(tenantId, owner, host, TRANSFERTASK_ASSIGNED);
//                            Handler<AsyncResult<Boolean>> handle = null;
                            _doPublishEvent(subject, updateResult.result(), handler);

                        } catch (Exception e) {
                            log.debug(e.getMessage());
                            handler.handle(Future.failedFuture(e));
                        }
                    } else {
                        // update failed
                        String msg = String.format("Error updating status of transfer task %s to ASSIGNED. %s",
                                uuid, updateResult.cause().getMessage());
                        doHandleError(updateResult.cause(), msg, body, handler);
                    }
                });
            } else {
                log.info("Skipping processing of child file items for transfer tasks in TransferTaskCreatedListener {} due to interrupt event.", uuid);
                String subject = createPushMessageSubject(tenantId, username, srcUri.getHost(), TRANSFERTASK_CANCELED_ACK);
                _doPublishEvent(subject, createdTransferTask.toJson(), resp -> {
                    handler.handle(Future.succeededFuture(false));
                });

            }
        } catch (Exception e) {
            log.error("Error with TransferTaskCreatedListener {}", e.getMessage());
            doHandleError(e, e.getMessage(), body, handler);
        }
    }

    /**
     * Checks whether user has access to the {@link RemoteSystem} with the given {@code systemId} in the {@code tenantId}
     * and with at least the {@code minimumRole}.
     * @param tenantId the id of the tenant containing the system
     * @param username the user to check for system access
     * @param systemId the system id of the {@link RemoteSystem}
     * @param minimumRole the minimum {@link RoleType} to check the user role against.
     * @return true if the {@code minimumRole} is less than or equal to the user's role on the system.
     * @throws SystemUnavailableException when the system status is down or has been disabled
     * @throws SystemUnknownException if no system with the given id in the given tenant exists
     * @see RemoteSystemAO#userHasMinimumRoleOnSystem(String, String, String, RoleType)
     */
    protected boolean userHasMinimumRoleOnSystem(String tenantId, String username, String systemId, RoleType minimumRole) throws SystemUnknownException, SystemRoleException, SystemUnavailableException {
        return getRemoteSystemAO().userHasMinimumRoleOnSystem(tenantId, username, systemId, minimumRole);
    }

    /**
     * Mockable getter for testing
     * @return a new instance of RemoteSystemAO
     */
    protected RemoteSystemAO getRemoteSystemAO() {
        return new RemoteSystemAO();
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }

}
