package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.RemoteSystemAO;
import org.iplantc.service.common.exceptions.PermissionException;
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
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.FLUSH_DELAY_NATS;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ASSIGNED;

public class TransferTaskCreatedListener extends AbstractNatsListener {
    private static final Logger log = LoggerFactory.getLogger(TransferTaskCreatedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_CREATED;

    private TransferTaskDatabaseService dbService;

    public TransferTaskCreatedListener() {
        super();
    }
    public TransferTaskCreatedListener(Vertx vertx) {super(vertx); }
    public TransferTaskCreatedListener(Vertx vertx, String eventChannel) {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() throws IOException, InterruptedException, TimeoutException {
        //EventBus bus = vertx.eventBus();

        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        Connection nc = _connect();
        Dispatcher d = nc.createDispatcher((msg) -> {});
        //bus.<JsonObject>consumer(getEventChannel(), msg -> {
        Subscription s = d.subscribe(EVENT_CHANNEL, msg -> {
            //msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");

            log.info("Transfer task {} created: {} -> {}", uuid, source, dest);

            try {
                processEvent(body, resp -> {
                    if (resp.succeeded()) {
                        log.info("Succeeded with the processing transfer created event for transfer task {}", uuid);
                        body.put("event", this.getClass().getName());
                        body.put("type", getEventChannel());
                        _doPublishEvent(MessageType.TRANSFERTASK_NOTIFICATION, body);
                    } else {
                        log.error("Error with return from creating the event {}", uuid);
                        _doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
                    }
                });
            } catch (Exception e){
                log.error("Error with the TRANSFERTASK_CREATED message.  The error is {}", e.getMessage());
                _doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
            }
        });
        d.subscribe(EVENT_CHANNEL);
        nc.flush(Duration.ofMillis(config().getInteger(String.valueOf(FLUSH_DELAY_NATS))));

        // cancel tasks
        //bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
        s = d.subscribe(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} cancel detected", uuid);
            if (uuid != null) {
                addCancelledTask(uuid);
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_CANCELED_SYNC);
        nc.flush(Duration.ofMillis(config().getInteger(String.valueOf(FLUSH_DELAY_NATS))));

        //bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
        s = d.subscribe(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");

            log.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
            if (uuid != null) {
                removeCancelledTask(uuid);
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_CANCELED_COMPLETED);
        nc.flush(Duration.ofMillis(config().getInteger(String.valueOf(FLUSH_DELAY_NATS))));

        // paused tasks
        //bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
        s = d.subscribe(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");

            log.info("Transfer task {} paused detected", uuid);
            if (uuid != null) {
                addPausedTask(uuid);
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_PAUSED_SYNC);
        nc.flush(Duration.ofMillis(config().getInteger(String.valueOf(FLUSH_DELAY_NATS))));

        //bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
        s = d.subscribe(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");

            log.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
            if (uuid != null) {
                addPausedTask(uuid);
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_PAUSED_COMPLETED);
        nc.flush(Duration.ofMillis(config().getInteger(String.valueOf(FLUSH_DELAY_NATS))));

    }

    /**
     * Validateas the source and dest in the transfer task and forwards the task to teh assigned event queue.
     * @param body the transfer task event body
     * @param handler callback to recieve a boolean response with the result of the assigned event processing.
     */
    public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        String uuid = body.getString("uuid");
        String source = body.getString("source");
        String dest = body.getString("dest");
        String username = body.getString("owner");
        String tenantId = (body.getString("tenant_id"));
        String protocol = "";
        TransferTask createdTransferTask = new TransferTask(body);

        try {
            URI srcUri;
            URI destUri;
            try {
                log.info("got into TransferTaskCreatedListener.processEvent");
                srcUri = URI.create(source);
                destUri = URI.create(dest);
            } catch (Exception e) {
                String msg = String.format("Unable to parse source uri %s for transfer task %s: %s",
                        source, uuid, e.getMessage());
                log.error(msg);
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

            // check for interrupted before proceeding
            log.info("checking for interrupted before proceeding");
            log.info(" rootTaskID = {}", createdTransferTask.getRootTaskId() );
            log.info(" parentTaskID = {}", createdTransferTask.getParentTaskId() );

            // check to be sure that the root task or parent task are not null first
            //if (createdTransferTask.getRootTaskId() != null && createdTransferTask.getParentTaskId() != null) {
                log.trace("Got past the rootTaskID and parentTaskID");
                // if there are values for root task and parent task then do the following
                if (taskIsNotInterrupted(createdTransferTask)) {
                    // update dt DB status here
                    log.info("set status to ASSIGNED");
                    getDbService().updateStatus(tenantId, uuid, TransferStatusType.ASSIGNED.toString(), updateResult -> {
                        if (updateResult.succeeded()) {
                            // continue assigning the task and return
                            log.info("Assigning transfer task {} for processing.", uuid);
                            _doPublishEvent(TRANSFERTASK_ASSIGNED, updateResult.result());
                            handler.handle(Future.succeededFuture(true));
                        } else {
                            // update failed
                            String msg = String.format("Error updating status of transfer task %s to ASSIGNED. %s",
                                    uuid, updateResult.cause().getMessage());
                            log.error(msg);
                            doHandleError(updateResult.cause(), msg, body, handler);
                        }
                    });
                } else {
                    log.info("Skipping processing of child file items for transfer tasks in TransferTaskCreatedListener {} due to interrupt event.", uuid);
                    _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, body);
                    handler.handle(Future.succeededFuture(false));
                }
//            } else {
//                log.info("Error. Root and parent tasks are null.");
//            }
        } catch (Exception e) {
            log.error("Error with TransferTaskCreatedListener {}", e.toString());
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
