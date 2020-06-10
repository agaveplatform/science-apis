package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.RemoteSystemAO;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.SystemRoleException;
import org.iplantc.service.systems.exceptions.SystemUnavailableException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.manager.SystemManager;
import org.iplantc.service.systems.manager.SystemRoleManager;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.systems.model.SystemRole;
import org.iplantc.service.systems.model.enumerations.RoleType;
import org.iplantc.service.systems.model.enumerations.SystemStatusType;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ASSIGNED;

public class TransferTaskCreatedListener extends AbstractTransferTaskListener {
    private static final Logger logger = LoggerFactory.getLogger(TransferTaskCreatedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_CREATED;

    public TransferTaskCreatedListener() {
        super();
    }

    public TransferTaskCreatedListener(Vertx vertx) {
        this(vertx, null);
    }

    public TransferTaskCreatedListener(Vertx vertx, String eventChannel) {
        super();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() {
        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            logger.info("Transfer task {} created: {} -> {}", uuid, source, dest);

            assignTransferTask(body, resp -> {});
        });

        // cancel tasks
        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} cancel detected", uuid);
            //this.interruptedTasks.add(uuid);
            processInterrupt("add", body);

        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
            processInterrupt("remove", body);
            //this.interruptedTasks.remove(uuid);
        });

        // paused tasks
        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} paused detected", uuid);
            processInterrupt("add", body);
        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
            processInterrupt("remove", body);
        });

    }

    /**
     * Validateas the source and dest in the transfer task and forwards the task to teh assigned event queue.
     * @param body the transfer task event body
     * @param handler callback to recieve a boolean response with the result of the assigned event processing.
     */
    public void assignTransferTask(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        String uuid = body.getString("uuid");
        String source = body.getString("source");
        String dest = body.getString("dest");
        String username = body.getString("owner");
        String tenantId = body.getString("tenantId");
        String protocol = "";
        TransferTask createdTransferTask = new TransferTask(body);

        try {
            URI srcUri;
            URI destUri;
            try {
                srcUri = URI.create(source);
                destUri = URI.create(dest);
            } catch (Exception e) {
                String msg = String.format("Unable to parse source uri %s for transfertask %s: %s",
                        source, uuid, e.getMessage());
                throw new RemoteDataSyntaxException(msg, e);
            }

            // ensure we can make the transfer based on protocol
            if (RemoteDataClientFactory.isSchemeSupported(srcUri)) {
                // look up the system to check permissions
                if (srcUri.getScheme().equalsIgnoreCase("agave")) {
                    // TODO: ensure user has access to the system. We may need to look up file permissions her as well.
                    //  though it's a lot easier to say this is an internal service and permissions will be checked and
                    //  invalidated from another process and propagated here via events.
                    if (!userHasMinimumRoleOnSystem(tenantId, username, srcUri.getHost(), RoleType.GUEST)) {
                        String message = String.format("User does not have sufficient permissions to access the " +
                                "source file item %s for transfer task %s.", source, uuid);
                        throw new PermissionException(message);
                    }
                }
            } else {
                throw new RemoteDataSyntaxException(String.format("Unknown source schema %s for the transfertask %s",
                                        destUri.getScheme(), uuid));
            }

            if (RemoteDataClientFactory.isSchemeSupported(destUri)) {
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
            } else {
                throw new RemoteDataSyntaxException(String.format("Unknown destination schema %s for the transfertask %s",
                        destUri.getScheme(), uuid));
            }


            // check for interrupted before proceeding
            if (taskIsNotInterrupted(createdTransferTask)) {
                // continue assigning the task and return
                logger.info("Assigning transfer task {} for processing.", uuid);
                _doPublishEvent(TRANSFERTASK_ASSIGNED, body);
                handler.handle(Future.succeededFuture(true));
            }
        } catch (Exception e) {
            logger.info(e.getMessage());
            JsonObject json = new JsonObject()
                    .put("cause", e.getClass().getName())
                    .put("message", e.getMessage())
                    .mergeIn(body);

            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
            handler.handle(Future.failedFuture(e));
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


}
