package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
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
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ASSIGNED;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELED_ACK;

public class TransferTaskCreatedListener extends AbstractNatsListener {
    private static final Logger log = LoggerFactory.getLogger(TransferTaskCreatedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_CREATED;
    private TransferTaskDatabaseService dbService;
//    private NatsJetstreamMessageClient natsClient; // = new NatsJetstreamMessageClient(NATS_URL, "DEV", _createConsumerName("DEV", "transfers", "tenantId","owner", "host", EVENT_CHANNEL));;
//    private static String streamName = "DEV";
//    private static String NATS_URL = "nats://nats:4222";


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

        DateTimeZone.setDefault(DateTimeZone.forID("America/Chicago"));
        TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"));

        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        try {
            String subject = createPushMessageSubject(streamName, "transfers", "tenantId", "owner", "host",  EVENT_CHANNEL);

            getMessageClient().push("DEV", subject, "Hello");

            Message message = getMessageClient().pop("DEV", subject);

            log.info(message.toString());

        } catch (Exception e) {
            log.debug("TRANSFERTASK_CREATED - Exception {}", e.getMessage());
            log.debug(e.getCause().toString());
        }

        //});
//
//        //**********************************************************************************
//        // Process the TRANSFERTASK_CANCELED_SYNC messages
//        //**********************************************************************************
//        // Build our subscription options. Durable is REQUIRED for pull based subscriptions
//        PullSubscribeOptions pullOptionsSync = PullSubscribeOptions.builder()
//                .durable("TRANSFERTASK_CANCELED_SYNC_Consumer")
//                .stream("TRANSFERTASK_CANCELED_SYNC")
//                .build();
//
//        try {
//            JetStreamSubscription sub = js.subscribe("TRANSFERTASK_CANCELED_SYNC", pullOptionsSync);
//            log.info("got subscription: {}", sub.getConsumerInfo().toString());
//
//            sub.pull(1);
//            Message m = sub.nextMessage(Duration.ofSeconds(1));
//            if (m != null) {
//                if (m.isJetStream()) {
//                    log.info(m.getData().toString());
//
//                    String response = new String(m.getData(), StandardCharsets.UTF_8);
//                    JsonObject body = new JsonObject(response);
//                    String uuid = body.getString("uuid");
//                    log.info("Transfer task {} cancel detected", uuid);
//                    if (uuid != null) {
//                        addCancelledTask(uuid);
//                    }
//                }
//            }
//        } catch (JetStreamApiException e) {
//            log.debug("TRANSFERTASK_CANCELED_SYNC - Error with subsription {}", e.getMessage());
//        }
//        getConnection().flush(Duration.ofMillis(500));
//
//
//
//        //**********************************************************************************
//        // Process the TRANSFERTASK_CANCELED_COMPLETED messages
//        //**********************************************************************************
//        // Build our subscription options. Durable is REQUIRED for pull based subscriptions
//        PullSubscribeOptions pullOptionsCanceled = PullSubscribeOptions.builder()
//                .durable("TRANSFERTASK_CANCELED_COMPLETED_Consumer")
//                .stream("TRANSFERTASK_CANCELED_COMPLETED")
//                .build();
//
//        try {
//            JetStreamSubscription sub = js.subscribe("TRANSFERTASK_CANCELED_COMPLETED", pullOptionsCanceled);
//            log.info("got subscription: {}", sub.getConsumerInfo().toString());
//
//            sub.pull(1);
//            Message m = sub.nextMessage(Duration.ofSeconds(1));
//            if (m != null) {
//                if (m.isJetStream()) {
//                    log.info(m.getData().toString());
//                    String response = new String(m.getData(), StandardCharsets.UTF_8);
//                    JsonObject body = new JsonObject(response);
//                    String uuid = body.getString("uuid");
//
//                    log.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
//                    if (uuid != null) {
//                        removeCancelledTask(uuid);
//                    }
//                }
//            }
//        } catch (JetStreamApiException e) {
//            log.debug("TRANSFERTASK_CANCELED_COMPLETED - Error with subsription {}", e.getMessage());
//        }
//        getConnection().flush(Duration.ofMillis(500));
//
//
//
//
//        //**********************************************************************************
//        // Process the TRANSFERTASK_CANCELED_PAUSED messages
//        //**********************************************************************************
//        // paused tasks
//        // Build our subscription options. Durable is REQUIRED for pull based subscriptions
//        PullSubscribeOptions pullOptionsPAUSED = PullSubscribeOptions.builder()
//                .durable("TRANSFERTASK_CANCELED_PAUSED_Consumer")
//                .stream("TRANSFERTASK_CANCELED_PAUSED")
//                .build();
//
//        try {
//            JetStreamSubscription sub = js.subscribe("TRANSFERTASK_CANCELED_PAUSED", pullOptionsPAUSED);
//            log.info("got subscription: {}", sub.getConsumerInfo().toString());
//
//            sub.pull(1);
//            Message m = sub.nextMessage(Duration.ofSeconds(1));
//            if (m != null) {
//                if (m.isJetStream()) {
//                    log.info(m.getData().toString());
//                    String response = new String(m.getData(), StandardCharsets.UTF_8);
//                    JsonObject body = new JsonObject(response);
//
//                    String uuid = body.getString("uuid");
//                    log.info("Transfer task {} paused detected", uuid);
//                    if (uuid != null) {
//                        addPausedTask(uuid);
//                    }
//                }
//            }
//        } catch (JetStreamApiException e) {
//            log.debug("TRANSFERTASK_CANCELED_PAUSED - Error with subsription {}", e.getMessage());
//        }
//        getConnection().flush(Duration.ofMillis(500));
//
//
//
//
//
//        // Build our subscription options. Durable is REQUIRED for pull based subscriptions
//        PullSubscribeOptions pullOptionsPausedCompleted = PullSubscribeOptions.builder()
//                .durable("TRANSFERTASK_PAUSED_COMPLETED_Consumer")
//                .stream("TRANSFERTASK_PAUSED_COMPLETED")
//                .build();
//
//        try {
//            JetStreamSubscription sub = js.subscribe("TRANSFERTASK_PAUSED_COMPLETED", pullOptionsPausedCompleted);
//            log.info("got subscription: {}", sub.getConsumerInfo().toString());
//
//            sub.pull(1);
//            Message m = sub.nextMessage(Duration.ofSeconds(1));
//            if (m != null) {
//                if (m.isJetStream()) {
//                    log.info(m.getData().toString());
//                    String response = new String(m.getData(), StandardCharsets.UTF_8);
//                    JsonObject body = new JsonObject(response);
//
//                    String uuid = body.getString("uuid");
//                    log.info("Transfer task {} paused detected", uuid);
//                    if (uuid != null) {
//                        addPausedTask(uuid);
//                    }
//                }
//            }
//        } catch (JetStreamApiException e) {
//            log.debug("TRANSFERTASK_PAUSED_COMPLETED - Error with subsription {}", e.getMessage());
//        }
//        getConnection().flush(Duration.ofMillis(500));
//
       // });
    }
//    public MessageHandler _MessageHandler(JetStreamManagement jsm, int count) {
//        CountDownLatch msgLatch = new CountDownLatch(count);
//        AtomicInteger received = new AtomicInteger();
//        AtomicInteger ignored = new AtomicInteger();
//
//        /** Handler method called when a message arrives from a transfertask created channel.
//         * @param msg The message that arrived
//         */
//        MessageHandler handler = msg -> {
//            if (msgLatch.getCount() == 0) {
//                ignored.incrementAndGet();
//                if (msg.isJetStream()) {
//                    log.info("Message Ignored, latch count already reached "
//                            + new String(msg.getData(), StandardCharsets.UTF_8));
//                    msg.nak();
//                }
//            } else {
//                received.incrementAndGet();
//                String response = new  String(msg.getData(), StandardCharsets.UTF_8);
//                log.info("  Subject: {}  Data: {}", msg.getSubject(), response);
//                JsonObject body = new JsonObject(response);
//                try {
//                    processEvent(body, resp -> {
//                        if (resp.succeeded()) {
//                            msg.ack();
//                        }else{
//                            msg.nak();
//                        }
//                    });
//                } catch (IOException e) {
//                    log.debug(e.getMessage());
//                }
//                msgLatch.countDown();
//            }
//        };
//        return handler;
//    }
//
//    public void tt_Created(Message m, Handler<AsyncResult<Boolean>> handler){
//        if (m.isJetStream()) {
//            log.info(Arrays.toString(m.getData()));
//
//            String response = new String(m.getData(), StandardCharsets.UTF_8);
//            JsonObject body = new JsonObject(response);
//            String uuid = body.getString("uuid");
//            String source = body.getString("source");
//            String dest = body.getString("dest");
//            String tenantId = body.getString("tenantId");
//            String owner = body.getString("owner");
//            URI srcUri = URI.create(body.getString("source"));
//            log.info("Transfer task {} created: {} -> {}", uuid, source, dest);
//
//            try {
//                processEvent(body, resp -> {
//                    if (resp.succeeded()) {
//                        log.info("Succeeded with the processing transfer created event for transfer task {}", uuid);
//                        body.put("event", this.getClass().getName());
//                        body.put("type", getEventChannel());
//                        try {
//                            String messageName = _createConsumerName("DEV", "transfers", tenantId, owner, srcUri.getHost().toString(),MessageType.TRANSFERTASK_NOTIFICATION);
//                            natsClient.setConsumerName(messageName);
//                            natsClient.push("DEV", messageName, resp.result().toString());
//
//                        } catch (Exception e) {
//                            log.debug(e.getMessage());
//                        }
//
//                        handler.handle(Future.succeededFuture(true));
//                    } else {
//                        log.error("Error with return from creating the event {}", uuid);
//                        try {
//                            String messageName = _createConsumerName("DEV", "transfers", tenantId, owner, srcUri.getHost().toString(),MessageType.TRANSFERTASK_ERROR);
//                            natsClient.setConsumerName(messageName);
//                            natsClient.push("DEV", messageName, resp.result().toString());
//                        } catch (Exception e) {
//                            log.debug(e.getMessage());
//                        }
//
//                        handler.handle(Future.succeededFuture(false));
//                    }
//                });
//            } catch (Exception ex) {
//                log.error("Error with the TRANSFERTASK_CREATED message.  The error is {}", ex.getMessage());
//                try {
//                    //_doPublishNatsJSEvent( MessageType.TRANSFERTASK_ERROR, body);
//                    String messageName = _createConsumerName("DEV", "transfers", tenantId, owner, srcUri.getHost().toString(),MessageType.TRANSFERTASK_ERROR);
//                    natsClient.setConsumerName(messageName);
//                    natsClient.push("DEV", messageName, body.toString());
//                } catch (Exception e) {
//                    log.debug(e.getMessage());
//                }
//            }
//        } else {
//            //m.getData();
//            log.info("TRANSFERTASK_CREATED Subject: {}", m.getSubject());
//            log.info("TRANSFERTASK_CREATED Data:  {}", m.getData());
//        }
//
//    }

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
                        try {
                            String owner = updateResult.result().getString("owner");
                            String host = srcUri.getHost();
                            //_doPublishNatsJSEvent(TRANSFERTASK_ASSIGNED, updateResult.result());
                            String subject = createPushMessageSubject("DEV", "transfers", tenantId, owner, host, TRANSFERTASK_ASSIGNED);
                            getMessageClient().push(streamName, subject, updateResult.result().toString());
                        } catch (Exception e) {
                            log.debug(e.getMessage());
                        }
                        handler.handle(Future.succeededFuture(true));
                    } else {
                        // update failed
                        String msg = String.format("Error updating status of transfer task %s to ASSIGNED. %s",
                                uuid, updateResult.cause().getMessage());
                        log.error(msg);
                        try {
                            doHandleError(updateResult.cause(), msg, body, handler);
                        } catch (IOException | InterruptedException e) {
                            log.debug(e.getMessage());
                            log.debug(e.getCause().toString());
                        }
                    }
                });
            } else {
                log.info("Skipping processing of child file items for transfer tasks in TransferTaskCreatedListener {} due to interrupt event.", uuid);
                String subject = createPushMessageSubject("DEV","transfers", tenantId, username, srcUri.getHost(), TRANSFERTASK_CANCELED_ACK);
                getMessageClient().push(streamName, subject, createdTransferTask.toString());
                handler.handle(Future.succeededFuture(false));
            }
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
