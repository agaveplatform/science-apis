package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELED_SYNC;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.CANCELING_WAITING;

public class TransferTaskDeletedListener extends AbstractTransferTaskListener {
    private static final Logger logger = LoggerFactory.getLogger(TransferTaskDeletedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_DELETED;

    private TransferTaskDatabaseService dbService;
    private List<TransferTask> ttTree = new ArrayList<TransferTask>();

    public TransferTaskDeletedListener() {
        super();
    }
    public TransferTaskDeletedListener(Vertx vertx) {
        super(vertx);
    }
    public TransferTaskDeletedListener(Vertx vertx, String eventChannel) {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() {
        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");
            logger.info("Transfer task {} cancel detected.", uuid);
            this.processDeletedRequest(body, result -> {
                //result should be true
            });
        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_DELETED_ACK, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} ackowledged cancellation", uuid);
            //this.processDeletedAck(body, result -> {});
        });
    }

    protected void processDeletedRequest(JsonObject body, Handler<AsyncResult<Boolean>> resultHandler) {
        String tenantId = body.getString("tenantId");
        String uuid = body.getString("uuid");
        String status = body.getString("status");
        String parentTaskId = body.getString("parentTask");

        Long id = 0L;


        // update target transfer task DB status here
        // we can't just update the deleted status here because we would force toggle the status from a terminal state
        // to a pending state that could never get resolved if the task were already deleted. We have to lookup the
        // status first, then we can make the update
        getDbService().getById(tenantId, uuid, getByIdReply -> {
            if (getByIdReply.succeeded()) {
                logger.trace("Retrieved transfer task {}: {}", uuid, getByIdReply.result());
                TransferTask targetTransferTask = new TransferTask(getByIdReply.result());
                logger.info("Transfer task {} has root id {}", uuid, targetTransferTask.getRootTaskId());

                if (targetTransferTask.getStatus().isActive()) {
                    // push the event transfer task onto the queue. this will cause all listening verticals
                    // actively processing any of its children to cancel their existing work and ack.
                    // the ack responses will bubble back up, eventually reaching the root, at which time,
                    // the root transfer task will be marked as DELETED.
//                    logger.debug("Updating status of transfer task {} to {} prior to sending cancel sync event.",
//                            uuid, DELETED_WAITING.name());
                    getDbService().updateStatus(tenantId, uuid, CANCELING_WAITING.name(), updateReply -> {
                        if (updateReply.succeeded()) {

                            logger.debug(String.format("Successfully updated the status of transfer task %s to %s prior " +
                                            "to sending %s event.",
                                    uuid, CANCELING_WAITING.name(), TRANSFERTASK_CANCELED_SYNC));
                            logger.debug("Sending cancel sync event for transfer task {} to signal children to cancel any active work.", uuid);
                            getVertx().eventBus().publish(TRANSFERTASK_CANCELED_SYNC, updateReply.result());
                            resultHandler.handle(Future.succeededFuture(true));
                        } else {
                            String msg = String.format("Unable to update the status of transfer task %s to %s prior " +
                                            "to sending %s event. No sync event will be sent.",
                                    uuid, CANCELING_WAITING.name(), TRANSFERTASK_CANCELED_SYNC);
                            doHandleError(updateReply.cause(), msg, body, resultHandler);
                        }
                    });
                }
            } else {
                // failure
                String msg = "Unable to verify the current status of transfer task " + uuid + ". " + getByIdReply.cause();
                doHandleError(getByIdReply.cause(), msg, body, resultHandler);
            }

        });
    }

//            /**
//             * Processes the {@link MessageType#TRANSFERTASK_CANCELED_ACK} event for a {@link TransferTask} marshalled to
//             * the {@code body} argument. If the task is not a root transfer task itself, it will call {@link #processParentAck(String, String, Handler)}
//             * to see if any of its siblings are currently  active. If not, a {@link MessageType#TRANSFERTASK_CANCELED_ACK} will
//             * be sent for the parent. Regardless of the parent check, a {@link MessageType#TRANSFERTASK_CANCELED_COMPLETED} event
//             * will be raised after the task status is updated in the db.
//             *
//             * @param body the {@link TransferTask} marshalled as json
//             * @param resultHandler the handler to resolve with {@link Boolean#TRUE} if no parent task or a {@link MessageType#TRANSFERTASK_CANCELED_COMPLETED}
//             *                      event was sent;
//             *                      a {@link Boolean#FALSE} if the check succeeded, but the parent processing failed,
//             *                      ;failed if the parent could not be checked for any reason.
//             * @see #processParentAck(String, String, Handler)
//             */
//            protected void processCancelAck(JsonObject body, Handler<AsyncResult<Boolean>> resultHandler) {
//                String parentTaskId = body.getString("parentTask");
//                String uuid = body.getString("uuid");
//                String tenantId = body.getString("tenantId");
//
//                logger.trace("getting into allChildren");
//
//
//


    /**
     * Fetches a {@link TransferTask} by id, logging any exception that comes up.
     * @deprecated
     * @param tenantId tenant in which the transfer task should exist
     * @param uuid uuid of the transfer task
     * @param resultHandler callback to pass the returned transfertask
     */
    protected void getTransferTask(String tenantId, String uuid, Handler<AsyncResult<TransferTask>> resultHandler) {
        getDbService().getById(tenantId, uuid, getTaskById -> {
            if (getTaskById.succeeded()) {
                TransferTask tt = new TransferTask(getTaskById.result());
                resultHandler.handle(Future.succeededFuture(tt));
            } else {
                logger.info("Failed to lookup transfer task {}", uuid);
                resultHandler.handle(Future.failedFuture(getTaskById.cause()));
            }
        });
    }
    public TransferTaskDatabaseService getDbService() {return dbService;}

    public void setDbService(TransferTaskDatabaseService dbService) {this.dbService = dbService;}

}
