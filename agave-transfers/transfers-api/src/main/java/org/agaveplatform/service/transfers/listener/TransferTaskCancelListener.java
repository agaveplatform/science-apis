package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.exception.TransferException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELED_SYNC;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELED;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferTaskCancelListener extends AbstractTransferTaskListener {
    private static final Logger logger = LoggerFactory.getLogger(TransferTaskCancelListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_CANCELED;

    private TransferTaskDatabaseService dbService;
    private List<TransferTask> ttTree = new ArrayList<TransferTask>();

    public TransferTaskCancelListener() {
        super();
    }
    public TransferTaskCancelListener(Vertx vertx) {
        super(vertx);
    }
    public TransferTaskCancelListener(Vertx vertx, String eventChannel) {
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
            msg.reply(TransferTaskCancelListener.class.getName() + " received.");

            JsonObject body = msg.body();
            String uuid = body.getString("uuid");
            logger.info("Transfer task {} cancel detected.", uuid);
            this.processCancelRequest(body, result -> {
                //result should be true
            });
        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_ACK, msg -> {
            msg.reply(TransferTaskCancelListener.class.getName() + " received.");

            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            msg.reply("ok");

            logger.info("Transfer task {} ackowledged cancellation", uuid);
            this.processCancelAck(body, result -> {});
        });
    }

    protected void processCancelRequest(JsonObject body, Handler<AsyncResult<Boolean>> resultHandler) {
        String tenantId = body.getString("tenant_id");
        String uuid = body.getString("uuid");
        String status = body.getString("status");
        String parentTaskId = body.getString("parentTask");

        Long id = 0L;

        // update target transfer task DB status here
        // we can't just update the cancelled status here because we would force toggle the status from a terminal state
        // to a pending state that could never get resolved if the task were already cancelled. We have to lookup the
        // status first, then we can make the update
        getDbService().getById(tenantId, uuid, getByIdReply -> {
            if (getByIdReply.succeeded()) {
                logger.trace("Retrieved transfer task {}: {}", uuid, getByIdReply.result());
                TransferTask targetTransferTask = new TransferTask(getByIdReply.result());
                logger.info("Transfer task {} has root id {}", uuid, targetTransferTask.getRootTaskId());

                // if it's not already in a terminal state, process the cancell requrest
                if (StringUtils.isNotBlank(targetTransferTask.getRootTaskId()) ||
                        StringUtils.isNotBlank(targetTransferTask.getParentTaskId())) {
                    logger.info("The root id is {} and the parentID is {}",targetTransferTask.getRootTaskId(), targetTransferTask.getParentTaskId());
                    // TODO: think through cancel requests that are not a root tasks. This should never be able
                    //   to happen because the request should have been validated at whatever process created the
                    //   cancel request (probably the API vertical).
                    // failure
                    JsonObject json = new JsonObject()
                            .put("cause", TransferException.class.getName())
                            .put("message", "Cannot cancel non-root transfer tasks.")
                            .mergeIn(body);

                    _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                    resultHandler.handle(Future.succeededFuture(false));
                } else if (targetTransferTask.getStatus().isActive()) {
                    // push the event transfer task onto the queue. this will cause all listening verticals
                    // actively processing any of its children to cancel their existing work and ack.
                    // the ack responses will bubble back up, eventually reaching the root, at which time,
                    // the root transfer task will be marked as cancelled.
                    logger.debug("Updating status of transfer task {} to {} prior to sending cancel sync event.",
                            uuid, CANCELING_WAITING.name());
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
                } else {
                    // if the root is not in an active state, then there is nothing needed at this time. we
                    // choose to send the sync event anyway so we can ensure that any subtasks still running will
                    // be cleaned up. This is a bit of an aggressive way to override eventual consistency.
                    logger.info("Transfer task {} is not in an active state and will not be updated.", uuid);
                    logger.debug("Sending cancel sync event for transfer task {} to ensure children are cleaned up.", uuid);
                    getVertx().eventBus().publish(TRANSFERTASK_CANCELED_SYNC, getByIdReply.result());
                    resultHandler.handle(Future.succeededFuture(false));
                }
            } else {
                // failure
                String msg = "Unable to verify the current status of transfer task " + uuid + ". " + getByIdReply.cause();
                doHandleError(getByIdReply.cause(), msg, body, resultHandler);
            }
        });
    }

    /**
     * Processes the {@link MessageType#TRANSFERTASK_CANCELED_ACK} event for a {@link TransferTask} marshalled to
     * the {@code body} argument. If the task is not a root transfer task itself, it will call {@link #processParentAck(String, String, Handler)}
     * to see if any of its siblings are currently  active. If not, a {@link MessageType#TRANSFERTASK_CANCELED_ACK} will
     * be sent for the parent. Regardless of the parent check, a {@link MessageType#TRANSFERTASK_CANCELED_COMPLETED} event
     * will be raised after the task status is updated in the db.
     *
     * @param body the {@link TransferTask} marshalled as json
     * @param resultHandler the handler to resolve with {@link Boolean#TRUE} if no parent task or a {@link MessageType#TRANSFERTASK_CANCELED_COMPLETED}
     *                      event was sent;
     *                      a {@link Boolean#FALSE} if the check succeeded, but the parent processing failed,
     *                      ;failed if the parent could not be checked for any reason.
     * @see #processParentAck(String, String, Handler)
     */
    protected void processCancelAck(JsonObject body, Handler<AsyncResult<Boolean>> resultHandler) {
        String parentTaskId = body.getString("parentTask");
        String uuid = body.getString("uuid");
        String tenantId = body.getString("tenant_id");

        logger.trace("getting into allChildren");
        // if this task has children and all are cancelled or completed, then we can
        // mark this task as cancelled.
        getDbService().allChildrenCancelledOrCompleted(tenantId, uuid, allChildrenCancelledOrCompletedResp -> {
            logger.trace("check allChildren");
            if (allChildrenCancelledOrCompletedResp.succeeded()) {
                // update the status of the transfertask since the verticle handling it has
                // reported back that it completed the cancel request since this tree is
                // done cancelling
                logger.info("processing the setTransferTaskCancelWhereNotCompleted next");
                //resultHandler.handle(Future.succeededFuture(true));
                getDbService().setTransferTaskCanceledWhereNotCompleted(tenantId, uuid, setTransferTaskCanceledIfNotCompletedResult -> {
                    if (setTransferTaskCanceledIfNotCompletedResult.succeeded()) {
                        logger.info("DB updated with status of CANCELED uuid = {}", uuid);

                        // this task and all its children are done, so we can send a complete event
                        // to safely clear out the uuid from all listener verticals' caches
                        _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_COMPLETED, body);

                        // we can now also check the parent, if present, for completion of its tree.
                        // if the parent is empty, the root will be as well. For children of the root
                        // transfer task, the root == parent
                        if (StringUtils.isNotEmpty(parentTaskId)) {
                            logger.debug("Checking parent task {} for completed transfer task {}.", parentTaskId, uuid);
                            processParentAck(tenantId, parentTaskId, processParentResp -> {
                                if (processParentResp.succeeded()) {
                                    logger.trace("Completed parent check for parent transfer task {} after cancel " +
                                            "ack of transfer task {}.", parentTaskId, uuid);
                                    resultHandler.handle(Future.succeededFuture(processParentResp.result()));
                                } else {
                                    String msg = String.format("Unable to process parent cancel ack for transfer task " +
                                            "%s after completing the cancel ack for child task %s: %s",
                                            parentTaskId, uuid, processParentResp.cause().getMessage());
                                    JsonObject json = new JsonObject()
                                            .put("cause", processParentResp.cause().getClass().getName())
                                            .put("message", msg)
                                            .mergeIn(body);

                                    _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
                                    resultHandler.handle(Future.succeededFuture(false));
                                }
                            });
                        } else {
                            logger.info("Skipping parent proccess complete for transfer task {}", uuid);
                            resultHandler.handle(Future.succeededFuture(true));
                        }
                    } else {
                        // update failed on current task
                        String msg = String.format("Unable to update the status of transfer task %s to cancelled. %s",
                                uuid, setTransferTaskCanceledIfNotCompletedResult.cause().getMessage());
                        logger.error(msg);
                        JsonObject json = new JsonObject()
                                .put("cause", setTransferTaskCanceledIfNotCompletedResult.cause().getClass().getName())
                                .put("message", msg)
                                .mergeIn(body);

                        _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                        resultHandler.handle(Future.succeededFuture(false));
                    }
                });
            } else {
                String msg = String.format("Unable to check the status of transfer task %s children. " +
                        "No acknowledgement will be sent. %s", uuid,
                        allChildrenCancelledOrCompletedResp.cause().getMessage());
                logger.error(msg);
                JsonObject json = new JsonObject()
                        .put("cause", allChildrenCancelledOrCompletedResp.cause().getClass().getName())
                        .put("message", msg)
                        .mergeIn(body);

                _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                resultHandler.handle(Future.succeededFuture(false));
            }
        });
    }

    /**
     * Checks on the status of the parent of the task that received the {@link MessageType#TRANSFERTASK_CANCELED_ACK}
     * event. If not currently active or, if active and all its children are in a non-active state, a
     * {@link MessageType#TRANSFERTASK_CANCELED_ACK} event is raised for the parent. Otherwise, errors are propagated
     * back up to the handler for exception handling.
     *
     * @param tenantId the tenant of the parent {@link TransferTask}
     * @param parentTaskId the id of the parent {@link TransferTask}
     * @param resultHandler the handler to resolve with {@link Boolean#TRUE} if a {@link MessageType#TRANSFERTASK_CANCELED_ACK}
     *                      was sent, {@link Boolean#FALSE} if the check succeeded, but the parent was not ready to have
     *                      an {@link MessageType#TRANSFERTASK_CANCELED_ACK} sent, or failed if the parent could not be
     *                      checked for any reason.
     */
    protected void processParentAck(String tenantId, String parentTaskId, Handler<AsyncResult<Boolean>> resultHandler) {
        getTransferTask(tenantId, parentTaskId, ttResult ->{
            if (ttResult.succeeded()) {
                TransferTask parentTask = ttResult.result();
                // if the parent is still active
                if (TransferStatusType.getActive().contains(parentTask.getStatus())) {
                    // check to see if it has active children
                    getDbService().allChildrenCancelledOrCompleted(tenantId, parentTaskId, parentChildCancelledOrCompleteResult -> {
                        if (parentChildCancelledOrCompleteResult.succeeded()) {
                            // if it does have active children, then we skip the ack. The parent will be checked again as each
                            // child completes
                            if (parentChildCancelledOrCompleteResult.result()) {
                                resultHandler.handle(Future.succeededFuture(false));
                            } else {
                                // parent has children, but all are now cancelled or completed, so send the parent ack
                                _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, parentTask.toJson());
                                resultHandler.handle(Future.succeededFuture(true));
                            }
                        } else {
                            // failed to query children statuses
                            resultHandler.handle(Future.failedFuture(parentChildCancelledOrCompleteResult.cause()));
                        }
                    });
                } else {
                    // the parent is not active. forward the ack anyway, just to ensure all ancestors get the message
                    _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, parentTask.toJson());
                    resultHandler.handle(Future.succeededFuture(false));
                }
            } else {
                resultHandler.handle(Future.failedFuture(ttResult.cause()));
            }
        });
    }


    /**
     * Handles processing of parent task to see if any other children are active. If not, we create a new
     * transfer.pause event for the parent. This allows us to ensure that when the last child completes,
     * it will propagate back up the transfer task tree to the root task, which is the parent with no parent,
     * and mark that as completed. This ensures all tasks will be marked as completed upon completion or
     * cancellation or failure.
     * @param tenantId the tenant of the transfertask
     * @param parentTaskId the id of the parent
     * @param resultHandler the handler to call with a boolean value indicating whether the parent event was found to be incomplete and needed to have a transfer.complete event created.
     */
    protected void processParentEvent(String tenantId, String parentTaskId, Handler<AsyncResult<Boolean>> resultHandler) {

		Promise<Boolean> promise = Promise.promise();
        // lookup parent transfertask
        logger.info("Got to the processParentEvent.");
        getDbService().getById(tenantId, parentTaskId, getTaskById -> {
            if (getTaskById.succeeded()) {
                //JsonObject tta = getTaskById.result();
                // check whether it's active or not by its status
                TransferTask parentTask = new TransferTask(getTaskById.result());
                if ( ! parentTask.getStatus().toString().isEmpty() &&
                        ! List.of(CANCELLED, COMPLETED, FAILED).contains(parentTask.getStatus())) {

                    // if the parent is active, check for any children still active to see if it needs to be notified
                    // that all children have completed.
                    getDbService().allChildrenCancelledOrCompleted(tenantId, parentTaskId, isAllChildrenCancelledOrCompletedResult -> {
                        if (isAllChildrenCancelledOrCompletedResult.succeeded()) {
                            // if all children are completed or cancelled, the parent is completed. create that event
                            if (isAllChildrenCancelledOrCompletedResult.result()) {
                                //logger.debug("All child tasks for parent transfer task {} are paused, cancelled or completed. " +
                                //		"A transfer.paused event will be created for this task.", parentTaskId);
                                // call to our publishing helper for easier testing.
                                _doPublishEvent(TRANSFERTASK_CANCELED, getTaskById.result());
                            } else {
                                //logger.debug("Parent transfer task {} has active children. " +
                                //		"Skipping further processing ", parentTaskId);
                                // parent has active children. let it run
                            }

                            // return true indicating the parent event was processed
                            resultHandler.handle(isAllChildrenCancelledOrCompletedResult);
                        } else {
                            //logger.debug("Failed to look up children for parent transfer task {}. " +
                            //		"Skipping further processing ", parentTaskId);
                            // failed to look up child tasks. processing of the parent failed. we
                            // forward on the exception to the handler
                            resultHandler.handle(Future.failedFuture(isAllChildrenCancelledOrCompletedResult.cause()));
                        }
                    });
                } else {
                    //logger.debug("Parent transfer task {} is already in a terminal state. " +
                    //		"Skipping further processing ", parentTaskId);
                    // parent is already terminal. let it lay
                    // return true indicating the parent event was processed
                    resultHandler.handle(Future.succeededFuture(false));
                }
            } else {
//				promise.fail(getTaskById.cause());
                //logger.error("Failed to lookup parent transfer task {}: {}", parentTaskId, getTaskById.cause().getMessage());
                resultHandler.handle(Future.failedFuture(getTaskById.cause()));
            }
        });
//		return promise;
    }

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

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }

}
