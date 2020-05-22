package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.apache.commons.lang3.StringUtils;

import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELLED;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_PAUSED;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferTaskCancelListener extends AbstractTransferTaskListener {
    private static final Logger logger = LoggerFactory.getLogger(TransferTaskCancelListener.class);

    public TransferTaskCancelListener(Vertx vertx) {
        super(vertx);
    }

    public TransferTaskCancelListener(Vertx vertx, String eventChannel) {
        super(vertx, eventChannel);
    }

    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_CANCELLED;

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }
    private TransferTaskDatabaseService dbService;
    private List<TransferTask> ttTree = new ArrayList<TransferTask>();

    @Override
    public void start() {
        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");
            logger.info("Transfer task {} cancel detected.", uuid);
            this.processCancelRequest(body, result -> {
            //result should be true
            });
        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_ACK, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} ackowledged cancellation", uuid);
            this.processCancelAck(body, result -> {

            });
        });
    }

    protected void processCancelRequest(JsonObject body, Handler<AsyncResult<Boolean>> resultHandler) {
       // Promise<Boolean> promise = Promise.promise();

        String tenantId = body.getString("tenantId");
        String uuid = body.getString("uuid");
        String status = body.getString("status");
        String parentTaskId = body.getString("parentTaskId");

        Long id = 0L;

            // lookup the transfer task from the db
            getDbService().getById(tenantId, uuid, gTTResult -> {
                //try {
                    if (gTTResult.succeeded()) {
                        logger.info("got transfer task uuid {}", uuid);
                        TransferTask tt = new TransferTask(gTTResult.result());
                        logger.info("The root id is {}", tt.getRootTaskId());
                        // if it's not already in a terminal state, process the cancell requrest
                        if (! StringUtils.isEmpty(tt.getRootTaskId()) || !StringUtils.isEmpty(tt.getParentTaskId())) {
                            logger.info("The root id is {} and the parentID is {}",tt.getRootTaskId(), tt.getParentTaskId());
                            // TODO: think through cancel requests that are not on root tasks. This should never be able
                            //   to happen because the request should have been validated at whatever process created the
                            //   cancel request (probably the API vertical).
                            //failure
                            JsonObject json = new JsonObject()
                                    .put("cause", "Cannot cancel non-root transfer tasks.")
                                    .put("message", "Cannot cancel non-root transfer tasks.")
                                    .mergeIn(body);

                            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                            resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                        } else if ( ! List.of(CANCELLED,COMPLETED,FAILED).contains(tt.getStatus())) {

    //                        getDbService().updateStatus(tenantId, uuid, CANCELLED.name(), reply -> {
    //                            if (reply.succeeded()) {
    //                                logger.debug("Transfer task {} status updated to PAUSED", uuid);
    //                                _doPublishEvent(TRANSFERTASK_CANCELLED, body);
    //
    //                                if (parentTaskId != null) {
    //                                    //logger.debug("Checking parent task {} for paused transfer task {}.", parentTaskId, uuid);
    //                                    processParentEvent(tenantId, parentTaskId, ttResult -> {
    //                                        if (ttResult.succeeded()) {
    //                                            //logger.debug("Check for parent task {} for paused transfer task {} done.", parentTaskId, uuid);
    //                                            // send notification events? or should listeners listen to the existing events?
    //                                            //_doPublishEvent("transfertask.notification", body);
    //                                            resultHandler.handle(Future.succeededFuture(ttResult.result()));
    //                                        } else {
    //                                            JsonObject json = new JsonObject()
    //                                                    .put("cause", ttResult.cause().getClass().getName())
    //                                                    .put("message", ttResult.cause().getMessage())
    //                                                    .mergeIn(body);
    //
    //                                            _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
    //                                            resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
    //                                        }
    //                                    });
    //                                }
    //                                else {
    //                                    logger.debug("Transfer task {} has no parent task to process.", uuid);
    //                                    resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
    //                                }
    //                            }
    //                            else {
    //                                logger.error("Failed to set status of transfertask {} to paused. error: {}", uuid, reply.cause());
    //                                JsonObject json = new JsonObject()
    //                                        .put("cause", reply.cause().getClass().getName())
    //                                        .put("message", reply.cause().getMessage())
    //                                        .mergeIn(body);
    //
    //                                _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
    //                                resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
    //                            }
    //                        });

                            // push the event transfer task onto the queue. this will cause all listening verticals
                            // actively processing any of its children to cancel their existing work and ack.
                            // the ack responses will bubble back up, eventually reaching the root, at which time,
                            // the root transfer task will be marked as cancelled.
                            _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_SYNC, gTTResult.result());
                            resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                        } else {
                            // if the root is not in an active state, then there is nothing needed at this time. we
                            // choose to send the sync event anyway so we can ensure that any subtasks still running will
                            // be cleaned up. This is a bit of an aggressive way to override eventual consistency.
                            logger.info("Status returned a Canceled, Complete or Failed. Return TRUE if everything is ok uuid {}", uuid);
                            _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_SYNC, gTTResult.result());
                            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                        //} else {
                            // TODO: think through cancel requests that are not on root tasks
                          //  _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_SYNC, body);
                            //resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                        }
                    } else {
                        //failure
                        JsonObject json = new JsonObject()
                                .put("cause", gTTResult.cause().getClass().getName())
                                .put("message", gTTResult.cause().getMessage())
                                .mergeIn(body);

                        _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                        //return promise.future();
                        resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                    }
//                } catch (Exception e) {
//                    logger.error(e.getMessage());
//                    JsonObject json = new JsonObject()
//                            .put("cause", e.getClass().getName())
//                            .put("message", e.getMessage())
//                            .mergeIn(body);
//
//                    _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
//                    resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
//                }
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
//        if (StringUtils.isEmpty(body.getString("parentTaskId"))) {
//            parentTaskId = "";
//        } else {
//            parentTaskId = body.getString("parentTaskId");
//        }
        String uuid = body.getString("uuid");
        String tenantId = body.getString("tenantId");

        //Promise<Boolean> promise = Promise.promise();
        logger.info("getting into allChildren");
        // if this task has children and all are cancelled or completed, then we can
        // mark this task as cancelled.
        getDbService().allChildrenCancelledOrCompleted(tenantId, uuid, allChildrenCancelledOrCompletedResp -> {
            logger.info("check allChildren");
            if (allChildrenCancelledOrCompletedResp.succeeded()) {
                // update the status of the transfertask since the verticle handling it has
                // reported back that it completed the cancel request since this tree is
                // done cancelling
                getDbService().setTransferTaskCanceledWhereNotCompleted(tenantId, uuid, setTransferTaskCanceledIfNotCompletedReso -> {
                    if (setTransferTaskCanceledIfNotCompletedReso.succeeded()) {
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
                                    logger.debug("Check for parent task {} for completed transfer task {} done.", parentTaskId, uuid);
                                    resultHandler.handle(Future.succeededFuture(processParentResp.result()));
                                } else {
                                    JsonObject json = new JsonObject()
                                            .put("cause", processParentResp.cause().getClass().getName())
                                            .put("message", processParentResp.cause().getMessage())
                                            .mergeIn(body);

                                    _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
                                    resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                                }
                            });
                        } else {
                            logger.info("Skipping parent.  Proccess complete for uuid {}", uuid);
                            resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                        }

                    } else {
                        // update failed on current task
                        logger.debug("Error with ProcessCancelAck.  We couldn't update the transfer task uuid {}", uuid );
                        JsonObject json = new JsonObject()
                                .put("cause", setTransferTaskCanceledIfNotCompletedReso.cause().getClass().getName())
                                .put("message", setTransferTaskCanceledIfNotCompletedReso.cause().getMessage())
                                .mergeIn(body);

                        _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                        resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                    }
                });
            } else {
                JsonObject json = new JsonObject()
                        .put("cause", allChildrenCancelledOrCompletedResp.cause().getClass().getName())
                        .put("message", allChildrenCancelledOrCompletedResp.cause().getMessage())
                        .mergeIn(body);

                _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
            }
        });

//        return resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
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
                if (TransferStatusType.getActiveStatusValues().contains(parentTask.getStatus())) {
                    // check to see if it has active children
                    getDbService().allChildrenCancelledOrCompleted(tenantId, parentTaskId, parentChildCancelledOrCompleteResult -> {
                        if (parentChildCancelledOrCompleteResult.succeeded()) {
                            // if it does have active children, then we skip the ack. The parent will be checked again as each
                            // child completes
                            if (parentChildCancelledOrCompleteResult.result()) {
                                resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                            } else {
                                // parent has children, but all are now cancelled or completed, so send the parent ack
                                _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, parentTask.toJSON());
                                resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
                            }
                        } else {
                            // failed to query children statuses
                            resultHandler.handle(Future.failedFuture(parentChildCancelledOrCompleteResult.cause()));
                        }
                    });
                } else {
                    // the parent is not active. forward the ack anyway, just to ensure all ancestors get the message
                    _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, parentTask.toJson());
                    resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
                }
            } else {
                // failure to lookup parent
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
     * @return boolean promise indicating whether an transfer.complete event was created for the parent transfertask
     */
    void processParentEvent(String tenantId, String parentTaskId, Handler<AsyncResult<Boolean>> resultHandler) {
//		Promise<Boolean> promise = Promise.promise();
        // lookup parent transfertask
        getDbService().getById(tenantId, parentTaskId, getTaskById -> {
            if (getTaskById.succeeded()) {
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
                                _doPublishEvent(TRANSFERTASK_CANCELLED, getTaskById.result());
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
                    resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
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
     * @deprecated
     * @param tenantId
     * @param uuid
     * @param resultHandler
     */
    protected void getTransferTask(String tenantId, String uuid, Handler<AsyncResult<TransferTask>> resultHandler) {
        getDbService().getById(tenantId, uuid, getTaskById -> {
            if (getTaskById.succeeded()) {
                TransferTask tt = new TransferTask(getTaskById.result());

                resultHandler.handle(Future.succeededFuture(tt));
            } else {
                logger.info("");
                resultHandler.handle(Future.failedFuture(getTaskById.cause()));
            }
        });
        //return (TransferTask) promise.future();
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }

}
