package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.apache.commons.lang3.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELLED;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_PAUSED;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferTaskCancelListener extends AbstractTransferTaskListener {
    private final Logger logger = LoggerFactory.getLogger(TransferTaskCancelListener.class);

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

    protected Future<Boolean> processCancelRequest(JsonObject body, Handler<AsyncResult<Boolean>> resultHandler) {
        Promise<Boolean> promise = Promise.promise();

        String tenantId = body.getString("tenantId");
        String uuid = body.getString("uuid");
        String status = body.getString("status");
        String parentTaskId = body.getString("parentTaskId");

        Long id = 0L;
        try {
            // lookup the transfer task from the db
            TransferTask tt = getTransferTask(tenantId, uuid, gTTResult -> {
                if ( gTTResult.succeeded()){
                    logger.info("got transfer task uuid {}", uuid);
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
            });
            // if it's not already in a terminal state, process the cancell requrest
            if (tt.getStatus() != TransferStatusType.CANCELLED ||
                    tt.getStatus() != TransferStatusType.COMPLETED ||
                    tt.getStatus() != TransferStatusType.FAILED) {


                getDbService().updateStatus(tenantId, uuid, CANCELLED.name(), reply -> {
                    if (reply.succeeded()) {
                        //logger.debug("Transfer task {} status updated to PAUSED", uuid);
                        _doPublishEvent(TRANSFERTASK_CANCELLED, body);

                        if (parentTaskId != null) {
                            //logger.debug("Checking parent task {} for paused transfer task {}.", parentTaskId, uuid);
                            processParentEvent(tenantId, parentTaskId, ttResult -> {
                                if (ttResult.succeeded()) {
                                    //logger.debug("Check for parent task {} for paused transfer task {} done.", parentTaskId, uuid);
                                    // TODO: send notification events? or should listeners listen to the existing events?
                                    //_doPublishEvent("transfertask.notification", body);
                                    promise.complete(ttResult.result());
                                } else {
                                    JsonObject json = new JsonObject()
                                            .put("cause", ttResult.cause().getClass().getName())
                                            .put("message", ttResult.cause().getMessage())
                                            .mergeIn(body);

                                    _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
                                    promise.complete(Boolean.FALSE);
                                }
                            });
                        }
                        else {
//						logger.debug("Transfer task {} has no parent task to process.", uuid);
                            promise.complete(Boolean.TRUE);
                        }
                    }
                    else {
//					logger.error("Failed to set status of transfertask {} to paused. error: {}", uuid, reply.cause());
                        JsonObject json = new JsonObject()
                                .put("cause", reply.cause().getClass().getName())
                                .put("message", reply.cause().getMessage())
                                .mergeIn(body);

                        _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                        promise.fail(reply.cause());
                    }
                });

                // push the event transfer task onto the queue. this will cause all listening verticals
                // actively processing any of its children to cancel their existing work and ack
                _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_SYNC, tt);
            } else {
                logger.info("Status returned a Canceled, Complete or Failed. Return TRUE if everything is ok uuid {}", uuid);
                _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_COMPLETED, tt.toJson());
                //return promise.future();
                resultHandler.handle(Future.succeededFuture(Boolean.TRUE));
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            JsonObject json = new JsonObject()
                    .put("cause", e.getClass().getName())
                    .put("message", e.getMessage())
                    .mergeIn(body);

            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
        }

        return promise.future();
    }

    protected Future<Boolean> processCancelAck(JsonObject body, Handler<AsyncResult<Boolean>> resultHandler) {
        String parentTaskId;
        if (StringUtils.isEmpty(body.getString("parentTaskId"))) {
            parentTaskId = "";
        } else {
            parentTaskId = body.getString("parentTaskId");
        }
        String uuid = body.getString("uuid");
        String tenantId = body.getString("owner");

        Promise<Boolean> promise = Promise.promise();

        // if this task has children and all are cancelled or completed, then we can
        // mark this task as cancelled.
        getDbService().allChildrenCancelledOrCompleted(tenantId, uuid, result -> {
            if (result.succeeded()) {
                // update the status of the transfertask since the verticle handling it has
                // reported back that it completed the cancel request since this tree is
                // done cancelling
                getDbService().setTransferTaskCanceledIfNotCompleted(tenantId, uuid, resultTTCan -> {
                    if (resultTTCan.succeeded()) {
                        logger.info("DB updated with status of CANCELED uuid = {}", uuid);
                    } else {
                        JsonObject json = new JsonObject()
                                .put("cause", resultTTCan.cause().getClass().getName())
                                .put("message", resultTTCan.cause().getMessage())
                                .mergeIn(body);

                        _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                        promise.complete(Boolean.FALSE);
                    }
                });

                // this task and all its children are done, so we can send a complete event
                // to safely clear out the uuid from all listener verticals' caches
                _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_COMPLETED, body);

                // we can now also check the parent, if present, for completion of its tree.
                // if the parent is empty, the root will be as well. For children of the root
                // transfer task, the root == parent
                if (StringUtils.isNotEmpty(parentTaskId)) {

                    // once all tasks in the parents tree are complete, we can update the status of the parent
                    // and send out the completed event to clear its uuid out of any caches that may have it.
                    getDbService().allChildrenCancelledOrCompleted(tenantId, parentTaskId, attcResult -> {
                        if (attcResult.succeeded()) {
                            attcResult.result();

                            getDbService().setTransferTaskCanceledIfNotCompleted(parentTaskId, uuid, ttSac -> {
                                if (ttSac.succeeded()) {
                                    TransferTask parentTask = new TransferTask();
                                     getTransferTask(tenantId, parentTaskId, ttSacResult -> {
                                        if(ttSacResult.succeeded()) {
                                            ttSacResult.result();
                                            _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, parentTask.toJSON());
                                        } else {
                                            JsonObject json = new JsonObject()
                                                    .put("cause", ttSac.cause().getClass().getName())
                                                    .put("message", ttSac.cause().getMessage())
                                                    .mergeIn(body);

                                            _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
                                            //ttSacResult.handle(Future.failedFuture(json.toString()));
                                        }
                                    });

                                } else {
                                    JsonObject json = new JsonObject()
                                            .put("cause", ttSac.cause().getClass().getName())
                                            .put("message", ttSac.cause().getMessage())
                                            .mergeIn(body);

                                    _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
                                }
                            });

                        } else {
                            JsonObject json = new JsonObject()
                                    .put("cause", attcResult.cause().getClass().getName())
                                    .put("message", attcResult.cause().getMessage())
                                    .mergeIn(body);

                            _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
                        }

                        TransferTask parentTask = new TransferTask();
                        getTransferTask(tenantId, parentTaskId, ttResult ->{
                            if (result.succeeded()) {
                                ttResult.result();
                                _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, parentTask.toJSON());
                            } else {
                                //failure
                                JsonObject json = new JsonObject()
                                        .put("cause", attcResult.cause().getClass().getName())
                                        .put("message", attcResult.cause().getMessage())
                                        .mergeIn(body);

                                _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
                            }
                        });

                    });
                }
            } else {
                JsonObject json = new JsonObject()
                        .put("cause", result.cause().getClass().getName())
                        .put("message", result.cause().getMessage())
                        .mergeIn(body);

                _doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
                promise.complete(Boolean.FALSE);
            }
        });

        return promise.future();
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
                    getDbService().allChildrenCancelledOrCompleted(tenantId, parentTaskId, isAllChildrenCancelledOrCompleted -> {
                        if (isAllChildrenCancelledOrCompleted.succeeded()) {
                            // if all children are completed or cancelled, the parent is completed. create that event
                            if (isAllChildrenCancelledOrCompleted.result()) {
                                //logger.debug("All child tasks for parent transfer task {} are paused, cancelled or completed. " +
                                //		"A transfer.paused event will be created for this task.", parentTaskId);
                                // call to our publishing helper for easier testing.
                                _doPublishEvent(TRANSFERTASK_PAUSED, getTaskById.result());
                            } else {
                                //logger.debug("Parent transfer task {} has active children. " +
                                //		"Skipping further processing ", parentTaskId);
                                // parent has active children. let it run
                            }

                            // return true indicating the parent event was processed
                            resultHandler.handle(isAllChildrenCancelledOrCompleted);
                        } else {
                            //logger.debug("Failed to look up children for parent transfer task {}. " +
                            //		"Skipping further processing ", parentTaskId);
                            // failed to look up child tasks. processing of the parent failed. we
                            // forward on the exception to the handler
                            resultHandler.handle(Future.failedFuture(isAllChildrenCancelledOrCompleted.cause()));
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

    protected Future<Boolean> allChildrenCancelledOrCompleted(String parentTaskId, Handler<AsyncResult<TransferTask>> result) {
        Promise<Boolean> promise = Promise.promise();
        //TODO: "select count(id) from transfertasks where (parentTask = {}) and status not in (('COMPLETED', 'CANCELLED','FAILED')"

        getDbService().getAllChildrenCanceledOrCompleted(parentTaskId, getActiveRootTaskIds -> {
            if (getActiveRootTaskIds.succeeded()) {
                //logger.info("Found {} active transfer tasks", getActiveRootTaskIds.result().size());
                getActiveRootTaskIds.result().getList().forEach(parentTask -> {
                    logger.debug("[{}] Running health check on transfer task {}",
                    		((JsonObject)parentTask).getString("tenantId"),
                    		((JsonObject)parentTask).getString("uuid"));
                    _doPublishEvent(TRANSFERTASK_CANCELLED, parentTask);
                });
                promise.handle(Future.succeededFuture());
            }
            else {
                //logger.error("Unable to retrieve list of active transfer tasks: {}", getActiveRootTaskIds.cause());
                promise.handle(Future.failedFuture(getActiveRootTaskIds.cause()));
            }
        });
        return promise.future();
    }

    protected Future<Boolean> setTransferTaskCancelledIfNotCompleted(String tenantId, String uuid) {
        Promise<Boolean> promise = Promise.promise();
        //TODO: "update transfertasks set status = CANCELLED, lastUpdated = now() where uuid = {} and status not in ('COMPLETED', 'CANCELLED','FAILED')"

        getDbService().setTransferTaskCanceledIfNotCompleted(tenantId, uuid, result -> {
            if (result.succeeded()) {
                //logger.info("[{}] Transfer task {} updated.", tenantId, uuid);
                promise.handle(Future.succeededFuture(Boolean.TRUE));
            } else {
                //logger.error("[{}] Task {} update failed: {}", tenantId, uuid, result.cause());
                JsonObject json = new JsonObject()
                        .put("cause", result.cause().getClass().getName())
                        .put("message", result.cause().getMessage());
                _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                promise.handle(Future.failedFuture(result.cause()));
            }
        });
        return promise.future();
        }


    protected List<TransferTask> getTransferTaskTree(String rootUuid) {
        // TODO: make db or api call to service
        return new ArrayList<TransferTask>();
    }

    protected TransferTask getTransferTask(String tenantId, String uuid, Handler<AsyncResult<TransferTask>> resultHandler) {
        Promise<TransferTask> promise = Promise.promise();

        dbService.getById(tenantId, uuid, getTaskById -> {
            if (getTaskById.succeeded()) {
                TransferTask tt = new TransferTask(getTaskById.result());

                resultHandler.handle(Future.succeededFuture(tt));
            } else {
                logger.info("");
                resultHandler.handle(Future.failedFuture(getTaskById.cause()));
            }
        });
        return (TransferTask) promise.future();
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }


}
