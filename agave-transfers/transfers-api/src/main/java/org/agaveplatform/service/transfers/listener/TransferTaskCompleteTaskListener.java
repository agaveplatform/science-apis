package org.agaveplatform.service.transfers.listener;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferTaskCompleteTaskListener extends AbstractTransferTaskListener {
	private final static Logger logger = LoggerFactory.getLogger(TransferTaskCompleteTaskListener.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_COMPLETED;

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<String>();

	public TransferTaskCompleteTaskListener() { super(); }

	public TransferTaskCompleteTaskListener(Vertx vertx) {
		setVertx(vertx);
	}

	public TransferTaskCompleteTaskListener(Vertx vertx, String eventChannel) {
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
            msg.reply(TransferTaskCompleteTaskListener.class.getName() + " completed.");

            JsonObject body = msg.body();
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");
			TransferTask tt = new TransferTask(body);

			logger.info("Transfer task {} completed: {} -> {}", uuid, source, dest);

			this.processEvent(body, result -> {
				if (result.succeeded()) {
					logger.error("Succeeded with the processing transfer completed event for transfer task {}", uuid);
				} else {
					logger.error("Error with return from complete event {}", uuid);
					_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
				}
			});
		});
	}

	public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
		//TransferTask bodyTask = new TransferTask(body);
		body.put("status", TransferStatusType.COMPLETED);
		String tenantId = body.getString("tenantId");
		String uuid = body.getString("uuid");
		String status = body.getString("status");
		String parentTaskId = body.getString("parentTask");
		logger.debug("Updating status of transfer task {} to COMPLETED", uuid);

		try {
//			dbService.updateStatus(tenantId, uuid, status, reply -> this.handleUpdateStatus(reply, tenantId, parentTaskId));
			getDbService().updateStatus(tenantId, uuid, TransferStatusType.COMPLETED.name(), reply -> {
				if (reply.succeeded()) {
					logger.debug(TransferTaskCompleteTaskListener.class.getName() + ":Transfer task {} status updated to COMPLETED", uuid);
					_doPublishEvent(MessageType.TRANSFERTASK_FINISHED, body);

					if (parentTaskId != null ) {
						logger.debug("Checking parent task {} for completed transfer task {}.", parentTaskId, uuid);
						processParentEvent(tenantId, parentTaskId, tt -> {
							if (tt.succeeded()) {
								tt.result();
								logger.debug("Check for parent task {} for completed transfer task {} done.", parentTaskId, uuid);
								handler.handle(Future.succeededFuture(true));
							} else {
								JsonObject json = new JsonObject()
										.put("cause", tt.cause().getClass().getName())
										.put("message", tt.cause().getMessage())
										.mergeIn(body);

								_doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
								handler.handle(Future.succeededFuture(false));
							}
						});
					} else {
						//Transfer task is the root/parent task
						logger.debug("Transfer task {} has no parent task to process.", uuid);
						handler.handle(Future.succeededFuture(true));
					}
				}
				else {
					String msg = String.format("Failed to set status of transfer task %s to completed. error: %s",
							uuid, reply.cause().getMessage());
					doHandleError(reply.cause(), msg, body, handler);
				}
			});
		} catch (Exception e) {
			doHandleError(e, e.getMessage(), body, handler);
		}
	}


	/**
	 * Handles processing of parent task to see if any other children are active. If not, we create a new
	 * transfer.complete event for the parent. This allows us to ensure that when the last child completes,
	 * it will propagate back up the transfer task tree to the root task, which is the parent with no parent,
	 * and mark that as completed. This ensures all tasks will be marked as completed upon completion or
	 * cancellation or failure.
	 * @param tenantId the tenant of the transfertask
	 * @param parentTaskId the id of the parent
	 * @param resultHandler the handler to call with a boolean value indicating whether the parent event was found to be incomplete and needed to have a transfer.complete event created.
	 */
	protected void processParentEvent(String tenantId, String parentTaskId, Handler<AsyncResult<Boolean>> resultHandler) {
		// lookup parent transfertask
		getDbService().getById(tenantId, parentTaskId, getTaskById -> {
			if (getTaskById.succeeded()) {
				// check whether it's active or not by its status2spy

				TransferTask parentTask = new TransferTask(getTaskById.result());
				if ( ! parentTask.getStatus().toString().isEmpty() &&
						! List.of( PAUSED,CANCELLED, COMPLETED,COMPLETED_WITH_ERRORS,ERROR, FAILED).contains(parentTask.getStatus())) {

					// if the parent is active, check for any children still active to see if it needs to be notified
					// that all children have completed.
					getDbService().allChildrenCancelledOrCompleted(tenantId, parentTaskId, isAllChildrenCancelledOrCompleted -> {
						if (isAllChildrenCancelledOrCompleted.succeeded()) {
							// if all children are completed or cancelled, the parent is completed. create that event
							if (isAllChildrenCancelledOrCompleted.result()) {
								logger.debug("All child tasks for parent transfer task {} are cancelled or completed. " +
										"A transfer.completed event will be created for this task.", parentTaskId);
								// call to our publishing helper for easier testing.
								_doPublishEvent(MessageType.TRANSFER_COMPLETED, getTaskById.result());
							} else {
								logger.debug("Parent transfer task {} has active children. " +
										"Skipping further processing ", parentTaskId);
								// parent has active children. let it run
							}

							// return true indicating the parent event was processed
							resultHandler.handle(isAllChildrenCancelledOrCompleted);
						} else {
							logger.debug("Failed to look up children for parent transfer task {}. " +
									"Skipping further processing ", parentTaskId);
							// failed to look up child tasks. processing of the parent failed. we
							// forward on the exception to the handler
							resultHandler.handle(Future.failedFuture(isAllChildrenCancelledOrCompleted.cause()));
						}
					});
				} else {
					logger.debug("Parent transfer task {} is already in a terminal state. " +
							"Skipping further processing ", parentTaskId);
					// parent is already terminal. let it lay
					// return true indicating the parent event was processed
					resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
				}
			} else {
				logger.error("Failed to lookup parent transfer task {}: {}", parentTaskId, getTaskById.cause().getMessage());
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
