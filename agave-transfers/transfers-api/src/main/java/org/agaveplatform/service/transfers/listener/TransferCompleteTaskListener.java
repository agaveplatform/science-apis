package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
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

public class TransferCompleteTaskListener extends AbstractTransferTaskListener {
	private final static Logger logger = LoggerFactory.getLogger(TransferCompleteTaskListener.class);

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<String>();

	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_COMPLETED;

	public TransferCompleteTaskListener(Vertx vertx) {
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
			String status = body.getString("status");
			logger.info("Transfer task {} completed with status {}", uuid, status);

			this.processEvent(body);
		});
	}

	public Future<Boolean> processEvent(JsonObject body) {
		Promise<Boolean> promise = Promise.promise();
		// udpate transfer task status to completed

		//TransferTask bodyTask = new TransferTask(body);
		body.put("status", TransferStatusType.COMPLETED);
		String tenantId = body.getString("tenantId");
		String uuid = body.getString("uuid");
		String status = body.getString("status");
		String parentTaskId = body.getString("parentTask");

		try {
//			dbService.updateStatus(tenantId, uuid, status, reply -> this.handleUpdateStatus(reply, tenantId, parentTaskId));
			getDbService().updateStatus(tenantId, uuid, TransferStatusType.COMPLETED.name(), reply -> {
				if (reply.succeeded()) {

					_doPublishEvent(MessageType.TRANSFERTASK_COMPLETED, body);

					if (parentTaskId != null) {
						processParentEvent(tenantId, parentTaskId, tt -> {
							if (tt.succeeded()) {
								// TODO: send notification events? or should listeners listen to the existing events?
								//_doPublishEvent("transfertask.notification", body);
								promise.complete(tt.result());
							} else {
								logger.error(tt.cause().getMessage());
								JsonObject json = new JsonObject()
										.put("cause", tt.cause().getClass().getName())
										.put("message", tt.cause().getMessage())
										.mergeIn(body);

								_doPublishEvent(MessageType.TRANSFERTASK_PARENT_ERROR, json);
								promise.complete(Boolean.FALSE);
							}
						});
					}
					else {
						promise.complete(Boolean.TRUE);
					}
				}
				else {
					logger.error("Failed to set status of transfertask {} to completed. error: {}", uuid, reply.cause());
					JsonObject json = new JsonObject()
							.put("cause", reply.cause().getClass().getName())
							.put("message", reply.cause().getMessage())
							.mergeIn(body);

					_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
					promise.fail(reply.cause());
				}
			});
		} catch (Exception e) {
			logger.error(e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);

			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			promise.fail(e);
		}

		logger.debug("exiting processEvent for transfertask {}", uuid);
		return promise.future();
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
	 * @return boolean promise indicating whether an transfer.complete event was created for the parent transfertask
	 */
	protected void processParentEvent(String tenantId, String parentTaskId, Handler<AsyncResult<Boolean>> resultHandler) {
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
								// call to our publishing helper for easier testing.
								_doPublishEvent(MessageType.TRANSFER_COMPLETED, getTaskById.result());
							} else {
								// parent has active children. let it run
							}

							// return true indicating the parent event was processed
							resultHandler.handle(isAllChildrenCancelledOrCompleted);
						} else {
							// failed to look up child tasks. processing of the parent failed. we
							// forward on the exception to the handler
							resultHandler.handle(Future.failedFuture(isAllChildrenCancelledOrCompleted.cause()));
						}
					});
				} else {
					// parent is already terminal. let it lay
					// return true indicating the parent event was processed
					resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
				}
			} else {
//				promise.fail(getTaskById.cause());
//				logger.error("Failed to set status of transfertask {} to completed. error: {}", parentTaskId, getTaskById.cause());
				resultHandler.handle(Future.failedFuture(getTaskById.cause()));
			}
		});

//		return promise;
	}

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}


}
