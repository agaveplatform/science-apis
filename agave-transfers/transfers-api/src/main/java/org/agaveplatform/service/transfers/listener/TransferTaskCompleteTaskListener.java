package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.vertx.core.*;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.TransferRateHelper;
import org.iplantc.service.common.messaging.Message;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferTaskCompleteTaskListener extends AbstractNatsListener {
	private final static Logger logger = LoggerFactory.getLogger(TransferTaskCompleteTaskListener.class);
	protected static final String EVENT_CHANNEL = TRANSFER_COMPLETED;

	private TransferTaskDatabaseService dbService;
	protected List<String>  parentList = new ArrayList<String>();
	public Connection nc;
	public TransferTaskCompleteTaskListener() throws IOException, InterruptedException {
		super();
	}

	public TransferTaskCompleteTaskListener(Vertx vertx) throws IOException, InterruptedException {
		super();
		setVertx(vertx);
	}

	public TransferTaskCompleteTaskListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	/**
	 * Mockable method to initialize connection to the database from the pool
	 * @return {@link TransferTaskDatabaseService} connection to the database
	 */
	public TransferTaskDatabaseService createDatabaseConnection(){
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		return TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);
	}


	@Override
	public void start(Promise<Void> startPromise) throws IOException, InterruptedException, TimeoutException {
		DateTimeZone.setDefault(DateTimeZone.forID("America/Chicago"));
		TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"));

		// init our db connection from the pool
		dbService = createDatabaseConnection();

		try {
			//group subscription so each message only processed by this vertical type once
			subscribeToSubjectGroup(EVENT_CHANNEL, this::handleMessage);
		} catch (Exception e) {
			logger.error("TRANSFER_ALL - Exception {}", e.getMessage());
			startPromise.tryFail(e);
		}

	}

	protected void handleMessage(Message message) {
		try {
			JsonObject body = new JsonObject(message.getMessage());
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");
			logger.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);
			getVertx().<Boolean>executeBlocking(
					promise -> {
						try {
							processEvent(body, promise);
						} catch (IOException | InterruptedException e) {
							logger.error(e.getCause().toString());
						}
					},
					resp -> {
						if (resp.succeeded()) {
							logger.debug("Finished processing {} for transfer task {}", TRANSFERTASK_PAUSED, uuid);
						} else {
							logger.debug("Failed  processing {} for transfer task {}", TRANSFERTASK_PAUSED, uuid);
						}
					});
		} catch (DecodeException e) {
			logger.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
		} catch (Throwable t) {
			logger.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
		}
	}

	public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException {
		//body.put("status", TransferStatusType.COMPLETED);
		String tenantId = body.getString("tenant_id");
		String uuid = body.getString("uuid");
		String parentTaskId = body.getString("parentTask") == null ? body.getString("parent_task") : body.getString("parentTask");
		logger.debug("Updating status of transfer task {} to COMPLETED.  Tenant ID is {}", uuid, tenantId);
		TransferTask bodyTask = new TransferTask(body);

		try {
//			dbService.updateStatus(tenantId, uuid, status, reply -> this.handleUpdateStatus(reply, tenantId, parentTaskId));
			getDbService().update(tenantId, uuid, bodyTask, reply -> {
				if (reply.succeeded()) {
					logger.debug("Tenant ID is {}", tenantId);
					logger.debug(TransferTaskCompleteTaskListener.class.getName() + ":Transfer task {} status updated to COMPLETED", uuid);
					_doPublishEvent(TRANSFERTASK_FINISHED, reply.result(), finishedResp -> {
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
									logger.debug("update failed. {}.  The message is {}", tt.cause().getClass().getName(), tt.cause().getMessage());

									_doPublishEvent(TRANSFERTASK_PARENT_ERROR, json, parentErrorResp -> {
										handler.handle(Future.succeededFuture(false));
									});
								}
							});
						} else {
							//Transfer task is the root/parent task
							logger.debug("Transfer task {} has no parent task to process.", uuid);
							handler.handle(Future.succeededFuture(true));
						}
					});
				}
				else {
					String msg = String.format("Failed to set status of transfer task %s to completed. error: %s",
							uuid, reply.cause().getMessage());
					logger.debug(msg);
					doHandleError(reply.cause(), msg, body, handler);
				}
			});
		} catch (Exception e) {
			logger.debug("Something failed. {}", e.getMessage());
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
		getDbService().getByUuid(tenantId, parentTaskId, getTaskById -> {
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
								parentTask.setEndTime(Instant.now());

								getDbService().getBytesTransferredForAllChildren(tenantId, parentTaskId, getBytesTransferred -> {
									if (getBytesTransferred.succeeded()){
										parentTask.setBytesTransferred(getBytesTransferred.result().getLong("bytes_transferred"));
										_doPublishEvent(TRANSFER_COMPLETED, TransferRateHelper.updateTransferRate(parentTask).toJson(), completeResp -> {
											// return true indicating the parent event was processed
											resultHandler.handle(isAllChildrenCancelledOrCompleted);
										});
									} else {
										logger.debug("Failed to retrieve bytes transferred for all child tasks for parent task {}, {}", parentTaskId, getBytesTransferred.cause());
										resultHandler.handle(Future.failedFuture(getBytesTransferred.cause()));
									}
								});
							} else {
								logger.debug("Parent transfer task {} has active children. " +
										"Skipping further processing ", parentTaskId);

								// return true indicating the parent event was processed
								resultHandler.handle(isAllChildrenCancelledOrCompleted);
							}


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
