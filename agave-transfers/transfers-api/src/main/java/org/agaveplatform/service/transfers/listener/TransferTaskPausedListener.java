package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.exception.TransferException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.*;

public class TransferTaskPausedListener extends AbstractNatsListener {
	private static final Logger logger = LoggerFactory.getLogger(TransferTaskPausedListener.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_PAUSED;

	private TransferTaskDatabaseService dbService;
	private List<TransferTask> ttTree = new ArrayList<TransferTask>();
	public Connection nc;

	public TransferTaskPausedListener() throws IOException, InterruptedException {
		super();
		setConnection();
	}
	public TransferTaskPausedListener(Vertx vertx) throws IOException, InterruptedException {
		this(vertx, null);
		setConnection();
	}
	public TransferTaskPausedListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
		setConnection();
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	public Connection getConnection(){return nc;}

	public void setConnection() throws IOException, InterruptedException {
		try {
			nc = _connect(CONNECTION_URL);
		} catch (IOException e) {
			//use default URL
			nc = _connect(Options.DEFAULT_URL);
		}
	}

	@Override
	public void start() throws TimeoutException, IOException, InterruptedException {
		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		//EventBus bus = vertx.eventBus();
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		//Connection nc = _connect();
		Dispatcher d = getConnection().createDispatcher((msg) -> {});
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		Subscription s = d.subscribe(MessageType.TRANSFERTASK_PAUSED, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} pause detected.", uuid);

			processPauseRequest(body, result -> {
				if (result.succeeded()) {
					logger.error("Succeeded with the processing the pause transfer event for transfer task {}", uuid);
				} else {
					logger.error("Error with return from pausing the event {}", uuid);
					try {
						_doPublishNatsJSEvent("TRANSFERTASK", MessageType.TRANSFERTASK_ERROR, body);
					} catch (Exception e) {
						logger.debug(e.getMessage());
					}
				}
			});
		});
		d.subscribe(MessageType.TRANSFERTASK_PAUSED);
		getConnection().flush(Duration.ofMillis(500));

		//bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_ACK, msg -> {
		s = d.subscribe(MessageType.TRANSFERTASK_PAUSED_ACK, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");

//			msg.reply(TransferTaskPausedListener.class.getName() + " received.");

			String parentTaskId = body.getString("parentTaskId");
			String rootTaskId = body.getString("rootTaskId");
			String tenantId = body.getString("owner");

			logger.info("Transfer task {} ackowledged paused", uuid);

			processPauseAckRequest(body, result -> {

			});
		});
		d.subscribe(MessageType.TRANSFERTASK_PAUSED_ACK);
		getConnection().flush(Duration.ofMillis(500));

	}

	/**
	 * Handles processing of paused events, updating the transfer task status and checking for active siblings
	 * before sending the parent paused ack event as well.
	 *
	 * @param body the original event body
	 * @param handler to callback to return the result of the async processing
	 */
	protected void processPauseRequest(JsonObject body, Handler<AsyncResult<Boolean>> handler) {

		String tenantId = body.getString("tenant_id");
		String uuid = body.getString("uuid");
		String status = body.getString("status");
		String parentTaskId = body.getString("parentTask");
		logger.debug("Updating status of transfer task {} to PAUSED", uuid);

		getDbService().getByUuid(tenantId, uuid, reply -> {
			if (reply.succeeded()) {

				TransferTask targetTransferTask = new TransferTask(reply.result());
				String parentTaskId1 = targetTransferTask.getParentTaskId();

//				logger.info("Transfer task {} status updated to PAUSED", uuid);
//				_doPublishEvent(TRANSFERTASK_PAUSED, body);
				logger.info("Transfer task {} status updated to PAUSED", uuid);
//				_doPublishEvent(TRANSFERTASK_PAUSED_SYNC, body);

				// pausing should only happen to root tasks
				if (StringUtils.isNotBlank(targetTransferTask.getRootTaskId()) ||
						StringUtils.isNotBlank(targetTransferTask.getParentTaskId())) {
					logger.info("The root id is {} and the parentID is {}", targetTransferTask.getRootTaskId(), targetTransferTask.getParentTaskId());
					logger.debug("Checking parent task {} for paused transfer task {}.", parentTaskId1, uuid);

					if ( StringUtils.equals(targetTransferTask.getRootTaskId(),targetTransferTask.getUuid()) ||
							StringUtils.equals(targetTransferTask.getParentTaskId(), targetTransferTask.getUuid()) ){
						JsonObject json = new JsonObject()
								.put("cause", TransferException.class.getName() )
								.put("message", "Cannot have root task that matches child transfer tasks.")
								.mergeIn(body);
					try {
						_doPublishNatsJSEvent("TRANSFERTASK", MessageType.TRANSFERTASK_ERROR, json);
						handler.handle(Future.succeededFuture(true));
					} catch (Exception e) {
						logger.debug(e.getMessage());
					}

					}else {
						JsonObject json = new JsonObject()
								.put("cause", TransferException.class.getName() )
								.put("message", "Cannot cancel non-root transfer tasks.")
								.mergeIn(body);
						try {
							_doPublishNatsJSEvent("TRANSFERTASK", MessageType.TRANSFERTASK_ERROR, json);
						} catch (Exception e) {
							logger.debug(e.getMessage());
						}
						handler.handle(Future.succeededFuture(true));
					}
				} else if (targetTransferTask.getStatus().isActive()) {
					// push the event transfer task onto the queue. this will cause all listening verticals
					// actively processing any of its children to pause their existing work and ack.
					// the ack responses will bubble back up, eventually reaching the root, at which time,
					// the root transfer task will be marked as paused.
					logger.debug("Updating status of transfer task {} to {} prior to sending cancel sync event.",
							uuid, PAUSE_WAITING.name());
					getDbService().updateStatus(tenantId, uuid, PAUSE_WAITING.name(), updateReply -> {
						if (updateReply.succeeded()) {
							processParentEvent(tenantId, uuid, processParentReply -> {
								if (processParentReply.succeeded()) {

									logger.debug(String.format("Successfully updated the status of transfer task %s to %s prior " +
													"to sending %s event.",
											uuid, PAUSE_WAITING.name(), TRANSFERTASK_PAUSED_SYNC));
									logger.debug("Sending cancel sync event for transfer task {} to signal children to cancel any active work.", uuid);
									try {
										_doPublishNatsJSEvent("TRANSFERTASK", TRANSFERTASK_PAUSED_SYNC, updateReply.result());
										handler.handle(Future.succeededFuture(true));
									} catch (Exception e) {
										logger.debug(e.getMessage());
									}
								} else {
									String message = String.format("Failed to process paused ack event for parent " +
											"transfertask %s. %s", parentTaskId, processParentReply.cause());
									logger.error(message);
									JsonObject json = new JsonObject()
											.put("cause", processParentReply.cause().getClass().getName())
											.put("message", message)
											.mergeIn(body);
									try {
										_doPublishNatsJSEvent("TRANSFERTASK", MessageType.TRANSFERTASK_PARENT_ERROR, json);
										handler.handle(Future.succeededFuture(false));
									} catch (Exception e) {
										logger.debug(e.getMessage());
									}
								}
							});

						} else {
							String msg = String.format("Unable to update the status of transfer task %s to %s prior " +
											"to sending %s event. No sync event will be sent.",
									uuid, CANCELING_WAITING.name(), TRANSFERTASK_CANCELED_SYNC);
							try {
								logger.debug(msg);
								doHandleError(updateReply.cause(), msg, body, handler);
							} catch (IOException e) {
								logger.debug(e.getMessage());
							} catch (InterruptedException e) {
								logger.debug(e.getMessage());
							}
						}
					});

				}
				else {
					logger.info("Transfer task {} is not in an active state and will not be updated.", uuid);
					logger.debug("Sending cancel sync event for transfer task {} to ensure children are cleaned up.", uuid);
					logger.debug("Transfer task {} has no parent task to process.", uuid);
					handler.handle(Future.succeededFuture(true));
				}
			}
			else {
				String message = String.format("Failed to set status of transfertask %s to paused. error: %s", uuid, reply.cause());
				logger.debug(message);
				try {
					doHandleError(reply.cause(), message, body, handler);
				} catch (IOException | InterruptedException e) {
					logger.debug(e.getMessage());
				}
			}
		});
	}

	/**
	 * Handles processing of paused ack events, updating the transfer task status and checking for active siblings
	 * before sending the parent paused ack event as well.
	 *
	 * @param body the original event body
	 * @param handler to callback to return the result of the async processing
	 */
	protected void processPauseAckRequest(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
		String uuid = body.getString("uuid");
		String parentTaskId = body.getString("parentTaskId");
		String rootTaskId = body.getString("rootTaskId");
		String tenantId = body.getString("owner");


		// if this task has children and all are cancelled or completed, then we can
		// mark this task as cancelled.
		getDbService().allChildrenCancelledOrCompleted(tenantId, uuid, reply -> {
			if (reply.succeeded()) {
				// update the status of the transfertask since the verticle handling it has
				// reported back that it completed the cancel request since this tree is
				// done cancelling
				getDbService().setTransferTaskCanceledWhereNotCompleted(tenantId, uuid, updateReply -> {
					if (updateReply.succeeded()) {
						logger.debug("Updated state of transfer task {} to PAUSED. Creating paused ack event.", uuid);
						// this task and all its children are done, so we can send a complete event
						// to safely clear out the uuid from all listener verticals' caches
						try {
							_doPublishNatsJSEvent("TRANSFERTASK", MessageType.TRANSFERTASK_PAUSED_COMPLETED, body);
						} catch (Exception e) {
							logger.debug(e.getMessage());
						}
						// we can now also check the parent, if present, for completion of its tree.
						// if the parent is empty, the root will be as well. For children of the root
						// transfer task, the root == parent
						if (!StringUtils.isEmpty(parentTaskId)) {
							logger.debug("Checking for active child transfer tasks for transfer task {}.", parentTaskId);
							// once all tasks in the parents tree are complete, we can update the status of the parent
							// and send out the completed event to clear its uuid out of any caches that may have it.
							getDbService().allChildrenCancelledOrCompleted(tenantId, parentTaskId, siblingReply -> {
								if (siblingReply.succeeded()) {
									if (siblingReply.result()) {
										logger.debug("All children of parent transfer task {} are inactive. ", parentTaskId);
										getDbService().getByUuid(tenantId, parentTaskId, parentReply -> {
											if (parentReply.succeeded()) {
												logger.info("Sending pause ack event for parent transfer task {}", parentTaskId);
												//getVertx().eventBus().publish(MessageType.TRANSFERTASK_PAUSED_ACK, parentReply.result());
												_doPublishNatsJSEvent("TRANSFERTASK", MessageType.TRANSFERTASK_PAUSED_ACK, parentReply.result());
												handler.handle(Future.succeededFuture(true));
											} else {
												String message = String.format("Unable to lookup parent task %s for transfer task %s while processing a pause event. %s",
														parentTaskId, uuid, parentReply.cause().getMessage());
												logger.error(message);
												JsonObject json = new JsonObject()
														.put("cause", parentReply.cause().getClass().getName())
														.put("message", message)
														.mergeIn(body);
												try {
													_doPublishNatsJSEvent("TRANSFERTASK", TRANSFERTASK_ERROR, json);
													handler.handle(Future.failedFuture(parentReply.cause()));
												} catch (Exception e) {
													logger.debug(e.getMessage());
												}
											}
										});
									} else {
										logger.debug("Skipping pause due to all children being in a DONE state {}", parentTaskId);
										handler.handle(Future.succeededFuture(false));
									}
								} else {
									String message = String.format("Unable to lookup child state for parent transfer task %s  while processing a pause event. %s",
											parentTaskId, siblingReply.cause().getMessage());
									logger.error(message);
									JsonObject json = new JsonObject()
											.put("cause", siblingReply.cause().getClass().getName())
											.put("message", message)
											.mergeIn(body);
									try {
										_doPublishNatsJSEvent("TRANSFERTASK", TRANSFERTASK_ERROR, json);
										handler.handle(Future.failedFuture(siblingReply.cause()));
									} catch (Exception e) {
										logger.debug(e.getMessage());
									}
								}
							});
						} else {
							logger.debug("Skipping processing of parent task for root transfer task {}.", uuid);
							handler.handle(Future.succeededFuture(false));
						}
					} else {
						String message = String.format("Unable to update status of transfer task %s to PAUSED. %s",
								uuid, updateReply.cause().getMessage());
						logger.debug(message);
						try {
							doHandleError(updateReply.cause(), message, body, handler);
						} catch (IOException e) {
							logger.debug(e.getMessage());
						} catch (InterruptedException e) {
							logger.debug(e.getMessage());
						}
					}
				});
			} else {
				String message = String.format("Unable to lookup child tasks for transfer task %s while processing a pause event. %s",
						uuid, reply.cause().getMessage());
				logger.debug(message);
				try {
					doHandleError(reply.cause(), message, body, handler);
				} catch (IOException e) {
					logger.debug(e.getMessage());
				} catch (InterruptedException e) {
					logger.debug(e.getMessage());
				}
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
	void processParentEvent(String tenantId, String parentTaskId, Handler<AsyncResult<Boolean>> resultHandler) {
//		Promise<Boolean> promise = Promise.promise();
		// lookup parent transfertask
		getDbService().getByUuid(tenantId, parentTaskId, getTaskById -> {
			if (getTaskById.succeeded()) {

				TransferTask parentTask = new TransferTask(getTaskById.result());
				// double check to see if this is a child task
				if ( !StringUtils.isEmpty(parentTask.getParentTaskId()) || !StringUtils.isEmpty(parentTask.getRootTaskId())){
					//This is not a parent.  It is a child task
					resultHandler.handle(Future.succeededFuture(Boolean.FALSE));

				}

				// check whether it's active or not by its status
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
								try {
									_doPublishNatsJSEvent("TRANSFERTASK", TRANSFERTASK_PAUSED, getTaskById.result());
								} catch (Exception e) {
									logger.debug(e.getMessage());
								}
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

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}

	public void processInterrupt(JsonObject body, io.vertx.core.Handler<io.vertx.core.AsyncResult<Boolean>> handler) {}

}


























