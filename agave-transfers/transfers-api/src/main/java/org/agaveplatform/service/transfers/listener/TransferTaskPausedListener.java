package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.exception.TransferException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.messaging.Message;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.local.Local;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
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
	private final List<TransferTask> ttTree = new ArrayList<TransferTask>();
	public Connection nc;

	public TransferTaskPausedListener() throws IOException, InterruptedException {
		super();
	}
	public TransferTaskPausedListener(Vertx vertx) throws IOException, InterruptedException {
		this(vertx, null);
	}
	public TransferTaskPausedListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

//	public Connection getConnection(){return nc;}


	@Override
	public void start() throws TimeoutException, IOException, InterruptedException {
		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		try {
			//group subscription so each message only processed by this vertical type once
			subscribeToSubjectGroup(EVENT_CHANNEL, this::handleMessage);
		} catch (Exception e) {
			logger.error("TRANSFERTASK_PAUSED - Exception {}", e.getMessage());
		}

		try {
			//group subscription so each message only processed by this vertical type once
			subscribeToSubject(TRANSFERTASK_PAUSED_ACK, this::handlePausedAckMessage);
		} catch (Exception e) {
			logger.error("TRANSFERTASK_PAUSED_SYNC - Exception {}", e.getMessage());
		}
	}

	protected void handleMessage(Message message) {
		try {
			JsonObject body = new JsonObject(message.getMessage());
			String uuid = body.getString("uuid");
			getVertx().<Boolean>executeBlocking(
					promise -> processPauseRequest(body, promise),
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

	protected void handlePausedAckMessage(Message message) {
		try {
			JsonObject body = new JsonObject(message.getMessage());
			String uuid = body.getString("uuid");
			getVertx().<Boolean>executeBlocking(
					promise -> processPauseAckRequest(body, promise),
					resp -> {
						if (resp.succeeded()) {
							logger.debug("Finished processing {} for transfer task {}", TRANSFERTASK_PAUSED_ACK, uuid);
						} else {
							logger.debug("Failed  processing {} for transfer task {}", TRANSFERTASK_PAUSED_ACK, uuid);
						}
					});
		} catch (DecodeException e) {
			logger.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
		} catch (Throwable t) {
			logger.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
		}
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

				logger.info("Transfer task {} status updated to PAUSED", uuid);

				// pausing should only happen to root tasks
				if (StringUtils.isNotBlank(targetTransferTask.getRootTaskId()) ||
						StringUtils.isNotBlank(targetTransferTask.getParentTaskId())) {
					logger.info("The root id is {} and the parentID is {}", targetTransferTask.getRootTaskId(), targetTransferTask.getParentTaskId());
					logger.debug("Checking parent task {} for paused transfer task {}.", parentTaskId1, uuid);

					TransferException transferException;
					if ( StringUtils.equals(targetTransferTask.getRootTaskId(),targetTransferTask.getUuid()) ||
							StringUtils.equals(targetTransferTask.getParentTaskId(), targetTransferTask.getUuid())) {
						transferException = new TransferException("Cannot have root task that matches child transfer tasks.");
					}
					else {
						transferException = new TransferException("Cannot cancel non-root transfer tasks.");
					}

					JsonObject json = new JsonObject()
							.put("cause", transferException.getClass().getName() )
							.put("message",transferException.getMessage())
							.mergeIn(body);
					_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json, publishEventResp -> {
						handler.handle(Future.failedFuture(transferException));
					});
				}
				else if (targetTransferTask.getStatus().isActive()) {
					// push the event transfer task onto the queue. this will cause all listening verticals
					// actively processing any of its children to pause their existing work and ack.
					// the ack responses will bubble back up, eventually reaching the root, at which time,
					// the root transfer task will be marked as paused.
					logger.debug("Updating status of transfer task {} to {} prior to sending cancel sync event.",
							uuid, PAUSE_WAITING.name());
					getDbService().updateStatus(tenantId, uuid, PAUSE_WAITING.name(), updateReply -> {
						if (updateReply.succeeded()) {
							// this task is a root task, so we call processParentEvent on its own uuid to check
							// whether its children are all inactive so we can mark the task as paused right
							processParentEvent(tenantId, uuid, processParentReply -> {
								if (processParentReply.succeeded()) {
									logger.debug(String.format("Successfully updated the status of transfer task %s to %s prior " +
													"to sending %s event.",
											uuid, PAUSE_WAITING.name(), TRANSFERTASK_PAUSED_SYNC));
									logger.debug("Sending cancel sync event for transfer task {} to signal children to cancel any active work.", uuid);

									_doPublishEvent(TRANSFERTASK_PAUSED_SYNC, updateReply.result(), handler);
								} else {
									String message = String.format("Failed to process paused ack event for parent " +
											"transfertask %s. %s", parentTaskId, processParentReply.cause());
									JsonObject json = new JsonObject()
											.put("cause", processParentReply.cause().getClass().getName())
											.put("message", message)
											.mergeIn(body);

									_doPublishEvent( MessageType.TRANSFERTASK_PARENT_ERROR, json, errorResp -> {
										handler.handle(Future.succeededFuture(false));
									});
								}
							});

						} else {
							String msg = String.format("Unable to update the status of transfer task %s to %s prior " +
											"to sending %s event. No sync event will be sent.",
									uuid, CANCELING_WAITING.name(), TRANSFERTASK_CANCELED_SYNC);
							doHandleError(updateReply.cause(), msg, body, handler);
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
				doHandleError(reply.cause(), message, body, handler);
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
						_doPublishEvent(MessageType.TRANSFERTASK_PAUSED_COMPLETED, body, pausedCompletedResp -> {
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
													_doPublishEvent( MessageType.TRANSFERTASK_PAUSED_ACK, parentReply.result(), handler);
												} else {
													String message = String.format("Unable to lookup parent task %s for transfer task %s while processing a pause event. %s",
															parentTaskId, uuid, parentReply.cause().getMessage());
													logger.error(message);
													JsonObject json = new JsonObject()
															.put("cause", parentReply.cause().getClass().getName())
															.put("message", message)
															.mergeIn(body);

													_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json, errorResp -> {
														handler.handle(Future.failedFuture(parentReply.cause()));
													});
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
										_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json, errorResp -> {
											handler.handle(Future.failedFuture(siblingReply.cause()));
										});
									}
								});
							} else {
								logger.debug("Skipping processing of parent task for root transfer task {}.", uuid);
								handler.handle(Future.succeededFuture(false));
							}
						});
					} else {
						String message = String.format("Unable to update status of transfer task %s to PAUSED. %s",
								uuid, updateReply.cause().getMessage());
						doHandleError(updateReply.cause(), message, body, handler);
					}
				});
			} else {
				String message = String.format("Unable to lookup child tasks for transfer task %s while processing a pause event. %s",
						uuid, reply.cause().getMessage());
				doHandleError(reply.cause(), message, body, handler);
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
		// lookup parent transfertask
		getDbService().getByUuid(tenantId, parentTaskId, getTaskById -> {
			if (getTaskById.succeeded()) {

				TransferTask parentTask = new TransferTask(getTaskById.result());
				// double check to see if this is a child task
//				if ( !StringUtils.isEmpty(parentTask.getParentTaskId()) || !StringUtils.isEmpty(parentTask.getRootTaskId())){
//					//This is not a parent. It is a child task
//					resultHandler.handle(Future.succeededFuture(Boolean.FALSE));
//				}
				// check whether it's active or not by its status
//				else
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
								_doPublishEvent(MessageType.TRANSFERTASK_PAUSED_ACK, getTaskById.result(), resultHandler);
							} else {
								//logger.debug("Parent transfer task {} has active children. " +
								//		"Skipping further processing ", parentTaskId);
								// parent has active children. let it run
								// return false indicating the parent event was processed, but not paused
								resultHandler.handle(Future.succeededFuture(false));
							}
						} else {
							//logger.debug("Failed to look up children for parent transfer task {}. " +
							//		"Skipping further processing ", parentTaskId);
							// failed to look up child tasks. processing of the parent failed. we
							// forward on the exception to the handler
							resultHandler.handle(Future.failedFuture(isAllChildrenCancelledOrCompleted.cause()));
						}
					});
				}
				else {
					// the parent is not active. forward the ack anyway, just to ensure all ancestors get the message
					_doPublishEvent(MessageType.TRANSFERTASK_PAUSED_ACK, parentTask.toJson(), ackResp -> {
						resultHandler.handle(Future.succeededFuture(false));
					});
				}
			} else {
				//logger.error("Failed to lookup parent transfer task {}: {}", parentTaskId, getTaskById.cause().getMessage());
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

	public void processInterrupt(JsonObject body, io.vertx.core.Handler<io.vertx.core.AsyncResult<Boolean>> handler) {}

	protected RemoteDataClient getRemoteDataClient(String tenantId, String username, URI target) throws NotImplementedException, SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, FileNotFoundException, RemoteDataException {
		TenancyHelper.setCurrentTenantId(tenantId);

		// allow for handling transfer of local files cached to the local (shared) file system. This happens during
		// file uploads and file processing operations between services.
		if (target.getScheme().equalsIgnoreCase("file")) {
			return new Local(null, "/", "/");
		} else {
			return new RemoteDataClientFactory().getInstance(username, null, target);
		}
	}
}


























