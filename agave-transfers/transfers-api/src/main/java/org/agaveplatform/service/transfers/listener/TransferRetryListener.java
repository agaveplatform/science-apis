package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.exception.MaxTransferTaskAttemptsExceededException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.HashSet;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

public class TransferRetryListener extends AbstractTransferTaskListener{
	private static final Logger log = LoggerFactory.getLogger(TransferRetryListener.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_RETRY;

	protected HashSet<String> interruptedTasks = new HashSet<String>();
	private TransferTaskDatabaseService dbService;

	public TransferRetryListener() {
		super();
	}
	public TransferRetryListener(Vertx vertx) {
		super(vertx);
	}
	public TransferRetryListener(Vertx vertx, String eventChannel) {
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
			String source = body.getString("source");
			String dest = body.getString("dest");
			log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);
			processRetryTransferTask(body);
		});

		// cancel tasks
		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			log.info("Transfer task {} cancel detected", uuid);
			super.processInterrupt("add", body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			log.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
			super.processInterrupt("remove", body);
		});

		// paused tasks
		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			log.info("Transfer task {} paused detected", uuid);
			super.processInterrupt("add", body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			log.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
			super.processInterrupt("remove", body);
		});
	}

	/**
	 * Handles the event body processing. This will lookup the task record, and if in an active state, attempt to retry
	 * task assignment after incrementing the attempt count.
	 * @param body the retry message body.
	 */
	protected void processRetryTransferTask(JsonObject body) {
		String uuid = body.getString("uuid");
		String tenantId = body.getString("tenantId");
		Integer attempts = body.getInteger("attempts");

		// check to see if the uuid is Canceled or Completed
		getDbService().getById(tenantId, uuid, reply -> {
			if (reply.succeeded()) {
				TransferTask transferTaskDb = new TransferTask(new JsonObject(String.valueOf(reply)));
				if (transferTaskDb.getStatus() != TransferStatusType.CANCELLED ||
						transferTaskDb.getStatus() != TransferStatusType.COMPLETED ||
						transferTaskDb.getStatus() != TransferStatusType.FAILED ||
						transferTaskDb.getStatus() != TransferStatusType.TRANSFERRING) {
					// we're good to to go forward.

					// the status is not in the states above.  Now check to see if the # of attempts exceeds the max
					int configMaxTries = config().getInteger("transfertask.max.tries");
					if (configMaxTries <= transferTaskDb.getAttempts()) {
						// # of retries is less.

						// increment the attempts
						transferTaskDb.setAttempts( attempts + 1 );
						getDbService().update(tenantId, uuid, transferTaskDb, updateBody -> {
							if (updateBody.succeeded()) {
								log.debug("Beginning attempt {} for transfer task {}", tenantId, uuid);
								processRetry(new TransferTask(updateBody.result()));
//								promise.handle(Future.succeededFuture(Boolean.TRUE));
							} else {
								log.error("[{}] Task {} update failed: {}",
										tenantId, uuid, reply.cause());
								JsonObject json = new JsonObject()
										.put("cause", updateBody.cause().getClass().getName())
										.put("message", updateBody.cause().getMessage())
										.mergeIn(body);
								_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
//								promise.handle(Future.failedFuture(updateBody.cause()));
							}
						});
					} else {
						String msg = "Maximum attempts have been exceeded for transfer task " + uuid + ". " +
										"No further retries will be attempted.";
						JsonObject json = new JsonObject()
								.put("cause", MaxTransferTaskAttemptsExceededException.class.getName())
								.put("message", msg)
								.mergeIn(body);

						_doPublishEvent(TRANSFERTASK_FAILED, json);
					}
				} else {
					log.debug("Skipping retry of transfer task {}. Task has a status of {} and is no longer in an active state.",
							uuid, transferTaskDb.getStatus().name());
				}
			} else {
				String msg = "Unable to verify the current status of transfer task " + uuid + ". " + reply.cause();
				JsonObject json = new JsonObject()
						.put("cause", reply.cause().getClass().getName())
						.put("message", msg)
						.mergeIn(body);

				_doPublishEvent(TRANSFERTASK_ERROR, json);
			}

		});
	}

	/**
	 * Handles the reassignment of this task. This is nearly identical to what happens in the
	 * {@link TransferTaskAssignedListener#processTransferTask(JsonObject)} method
	 * @param retryTransferTask the updated transfer task to retry
	 */
	public void processRetry(TransferTask retryTransferTask) {
		RemoteDataClient srcClient = null;
		RemoteDataClient destClient = null;

		try {
			URI srcUri;
			URI destUri;
			try {
				srcUri = URI.create(retryTransferTask.getSource());
				destUri = URI.create(retryTransferTask.getDest());
			} catch (Throwable e) {
				String msg = String.format("Unable to parse source uri %s for transfertask %s: %s",
						retryTransferTask.getSource(), retryTransferTask.getUuid(), e.getMessage());
				throw new RemoteDataSyntaxException(msg, e);
			}

			// check for task interruption
			if (taskIsNotInterrupted(retryTransferTask)) {

				// basic sanity check on uri again
				if (RemoteDataClientFactory.isSchemeSupported(srcUri)) {
					// if it's an "agave://" uri, look up the connection info, get a rdc, and process the remote
					// file item
					if (srcUri.getScheme().equalsIgnoreCase("agave")) {
						// pull the system out of the url. system id is the hostname in an agave uri
						RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
						// get a remtoe data client for the sytem
						srcClient = srcSystem.getRemoteDataClient();

						// pull the dest system out of the url. system id is the hostname in an agave uri
						RemoteSystem destSystem = new SystemDao().findBySystemId(destUri.getHost());
						destClient = destSystem.getRemoteDataClient();

						// stat the remote path to check its type
						RemoteFileInfo fileInfo = srcClient.getFileInfo(srcUri.getPath());

						// if the path is a file, then we can move it directly, so we raise an event telling the protocol
						// listener to move the file item
						if (fileInfo.isFile()) {
							// write to the protocol event channel. the uri is all they should need for this....
							// might need tenant id. not sure yet.
							_doPublishEvent("transfer." + srcSystem.getStorageConfig().getProtocol().name().toLowerCase(),
									retryTransferTask.toJson());//"agave://" + srcSystem.getSystemId() + "/" + srcUri.getPath());
						} else {
							// path is a directory, so walk the first level of the directory
							List<RemoteFileInfo> remoteFileInfoList = srcClient.ls(srcUri.getPath());

							// if the directory is emnpty, mark as complete and exit
							if (remoteFileInfoList.isEmpty()) {
								_doPublishEvent(TRANSFER_COMPLETED, retryTransferTask.toJson());
							}
							// if there are contents, walk the first level, creating directories on the remote side
							// as we go to ensure that out of order processing by worker tasks can still succeed.
							else {
								// create the remote directory to ensure it's present when the transfers begin. This
								// also allows us to check for things like permissions ahead of time and save the
								// traversal in the event it's not allowed. While this may have been created in the
								// original attempt, it may also have been deleted, so we ensure it's present
								// on every retry.
								destClient.mkdirs(destUri.getPath());

								for (RemoteFileInfo childFileItem : remoteFileInfoList) {
									// if the assigned or ancestor transfer task were cancelled while this was running,
									// skip the rest.
									if (taskIsNotInterrupted(retryTransferTask)) {
										// if it's a file, we can process this as we would if the original path were a file
										if (childFileItem.isFile()) {
											// build the child paths
											String childSource = retryTransferTask.getSource() + "/" + childFileItem.getName();
											String childDest = retryTransferTask.getDest() + "/" + childFileItem.getName();

											TransferTask transferTask = new TransferTask(childSource, childDest, retryTransferTask.getTenantId());
											transferTask.setTenantId(retryTransferTask.getTenantId());
											transferTask.setOwner(retryTransferTask.getOwner());
											transferTask.setParentTaskId(retryTransferTask.getUuid());
											if (StringUtils.isNotEmpty(retryTransferTask.getRootTaskId())) {
												transferTask.setRootTaskId(retryTransferTask.getRootTaskId());
											}
											_doPublishEvent(MessageType.TRANSFERTASK_CREATED, transferTask.toJson());
										}
										// if a directory, then create a new transfer task to repeat this process,
										// keep the association between this transfer task, the original, and the children
										// in place for traversal in queries later on.
										else {
											// build the child paths
											String childSource = retryTransferTask.getSource() + "/" + childFileItem.getName();
											String childDest = retryTransferTask.getDest() + "/" + childFileItem.getName();

//											// create the remote directory to ensure it's present when the transfers begin. This
//											// also allows us to check for things like permissions ahead of time and save the
//											// traversal in the event it's not allowed.
//											boolean isDestCreated = destClient.mkdirs(destUri.getPath() + "/" + childFileItem.getName());

											TransferTask transferTask = new TransferTask(childSource, childDest, retryTransferTask.getTenantId());
											transferTask.setTenantId(retryTransferTask.getTenantId());
											transferTask.setOwner(retryTransferTask.getOwner());
											transferTask.setParentTaskId(retryTransferTask.getUuid());
											if (StringUtils.isNotEmpty(retryTransferTask.getRootTaskId())) {
												transferTask.setRootTaskId(retryTransferTask.getRootTaskId());
											}
											_doPublishEvent(MessageType.TRANSFERTASK_CREATED, transferTask.toJson());
										}
									} else {
										// interrupt happened wild processing children. skip the rest.
										log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", retryTransferTask.getUuid());
										_doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, retryTransferTask.toJson());
										break;
									}
								}
							}
						}
					}
					// it's not an agave uri, so we forward on the raw uri as we know that we can
					// handle it from the wrapping if statement check
					else {
						_doPublishEvent("transfer." + srcUri.getScheme(), retryTransferTask.toJson());
					}
				} else {
					String msg = String.format("Unknown source schema %s for the transfertask %s",
							srcUri.getScheme(), retryTransferTask.getUuid());
					throw new RemoteDataSyntaxException(msg);
				}
			} else {
				// task was interrupted, so don't attempt a retry
				log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", retryTransferTask.getUuid());
				_doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, retryTransferTask.toJson());
//
//					// tell everyone else that you killed this task
//					// also set the status to CANCELLED
//					Promise<Boolean> promise = Promise.promise();
//					retryTransferTask.setStatus(TransferStatusType.CANCELLED);
//
//					getDbService().update(tenantId, uuid, retryTransferTask, updateBody -> {
//						if (updateBody.succeeded()) {
//							logger.info("[{}] Transfer task {} updated.", tenantId, uuid);
//							promise.handle(Future.succeededFuture(Boolean.TRUE));
//						} else {
//							logger.error("[{}] Task {} retry failed",
//									tenantId, uuid);
//							JsonObject json = new JsonObject()
//									.put("cause", updateBody.cause().getClass().getName())
//									.put("message", updateBody.cause().getMessage())
//									.mergeIn(body);
//							_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
//							promise.handle(Future.failedFuture(updateBody.cause()));
//						}
//					});
//
//					throw new InterruptedException(String.format("Transfer task %s interrupted due to cancel event", uuid));
				}
		}
		catch (RemoteDataSyntaxException e) {
			String message = String.format("Failing transfer task %s due to invalid source syntax. %s",
					retryTransferTask.getUuid(), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", message)
					.mergeIn(retryTransferTask.toJson());

			_doPublishEvent(TRANSFERTASK_FAILED, json);
		}
		catch (Exception e) {
			log.error(e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(retryTransferTask.toJson());

			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
		} finally {
			// cleanup the remote data client connections
			try { if (srcClient != null) srcClient.disconnect(); } catch (Exception ignored) {}
			try { if (destClient != null) destClient.disconnect(); } catch (Exception ignored) {}
		}
	}



//	/**
//	 * Checks whether the transfer task or any of its children exist in the list of
//	 * interrupted tasks.
//	 *
//	 * @param transferTask the current task being checked from the running task
//	 * @return true if the transfertask's uuid, parentTaskId, or rootTaskId are in the {@link #isTaskInterrupted(TransferTask)} list
//	 */


	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}


}
