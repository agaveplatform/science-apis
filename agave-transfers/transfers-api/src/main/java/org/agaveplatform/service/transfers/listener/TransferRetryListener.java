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
import org.agaveplatform.service.transfers.exception.MaxTransferTaskAttemptsExceededException;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.HashSet;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

public class TransferRetryListener extends AbstractTransferTaskListener {
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
			//processRetryTransferTask(body);
			try {
				processRetryTransferTask(body, resp -> {
					if (resp.succeeded()) {
						log.error("Succeeded with the procdessTransferTask in the assigning of the event {}", uuid);
						_doPublishEvent(MessageType.NOTIFICATION_TRANSFERTASK, body);
					} else {
						log.error("Error with return from creating the event {}", uuid);
						_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
					}
				});
			} catch (Exception e) {
				log.error("Error with the TRANSFER_RETRY message.  The error is {}", e.getMessage());
				_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
			}
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
	 *
	 * @param body the retry message body.
	 */
	protected void processRetryTransferTask(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
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
						transferTaskDb.setAttempts(attempts + 1);
						getDbService().update(tenantId, uuid, transferTaskDb, updateBody -> {
							if (updateBody.succeeded()) {
								log.debug("Beginning attempt {} for transfer task {}", tenantId, uuid);
								processRetry(new TransferTask(updateBody.result()), handler);
							} else {
								log.error("[{}] Task {} update failed: {}",
										tenantId, uuid, reply.cause());
								JsonObject json = new JsonObject()
										.put("cause", updateBody.cause().getClass().getName())
										.put("message", updateBody.cause().getMessage())
										.mergeIn(body);
								_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
								handler.handle(Future.succeededFuture(false));
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
						handler.handle(Future.succeededFuture(false));
					}
				} else {
					log.debug("Skipping retry of transfer task {}. Task has a status of {} and is no longer in an active state.",
							uuid, transferTaskDb.getStatus().name());
					handler.handle(Future.succeededFuture(true));
				}
			} else {
				String msg = "Unable to verify the current status of transfer task " + uuid + ". " + reply.cause();
				JsonObject json = new JsonObject()
						.put("cause", reply.cause().getClass().getName())
						.put("message", msg)
						.mergeIn(body);

				_doPublishEvent(TRANSFERTASK_ERROR, json);
				handler.handle(Future.succeededFuture(false));
			}

		});
		handler.handle(Future.succeededFuture(true));
	}

	/**
	 * Handles the reassignment of this task. This is nearly identical to what happens in the
	 * {@link TransferTaskAssignedListener#processTransferTask(JsonObject, Handler)} method
	 *
	 * @param retryTransferTask the updated transfer task to retry
	 * @param handler the callback to the calling method with the boolean async result of the processing task
	 */
	public void processRetry(TransferTask retryTransferTask, Handler<AsyncResult<Boolean>> handler) {
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
					// TODO: examine whether we want to collapse this condition and expand out all transfers regardless
					//   of system registration or not. By not doing that, the protocol vertical has to do directory
					//   copies every time
					if (srcUri.getScheme().equalsIgnoreCase("agave")) {
						// get a remote data client for the source and dest system
						srcClient = getRemoteDataClient(retryTransferTask.getTenantId(), retryTransferTask.getOwner(), srcUri);
						destClient = getRemoteDataClient(retryTransferTask.getTenantId(), retryTransferTask.getOwner(), destUri);

						// stat the remote path to check its type
						RemoteFileInfo fileInfo = srcClient.getFileInfo(srcUri.getPath());

						// if the path is a file, then we can move it directly, so we raise an event telling the protocol
						// listener to move the file item
						if (fileInfo.isFile()) {
							// write to the protocol event channel. the uri is all they should need for this....
							// might need tenant id. not sure yet.
							_doPublishEvent(TRANSFER_ALL, retryTransferTask.toJson());
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
						_doPublishEvent(TRANSFER_ALL, retryTransferTask.toJson());
					}
				} else {
					String msg = String.format("Unknown source schema %s for the transfertask %s",
							srcUri.getScheme(), retryTransferTask.getUuid());
					throw new RemoteDataSyntaxException(msg);
				}
				handler.handle(Future.succeededFuture(true));
			} else {
				// task was interrupted, so don't attempt a retry
				log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", retryTransferTask.getUuid());
				_doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, retryTransferTask.toJson());
				handler.handle(Future.succeededFuture(false));
			}
		} catch (RemoteDataSyntaxException e) {
			String message = String.format("Failing transfer task %s due to invalid source syntax. %s",
					retryTransferTask.getUuid(), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", message)
					.mergeIn(retryTransferTask.toJson());

			_doPublishEvent(TRANSFERTASK_FAILED, json);
			handler.handle(Future.failedFuture(e));
		} catch (Exception e) {
			log.error(e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(retryTransferTask.toJson());

			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			handler.handle(Future.failedFuture(e));
		} finally {
			// cleanup the remote data client connections
			try {
				if (srcClient != null) srcClient.disconnect();
			} catch (Exception ignored) {
			}
			try {
				if (destClient != null) destClient.disconnect();
			} catch (Exception ignored) {
			}
		}
		handler.handle(Future.succeededFuture(true));
	}

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}

	/**
	 * Obtains a new {@link RemoteDataClient} for the given {@code uri}. The schema and hostname are used to identify
	 * agave {@link RemoteSystem} URI vs externally accesible URI. Tenancy is honored.
	 * @param tenantId the tenant whithin which any system lookups should be made
	 * @param username the user for whom the system looks should be made
	 * @param target the uri from which to parse the system info
	 * @return a new instance of a {@link RemoteDataClient} for the given {@code target}
	 * @throws SystemUnknownException if the sytem is unknown
	 * @throws AgaveNamespaceException if the URI does match any known agave uri pattern
	 * @throws RemoteCredentialException if the credentials for the system represented by the URI cannot be found/refreshed/obtained
	 * @throws PermissionException when the user does not have permission to access the {@code target}
	 * @throws FileNotFoundException when the remote {@code target} does not exist
	 * @throws RemoteDataException when a connection cannot be made to the {@link RemoteSystem}
	 * @throws NotImplementedException when the schema is not supported
	 */
	protected RemoteDataClient getRemoteDataClient(String tenantId, String username, URI target) throws NotImplementedException, SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, FileNotFoundException, RemoteDataException {
		TenancyHelper.setCurrentTenantId(tenantId);
		return new RemoteDataClientFactory().getInstance(username, null, target);
	}
}
