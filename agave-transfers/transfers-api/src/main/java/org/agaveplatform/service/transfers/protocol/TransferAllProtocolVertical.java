package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Future;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.NotImplementedException;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.local.Local;
import org.iplantc.service.transfer.model.TransferTaskImpl;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.channels.ClosedByInterruptException;
import java.util.TimeZone;
import java.util.concurrent.*;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELED_ACK;

public class TransferAllProtocolVertical extends AbstractTransferTaskListener {
	private static final Logger log = LoggerFactory.getLogger(TransferAllProtocolVertical.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_ALL;
	private TransferTaskDatabaseService dbService;
	protected WorkerExecutor executor;

	public TransferAllProtocolVertical() {
		super();
	}
	public TransferAllProtocolVertical(Vertx vertx) {
		super(vertx);
	}
	public TransferAllProtocolVertical(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {
		// when called from multiple verticles using the same executor pool name, vertx only creates a single pool
		// that is shared among all verticles. In this case, we create a pool of size 20 with a 72 hour limit per
		// task, and share that among all TransferAllProtocolVertical instances.


		DateTimeZone.setDefault(DateTimeZone.forID("America/Chicago"));
		TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"));

		EventBus bus = vertx.eventBus();
		log.debug("Got into TransferAllProtocolVertical");


		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		bus.<JsonObject>consumer(getEventChannel(), msg -> {
            msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

            JsonObject body = msg.body();
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");

			log.info("Transfer task (ALL) {} transferring: {} -> {}", uuid, source, dest);
			processEvent(body, resp -> {
				if (resp.succeeded()) {
					log.debug("Completed processing (ALL) {} event for transfer task (TA) {}", getEventChannel(), uuid);
				} else {
					log.error("Unable to process (ALL) {} event for transfer task (TA) message: {}", getEventChannel(), body.encode(), resp.cause());
				}
			});
		});

		// cancel tasks
		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
            msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

            JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			log.info("Transfer task {} cancel detected", uuid);
			if (uuid != null) {
				addCancelledTask(uuid);
			}
		});

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
            msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

			log.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
			if (uuid != null) {
				removeCancelledTask(uuid);
			}
		});

        // paused tasks
        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
            msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

			log.info("Transfer task {} paused detected", uuid);
			if (uuid != null) {
				addPausedTask(uuid);
			}
		});

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
            msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

			log.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
			if (uuid != null) {
				addPausedTask(uuid);
			}
		});
	}

	/**
	 * Handles processing of the actual transfer operation using the {@link URLCopy} class to manage the transfer.
	 * A promise is returned wiht the result of the operation. Note that this use of {@link URLCopy} will not create
	 * and update legacy {@link TransferTaskImpl} records as it goes.
	 * @param body the transfer all event body
	 * @param handler the callback receiving the result of the event processing
	 */
	public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
		log.debug("Got into TransferAllProtocolVertical.processEvent");

		TransferTask tt = new TransferTask(body);
		String source = tt.getSource();
		String dest = tt.getDest();
		Boolean result = true;
		URI srcUri;
		URI destUri;
		RemoteDataClient srcClient = null;
		RemoteDataClient destClient = null;

		try {
			srcUri = URI.create(source);
			destUri = URI.create(dest);

			// stat the remote path to check its type
			//RemoteFileInfo fileInfo = srcClient.getFileInfo(srcUri.getPath());

			// migrate from the current transfertask format passed in as serialized json
			// to the legacy transfer task format managed by hibernate. Different db,
			// different packages, this won't work for real, but it will allow us to
			// smoke test this method with real object. We'll port the url copy class
			// over in the coming week to handle current transfertask objects so we
			// don't need this shim
			if (taskIsNotInterrupted(tt)) {
				// pull the system out of the url. system id is the hostname in an agave uri
				log.debug("Creating source remote data client to {} for transfer task {}", srcUri.getHost(), tt.getUuid());
				srcClient = getRemoteDataClient(tt.getTenantId(), tt.getOwner(), srcUri);

				log.debug("Creating dest remote data client to {} for transfer task {}", destUri.getHost(), tt.getUuid());
				// pull the dest system out of the url. system id is the hostname in an agave uri
				destClient = getRemoteDataClient(tt.getTenantId(), tt.getOwner(), destUri);

				WorkerExecutor executor = getVertx().createSharedWorkerExecutor("check-cancel-child-all-task-worker-pool");
				RemoteDataClient finalSrcClient = srcClient;
				RemoteDataClient finalDestClient = destClient;

//				executor.executeBlocking(promise -> {
					String taskId = (tt.getRootTaskId() == null) ? tt.getUuid() : tt.getRootTaskId();

					getDbService().getByUuid(tt.getTenantId(), taskId, getByUuidResp -> {
						if (getByUuidResp.succeeded()) {
							TransferTask targetTransferTask = new TransferTask(getByUuidResp.result());
							if (targetTransferTask.getStatus().isActive()) {
//								TransferTask resultingTransferTask = new TransferTask();
								try {
									log.info("Initiating worker transfer of {} to {} for transfer task {}", source, dest, tt.getUuid());

//									resultingTransferTask =
									processCopyRequest(finalSrcClient, finalDestClient, tt, handler);

//									promise.complete();
								} catch (Exception e) {
									log.error("Failed to copy Transfer Task {}", tt.toJSON());
									JsonObject json = new JsonObject()
											.put("cause", e.getClass().getName())
											.put("message", e.getMessage())
											.mergeIn(body);
									_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
									handler.handle(Future.failedFuture(e.getMessage()));
//									promise.fail(e.getMessage());
								}
							} else {
								log.info("Worker Transfer task {} was interrupted", tt.getUuid());
								_doPublishEvent(TRANSFERTASK_CANCELED_ACK, tt.toJson());
								handler.handle(Future.succeededFuture(false));
//								promise.complete();
							}
						} else {
							log.error("Failed to get status of parent Transfer Task {}, {}", tt.getParentTaskId(), tt.toJSON());
							JsonObject json = new JsonObject()
									.put("cause", getByUuidResp.cause())
									.put("message", getByUuidResp.cause().getMessage())
									.mergeIn(body);
							_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
							handler.handle(Future.failedFuture(getByUuidResp.cause()));
//							promise.fail("Failed to retrieve status....");
						}
					});
//				}, res -> {
//				});
			} else {
				// task was interrupted, so don't attempt a retry
				log.info("Skipping transfer of transfer tasks {} due to interrupt event.", tt.getUuid());
				_doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, body);
				handler.handle(Future.failedFuture(new ClosedByInterruptException()));
			}
		} catch (RemoteDataException e){
			log.error("RemoteDataException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			handler.handle(Future.failedFuture(e));
		} catch (RemoteCredentialException e){
			log.error("RemoteCredentialException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			handler.handle(Future.failedFuture(e));
		} catch (IOException e) {
			log.error("IOException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			handler.handle(Future.failedFuture(e));
		} catch (Exception e){
			log.error("Unexpected Exception occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);

			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			handler.handle(Future.failedFuture(e));
		}
	}

	/**
	 * Creates {@link WorkerExecutor} to run the {@link URLCopy} operation in the background. Upon completion, a
	 * {@link MessageType#TRANSFER_COMPLETED} event will be thrown signalling completion of the transfer.
	 * @param srcClient the remote data client to the source of the transfer task
	 * @param destClient the remote data client to the destination of the transfer task
	 * @param transferTask the transfer task to update with progress of the transfer.
	 * @return the completed transfer task.
	 * @throws RemoteDataException when a connection cannot be made to the {@link RemoteSystem}
	 * @throws IOException if unable to connect to the remote host.
	 */
	protected void processCopyRequest(RemoteDataClient srcClient, RemoteDataClient destClient, TransferTask transferTask, Handler<AsyncResult<Boolean>> handler) {
//			throws RemoteDataException, IOException {
		log.debug("Got into TransferAllProtocolVertical.processCopyRequest ");

		log.trace("Got up to the urlCopy");

		// TODO: pass in a {@link RemoteTransferListener} after porting this class over so the listener can check for
		//   interrupts in this method upon updates from the transfer thread and interrupt it. Alternatively, we can
		//   just run the transfer in an observable and interrupt it via a timer task started by vertx.
		URLCopy urlCopy = getUrlCopy(srcClient, destClient);

		getExecutor().executeBlocking(promise -> {

			ExecutorService executorService = Executors.newSingleThreadExecutor();
			CallableUrlCopy callableUrlQuery = new CallableUrlCopy(urlCopy, transferTask);
			java.util.concurrent.Future<TransferTask> copyFuture = executorService.submit(callableUrlQuery);
			try {
				int i=-1;
				while (!copyFuture.isDone()) {
					i++;
					log.debug("[{}] URLCopy task is not yet complete.", i);
					// if the transfer task has been externally interrupted via an incoming message,
					// cancel the executor.
					if (taskIsNotInterrupted(transferTask)) {
						log.debug("[{}] Waiting for interrupt", i);
						// check every 500 ms for completion
						Thread.sleep(500);
					} else {
						log.debug("[{}] Interrupt found.", i);
						callableUrlQuery.urlCopy.setKilled(true);
						copyFuture.cancel(true);
						break;
					}
				}

				if (copyFuture.isCancelled()) {
					// probably need to sync the transfer task since it's changed since the inital call to urlcopy
					log.debug("Completed interrupt of Transfer Task {}, {}", transferTask.getUuid(), transferTask.toJSON() );
					_doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, transferTask.toJson());

					promise.fail(new InterruptedException("URLCopy interrupted by external process."));
				} else {
					log.debug("URLCopy completed successfully.");
					TransferTask finishedTask = copyFuture.get();
					promise.complete(finishedTask != null);
				}
			} catch (ExecutionException | InterruptedException e) {
				log.error("Timeout received executing task. {}", e.getMessage());
				promise.fail(e);
			} catch (Exception e) {
				log.error("Unexpected exception received from task {}. {}", transferTask.getUuid(), e.getMessage() );
				promise.fail(e);
			} finally {
				log.debug("Shutting down URLCopy executor service.");
				// ensure we shut down the executor service no matter what
				executorService.shutdown();
				if (!executorService.isTerminated()) {
					log.error("Cancelling non-finished urlcopy operation");
				}
				executorService.shutdownNow();
				log.debug("URLCopy cancellation finished");
			}

		}, handler);


	}

	class CallableUrlCopy implements Callable<TransferTask> {
		URLCopy urlCopy;
		TransferTask transferTask;
		CallableUrlCopy(URLCopy urlCopy, TransferTask transferTask) {
			this.urlCopy = urlCopy;
			this.transferTask = transferTask;
		}

		@Override
		public TransferTask call() throws Exception {
			try {
				TransferTask finishedTask = urlCopy.copy(transferTask);
				_doPublishEvent(MessageType.TRANSFER_COMPLETED, finishedTask.toJson());
				log.info("Completed copy of {} to {} for transfer task {} with status {}", finishedTask.getSource(),
						finishedTask.getDest(), finishedTask.getUuid(), finishedTask);
				return finishedTask;
			} catch (InterruptedException|ClosedByInterruptException e) {
				throw e;
			} catch (Exception e) {
				log.error("Failed to copy Transfer Task {}, {}", transferTask.getUuid(), transferTask.toJSON() );
				JsonObject json = new JsonObject()
						.put("cause", e.getClass().getName())
						.put("message", e.getMessage())
						.mergeIn(transferTask.toJson());
				_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
				throw e;
			}
		}
	}

	/**
	 * Mockable method to instantiate an instance of the {@link URLCopy} using the current {@link #getVertx()} instance
	 * and method arguments.
	 * @param srcClient the remote data client to the source of the transfer task
	 * @param destClient the remote data client to the destination of the transfer task
	 * @return an instance of a {@link URLCopy} initialized to perform the transfer task.
	 */
	protected URLCopy getUrlCopy(RemoteDataClient srcClient, RemoteDataClient destClient){
		return new URLCopy(srcClient, destClient, getVertx(), getRetryRequestManager());
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

		// allow for handling transfer of local files cached to the local (shared) file system. This happens during
		// file uploads and file processing operations between services.
		if (target.getScheme() == null || target.getScheme().equalsIgnoreCase("file")) {
			return new Local(null, "/", "/");
		} else {
			return new RemoteDataClientFactory().getInstance(username, null, target);
		}
	}

	/**
	 * If your verticle has simple synchronous clean-up tasks to complete then override this method and put your clean-up
	 * code in here.
	 *
	 * @throws Exception
	 */
	@Override
	public void stop() throws Exception {
		try {
			getExecutor().close();
		} catch (Exception e) {
			log.debug("Failed to close transfer executor", e);
		}
		super.stop();
	}

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}

	/**
	 * Get the executor pool handling url copy operations
	 * @return the executor pool
	 */
	protected WorkerExecutor getExecutor() {
		if (executor == null) {
			executor = getVertx().createSharedWorkerExecutor("child-all-task-worker-pool", 20, 72, TimeUnit.HOURS);
		}
		return executor;
	}

	/**
	 * @param executor the executor pool to set
	 */
	protected void setExecutor(WorkerExecutor executor) {
		this.executor = executor;
	}
}
