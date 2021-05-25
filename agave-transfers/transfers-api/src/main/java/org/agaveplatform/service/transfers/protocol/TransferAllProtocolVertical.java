package org.agaveplatform.service.transfers.protocol;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.TransferTaskConfigProperties;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractNatsListener;
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
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.TransferTaskImpl;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_CANCELED_ACK;

public class TransferAllProtocolVertical extends AbstractNatsListener {
	private static final Logger log = LoggerFactory.getLogger(TransferAllProtocolVertical.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_ALL;
	private TransferTaskDatabaseService dbService;

	public TransferAllProtocolVertical() throws IOException, InterruptedException {
		super();
		setConnection();
	}
	public TransferAllProtocolVertical(Vertx vertx) throws IOException, InterruptedException {
		super(vertx);
		setConnection();
	}
	public TransferAllProtocolVertical(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
		super(vertx, eventChannel);
		setConnection();
	}

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}
	public Connection nc;

	public Connection getConnection(){return nc;}

	public void setConnection() throws IOException, InterruptedException {
		try {
			nc = _connect(config().getString(TransferTaskConfigProperties.NATS_URL));
		} catch (IOException e) {
			//use default URL
			nc = _connect(Options.DEFAULT_URL);
		}
	}

	@Override
	public void start() throws IOException, InterruptedException, TimeoutException {
		//EventBus bus = vertx.eventBus();
		DateTimeZone.setDefault(DateTimeZone.forID("America/Chicago"));
		TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"));

		EventBus bus = vertx.eventBus();
		log.debug("Got into TransferAllProtocolVertical");

		// init our db connection from the pool
		String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
		dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		//Connection nc = _connect();
		Dispatcher d = getConnection().createDispatcher((msg) -> {});
		//bus.<JsonObject>consumer(getEventChannel(), msg -> {
		Subscription s = d.subscribe(MessageType.TRANSFER_ALL, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");
            //msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

			log.info("Transfer task (ALL) {} transferring: {} -> {}", uuid, source, dest);
			processEvent(body, resp -> {
				if (resp.succeeded()) {
					log.debug("Completed processing (ALL) {} event for transfer task (TA) {}", getEventChannel(), uuid);
				} else {
					log.error("Unable to process (ALL) {} event for transfer task (TA) message: {}", getEventChannel(), body.encode(), resp.cause());
				}
			});
		});
		d.subscribe(MessageType.TRANSFER_ALL);
		getConnection().flush(Duration.ofMillis(500));


		// cancel tasks
		//bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
		s = d.subscribe(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");
            //msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

			log.info("Transfer task {} cancel detected", uuid);
			if (uuid != null) {
				addCancelledTask(uuid);
			}
		});
		d.subscribe(MessageType.TRANSFERTASK_CANCELED_SYNC);
		getConnection().flush(Duration.ofMillis(500));



        //bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
		s = d.subscribe(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");
            //msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

			log.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
			if (uuid != null) {
				removeCancelledTask(uuid);
			}
		});
		d.subscribe(MessageType.TRANSFERTASK_CANCELED_COMPLETED);
		getConnection().flush(Duration.ofMillis(500));


        // paused tasks
        //bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
		s = d.subscribe(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");
            //msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

			log.info("Transfer task {} paused detected", uuid);
			if (uuid != null) {
				addPausedTask(uuid);
			}
		});
		d.subscribe(MessageType.TRANSFERTASK_PAUSED_SYNC);
		getConnection().flush(Duration.ofMillis(500));



        //bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
		s = d.subscribe(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
			//msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
			String response = new String(msg.getData(), StandardCharsets.UTF_8);
			JsonObject body = new JsonObject(response) ;
			String uuid = body.getString("uuid");
			//msg.reply(TransferAllProtocolVertical.class.getName() + " received.");

			log.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
			if (uuid != null) {
				addPausedTask(uuid);
			}
		});
		d.subscribe(MessageType.TRANSFERTASK_PAUSED_COMPLETED);
		getConnection().flush(Duration.ofMillis(500));


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
			org.iplantc.service.transfer.model.TransferTask legacyTransferTask;
			boolean makeRealCopy = true;
			if (makeRealCopy) {

				// pull the system out of the url. system id is the hostname in an agave uri
				log.debug("Creating source remote data client to {} for transfer task {}", srcUri.getHost(), tt.getUuid());
				if (makeRealCopy) srcClient = getRemoteDataClient(tt.getTenantId(), tt.getOwner(), srcUri);

				log.debug("Creating dest remote data client to {} for transfer task {}", destUri.getHost(), tt.getUuid());
				// pull the dest system out of the url. system id is the hostname in an agave uri
				if (makeRealCopy) destClient = getRemoteDataClient(tt.getTenantId(), tt.getOwner(), destUri);

                WorkerExecutor executor = getVertx().createSharedWorkerExecutor("check-cancel-child-all-task-worker-pool");
                RemoteDataClient finalSrcClient = srcClient;
                RemoteDataClient finalDestClient = destClient;

                executor.executeBlocking(promise -> {
                	String taskId = (tt.getRootTaskId() == null) ? tt.getUuid() : tt.getRootTaskId();
                        getDbService().getByUuid(tt.getTenantId(), taskId, checkCancelled -> {
                            if (checkCancelled.succeeded()){
                                TransferTask targetTransferTask = new TransferTask(checkCancelled.result());
                                if (targetTransferTask.getStatus().isActive()){
                                    TransferTask resultingTransferTask = new TransferTask();
                                    try {
                                        log.info("Initiating worker transfer of {} to {} for transfer task {}", source, dest, tt.getUuid());

                                        resultingTransferTask = processCopyRequest(finalSrcClient, finalDestClient, tt);
                                        handler.handle(Future.succeededFuture(result));
                                        promise.complete();
                                    } catch (Exception ex) {
										log.error("Failed to copy Transfer Task {}", tt.toJSON() );
										JsonObject json = new JsonObject()
												.put("cause", ex.getClass().getName())
												.put("message", ex.getMessage())
												.mergeIn(body);
										try {
											_doPublishNatsJSEvent(MessageType.TRANSFERTASK_ERROR, json);
										} catch (Exception e) {
											log.debug(e.getMessage());
										}
										handler.handle(Future.failedFuture(ex.getMessage()));
										promise.fail(ex.getMessage());
									}
                                } else {
                                    log.info("Worker Transfer task {} was interrupted", tt.getUuid());
                                    try {
										_doPublishNatsJSEvent(TRANSFERTASK_CANCELED_ACK, tt.toJson());
									} catch (Exception e) {
										log.debug(e.getMessage());
									}
                                    handler.handle(Future.succeededFuture(false));
                                    promise.complete();
                                }
                            } else {
								log.error("Failed to get status of parent Transfer Task {}, {}", tt.getParentTaskId(), tt.toJSON());
								JsonObject json = new JsonObject()
										.put("cause", checkCancelled.cause())
										.put("message", checkCancelled.cause().getMessage())
										.mergeIn(body);
								try {
									_doPublishNatsJSEvent(MessageType.TRANSFERTASK_ERROR, json);
								} catch (Exception e) {
									log.debug(e.getMessage());
								}
                                handler.handle(Future.failedFuture(checkCancelled.cause()));
                                promise.fail("Failed to retrieve status....");
                            }
                        });
                    }, res -> {
					});

			} else {
				log.debug("Initiating fake transfer of {} to {} for transfer task {}", source, dest, tt.getUuid());
				log.debug("Completed fake transfer of {} to {} for transfer task {} with status {}", source, dest, tt.getUuid(), result);

//				_doPublishEvent(MessageType.TRANSFER_COMPLETED, body);
				handler.handle(Future.succeededFuture(true));
			}
		} catch (RemoteDataException ex){
			log.error("RemoteDataException occured for TransferAllVerticle {}: {}", body.getString("uuid"), ex.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", ex.getClass().getName())
					.put("message", ex.getMessage())
					.mergeIn(body);
			try {
				_doPublishNatsJSEvent(MessageType.TRANSFERTASK_ERROR, json);
			} catch (Exception e) {
				log.debug(e.getMessage());
			}
			handler.handle(Future.failedFuture(ex));

		} catch (RemoteCredentialException ex){
			log.error("RemoteCredentialException occured for TransferAllVerticle {}: {}", body.getString("uuid"), ex.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", ex.getClass().getName())
					.put("message", ex.getMessage())
					.mergeIn(body);
			try {
				_doPublishNatsJSEvent(MessageType.TRANSFERTASK_ERROR, json);
			} catch (Exception e) {
				log.debug(e.getMessage());
			}
			handler.handle(Future.failedFuture(ex));
		} catch (IOException ex){
			log.error("IOException occured for TransferAllVerticle {}: {}", body.getString("uuid"), ex.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", ex.getClass().getName())
					.put("message", ex.getMessage())
					.mergeIn(body);
			try {
				_doPublishNatsJSEvent(MessageType.TRANSFERTASK_ERROR, json);
			} catch (Exception e) {
				log.debug(e.getMessage());
			}
			handler.handle(Future.failedFuture(ex));
		} catch (Exception ex){
			log.error("Unexpected Exception occured for TransferAllVerticle {}: {}", body.getString("uuid"), ex.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", ex.getClass().getName())
					.put("message", ex.getMessage())
					.mergeIn(body);
			try {
				_doPublishNatsJSEvent(MessageType.TRANSFERTASK_ERROR, json);
			} catch (Exception e) {
				log.debug(e.getMessage());
			}
			handler.handle(Future.failedFuture(ex));
		}
	}

	protected TransferTask processCopyRequest(RemoteDataClient srcClient, RemoteDataClient destClient, TransferTask transferTask)
			throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException, InterruptedException {
		log.debug("Got into TransferAllProtocolVertical.processCopyRequest ");

		log.trace("Got up to the urlCopy");

		// TODO: pass in a {@link RemoteTransferListener} after porting this class over so the listener can check for
		//   interrupts in this method upon updates from the transfer thread and interrupt it. Alternatively, we can
		//   just run the transfer in an observable and interrupt it via a timer task started by vertx.
		URLCopy urlCopy = getUrlCopy(srcClient, destClient);

		log.debug("Calling urlCopy.copy");
		TransferTask updatedTransferTask = null;

		WorkerExecutor executor = getVertx().createSharedWorkerExecutor("child-all-task-worker-pool");
		executor.executeBlocking(promise -> {
			TransferTask finishedTask = null;
			try {
				finishedTask = urlCopy.copy(transferTask);
				_doPublishNatsJSEvent(MessageType.TRANSFER_COMPLETED, finishedTask.toJson());
				promise.complete();
				log.info("Completed copy of {} to {} for transfer task {} with status {}", finishedTask.getSource(),
						finishedTask.getDest(), finishedTask.getUuid(), finishedTask);
			} catch (Exception e) {
				log.error("Failed to copy Transfer Task {}, {}", transferTask.getUuid(), transferTask.toJSON() );
				JsonObject json = new JsonObject()
						.put("cause", e.getClass().getName())
						.put("message", e.getMessage())
						.mergeIn(transferTask.toJson());
				try {
					_doPublishNatsJSEvent(MessageType.TRANSFERTASK_ERROR, json);
				} catch (Exception ex) {
					log.debug(ex.getMessage());
				}
				promise.fail(e.getMessage());
			}
		}, res -> {
		});

		return updatedTransferTask;
	}

	protected URLCopy getUrlCopy(RemoteDataClient srcClient, RemoteDataClient destClient) throws IOException, InterruptedException {
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
		return new RemoteDataClientFactory().getInstance(username, null, target);
	}

	public TransferTaskDatabaseService getDbService() {
		return dbService;
	}

	public void setDbService(TransferTaskDatabaseService dbService) {
		this.dbService = dbService;
	}

}
