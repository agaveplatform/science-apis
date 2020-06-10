package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
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
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.enumerations.TransferStatusType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Date;

public class TransferAllProtocolVertical extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferAllProtocolVertical.class);
	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_ALL;

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
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");
			String source = body.getString("source");
			String dest = body.getString("dest");
			TransferTask tt = new TransferTask(body);

			logger.info("Transfer task {} transferring: {} -> {}", uuid, source, dest);
			processEvent(body);
		});

		// cancel tasks
		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} cancel detected", uuid);
			//this.interruptedTasks.add(uuid);
			super.processInterrupt("add", body);

		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
			super.processInterrupt("remove", body);
			//this.interruptedTasks.remove(uuid);
		});

		// paused tasks
		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} paused detected", uuid);
			super.processInterrupt("add", body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
			super.processInterrupt("remove", body);
		});
	}

	/**
	 * Handles processing of the actual transfer operation using the {@link URLCopy} class to manage the transfer.
	 * A promise is returned wiht the result of the operation. Note that this use of {@link URLCopy} will not create
	 * and update legacy {@link org.iplantc.service.transfer.model.TransferTask} records as it goes.
	 * @param body the transfer all event body
	 * @return a boolean future with the success of the operation.
	 */
	public Future<Boolean> processEvent(JsonObject body) {
		Promise<Boolean> promise = Promise.promise();

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
			// don' tneed this shim
			org.iplantc.service.transfer.model.TransferTask legacyTransferTask = null;
			if (false) {

				// pull the system out of the url. system id is the hostname in an agave uri
				logger.debug("Creating source remote data client to {} for transfer task {}", srcUri.getHost(), tt.getUuid());
				if (false) srcClient = getRemoteDataClient(tt.getTenantId(), tt.getOwner(), srcUri);

				logger.debug("Creating dest remote data client to {} for transfer task {}", destUri.getHost(), tt.getUuid());
				// pull the dest system out of the url. system id is the hostname in an agave uri
				if (false) destClient = getRemoteDataClient(tt.getTenantId(), tt.getOwner(), destUri);

				if (taskIsNotInterrupted(tt)) {
					legacyTransferTask =
							new org.iplantc.service.transfer.model.TransferTask(tt.getSource(), tt.getDest(), tt.getOwner(), null, null);

					legacyTransferTask.setUuid(tt.getUuid());
					legacyTransferTask.setTenantId(tt.getTenantId());
					legacyTransferTask.setStatus(TransferStatusType.valueOf(tt.getStatus().name()));
					legacyTransferTask.setAttempts(tt.getAttempts());
					legacyTransferTask.setBytesTransferred(tt.getBytesTransferred());
					legacyTransferTask.setTotalSize(tt.getTotalSize());
					legacyTransferTask.setCreated(Date.from(tt.getCreated()));
					legacyTransferTask.setLastUpdated(Date.from(tt.getLastUpdated()));
					legacyTransferTask.setStartTime(Date.from(tt.getStartTime()));
					legacyTransferTask.setEndTime(Date.from(tt.getEndTime()));
					if (tt.getParentTaskId() != null) {
						org.iplantc.service.transfer.model.TransferTask legacyParentTask = new org.iplantc.service.transfer.model.TransferTask();
						legacyParentTask.setUuid(tt.getParentTaskId());
						legacyTransferTask.setParentTask(legacyParentTask);
					}
					if (tt.getRootTaskId() != null) {
						org.iplantc.service.transfer.model.TransferTask legacyRootTask = new org.iplantc.service.transfer.model.TransferTask();
						legacyRootTask.setUuid(tt.getRootTaskId());
						legacyTransferTask.setRootTask(legacyRootTask);
					}

					logger.info("Initiating transfer of {} to {} for transfer task {}", source, dest, tt.getUuid());
					result = processCopyRequest(source, srcClient, dest, destClient, legacyTransferTask);
					logger.info("Completed copy of {} to {} for transfer task {} with status {}", source, dest, tt.getUuid(), result);
				} else {
					logger.info("Transfer task {} was interrupted", tt.getUuid());
				}
			}

			logger.debug("Initiating transfer of {} to {} for transfer task {}", source, dest, tt.getUuid());
			logger.debug("Completed transfer of {} to {} for transfer task {} with status {}", source, dest, tt.getUuid(), result);

			_doPublishEvent(MessageType.TRANSFER_COMPLETED, body);
		} catch (RemoteDataException e){
			logger.error("RemoteDataException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			promise.fail(e);
		} catch (RemoteCredentialException e){
			logger.error("RemoteCredentialException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
			promise.fail(e);
		} catch (IOException e){
			logger.error("IOException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
			promise.fail(e);
		} catch (Exception e){
			logger.error("Unexpected Exception occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);

			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			promise.fail(e);
		}

		return promise.future();
	}

	protected Boolean processCopyRequest(String source, RemoteDataClient srcClient, String dest, RemoteDataClient destClient, org.iplantc.service.transfer.model.TransferTask legacyTransferTask)
			throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException {

		URLCopy urlCopy = getUrlCopy(srcClient, destClient);

		legacyTransferTask = urlCopy.copy(source, dest, legacyTransferTask);

		return legacyTransferTask.getStatus() == TransferStatusType.COMPLETED;
	}

	protected URLCopy getUrlCopy(RemoteDataClient srcClient, RemoteDataClient destClient){
		return new URLCopy(srcClient,destClient);
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
	private RemoteDataClient getRemoteDataClient(String tenantId, String username, URI target) throws NotImplementedException, SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, FileNotFoundException, RemoteDataException {
		TenancyHelper.setCurrentTenantId(tenantId);
		return new RemoteDataClientFactory().getInstance(username, null, target);
	}

}
