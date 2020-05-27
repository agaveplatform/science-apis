package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.exception.InterruptableTransferTaskException;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
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

	public TransferAllProtocolVertical(Vertx vertx) {
		super(vertx);
	}

	public TransferAllProtocolVertical(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_ALL;

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	//boolean killOrPauseJob = false;

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
				if (false) srcClient = getRemoteDataClient(body.getString("owner"), srcUri);

				logger.debug("Creating dest remote data client to {} for transfer task {}", destUri.getHost(), tt.getUuid());
				// pull the dest system out of the url. system id is the hostname in an agave uri
				if (false) destClient = getRemoteDataClient(body.getString("owner"), destUri);

				if( ! super.checkTaskInterrupted(tt)) {
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
					logger.info("Job was Canceled or Paused {}", tt.getUuid());
				}
			}

			logger.debug("Initiating transfer of {} to {} for transfer task {}", source, dest, tt.getUuid());
			logger.debug("Completed transfer of {} to {} for transfer task {} with status {}", source, dest, tt.getUuid(), result);

		} catch (RemoteDataException e){
			logger.error("RemoteDataException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			Future.succeededFuture(Boolean.FALSE);
			//return false;

		} catch (RemoteCredentialException e){
			logger.error("RemoteCredentialException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
			Future.succeededFuture(Boolean.FALSE);
//			return e.toString();
		} catch (IOException e){
			logger.error("IOException occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
			Future.succeededFuture(Boolean.FALSE);
//			return e.toString();
		} catch (Exception e){
			logger.error("Unexpected Exception occured for TransferAllVerticle {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);

			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
			Future.succeededFuture(Boolean.FALSE);
//			return e.toString();
		} catch (InterruptableTransferTaskException e) {
			e.printStackTrace();
		}

		_doPublishEvent(MessageType.TRANSFER_COMPLETED, body);
		return Future.succeededFuture(Boolean.TRUE);
	}

	protected Boolean processCopyRequest(String source, RemoteDataClient srcClient, String dest, RemoteDataClient destClient, org.iplantc.service.transfer.model.TransferTask legacyTransferTask)
			throws TransferException, RemoteDataSyntaxException, RemoteDataException, IOException {

		URLCopy urlCopy = getUrlCopy(srcClient, destClient);

		legacyTransferTask = urlCopy.copy(source, dest, legacyTransferTask);

		return legacyTransferTask.getStatus() == TransferStatusType.COMPLETED;
	}

	protected URLCopy getUrlCopy(RemoteDataClient srcClient, RemoteDataClient destClient){
		URLCopy urlCopy = new URLCopy(srcClient,destClient);
		return urlCopy;
	}

	/**
	 * Returns a valid {@link RemoteDataClient} pointing at the system/endpoint represented by the URI. We break
	 * this out into a separate method for easier testing.
	 * @param apiUsername
	 * @param uri
	 * @return
	 */
	protected RemoteDataClient getRemoteDataClient(String apiUsername, URI uri) throws
			SystemUnknownException, AgaveNamespaceException, RemoteCredentialException,
			PermissionException, FileNotFoundException, RemoteDataException {
		return new RemoteDataClientFactory().getInstance(apiUsername, null, uri);
	}

}
