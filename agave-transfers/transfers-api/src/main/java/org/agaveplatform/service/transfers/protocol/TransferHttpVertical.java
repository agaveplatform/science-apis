package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.http.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_HTTP;

public class TransferHttpVertical extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferHttpVertical.class);

	public TransferHttpVertical(Vertx vertx) {
		super(vertx);
	}

	public TransferHttpVertical(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = TRANSFER_HTTP;

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

			logger.info("HTTP listener claimed task {} for source {} and dest {}", uuid, source, dest);
			processEvent(body);

			});


		// cancel tasks
		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} cancel detected", uuid);
			//this.interruptedTasks.add(uuid);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
			//this.interruptedTasks.remove(uuid);
		});

		// paused tasks
		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} paused detected", uuid);
			//this.interruptedTasks.add(uuid);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");

			logger.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
			//this.interruptedTasks.remove(uuid);
		});
	}

	public void processEvent(JsonObject body) {
		TransferTask tt = new TransferTask(body);
		String source = tt.getSource();
		String dest = tt.getDest();

		URI srcUri;
		URI destUri;
		RemoteDataClient srcClient = null;
		RemoteDataClient destClient = null;
		try {
			srcUri = URI.create(source);
			destUri = URI.create(dest);
			// pull the system out of the url. system id is the hostname in an agave uri

			srcClient = getRemoteDataClient(body.getString("owner"), srcUri);

			// pull the dest system out of the url. system id is the hostname in an agave uri
			destClient = getRemoteDataClient(body.getString("owner"), destUri);

			HTTP http = new HTTP(srcUri);
			http.get(srcUri.getPath(), destUri.getPath());



			_doPublishEvent(MessageType.TRANSFER_COMPLETED, body);

		}catch (RemoteDataException e){
			logger.error("Remote Data Exception occured for transfertask {}: {}", body.getString("uuid"), e.getMessage());
			JsonObject json = new JsonObject()
					.put("cause", e.getClass().getName())
					.put("message", e.getMessage())
					.mergeIn(body);

			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
		}catch (RemoteCredentialException e){
			logger.error("Remote Credential Exception occured {}",e.getMessage());
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
		}catch (IOException e){
			logger.error("IO Exception occured {}",e.getMessage());
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
		} catch (Exception e) {
			logger.error("Exception occured {}",e.getMessage());
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
		}
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
