package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.http.HTTP;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
		try {
			srcUri = URI.create(source);
			destUri = URI.create(dest);
			// pull the system out of the url. system id is the hostname in an agave uri
			RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
			// get a remtoe data client for the sytem
			RemoteDataClient srcClient = srcSystem.getRemoteDataClient();

			// pull the dest system out of the url. system id is the hostname in an agave uri
			RemoteSystem destSystem = new SystemDao().findBySystemId(destUri.getHost());
			RemoteDataClient destClient = destSystem.getRemoteDataClient();

			// stat the remote path to check its type
			RemoteFileInfo fileInfo = srcClient.getFileInfo(srcUri.getPath());

			HTTP http = new HTTP(srcUri);
			http.put(srcUri.getPath(), destUri.getPath());

		}catch (RemoteDataException e){
			logger.error("Remote Data Exception occured {}",e.getMessage());
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
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
		_doPublishEvent(MessageType.TRANSFER_COMPLETED, body);
	}

}
