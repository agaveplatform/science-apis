package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.exec.ExecuteException;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.URLCopy;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class TransferSftpVertical extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferSftpVertical.class);

	public TransferSftpVertical(Vertx vertx) {
		super(vertx);
	}

	public TransferSftpVertical(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_SFTP;

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

			logger.info("SFTP listener claimed task {} for source {} and dest {}", uuid, source, dest);
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
		String uuid = tt.getUuid();
		String owner = tt.getOwner();
		URI srcUri;
		URI destUri;
		try {
			srcUri = URI.create(source);
			destUri = URI.create(dest);
//		} catch (Exception e) {
//			String msg = String.format("Unable to parse source uri %s for transfertask %s: %s",
//					source, uuid, e.getMessage());
//			body.put("message", msg);
//
//			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
//			//throw new RemoteDataSyntaxException(msg, e);

//		try {
			// pull the system out of the url. system id is the hostname in an agave uri
			RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
			// get a remtoe data client for the sytem
			RemoteDataClient srcClient = srcSystem.getRemoteDataClient();

			// pull the dest system out of the url. system id is the hostname in an agave uri
			RemoteSystem destSystem = new SystemDao().findBySystemId(destUri.getHost());
			RemoteDataClient destClient = destSystem.getRemoteDataClient();

			// stat the remote path to check its type
			RemoteFileInfo fileInfo = srcClient.getFileInfo(srcUri.getPath());


			URLCopy urcCopy = new URLCopy(srcUri,destUri);
		}catch (Exception e){
			logger.error(e.toString());
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
		}


		_doPublishEvent(MessageType.TRANSFER_COMPLETED, body);
	}

}









