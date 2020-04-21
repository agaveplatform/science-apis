package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;

import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.URLCopy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class TransferAllVertical extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferAllVertical.class);

	public TransferAllVertical(Vertx vertx) {
		super(vertx);
	}

	public TransferAllVertical(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_ALL;

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

			logger.info("Default listener claimed task {} for source {} and dest {}", uuid, source, dest);
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

	public String processEvent(JsonObject body) {
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

			URLCopy urlCopy = new URLCopy(srcClient,destClient);

			org.iplantc.service.transfer.model.TransferTask transferTaskIplant = new org.iplantc.service.transfer.model.TransferTask(source, dest);

			urlCopy.copy(source,dest, transferTaskIplant);

		}catch (Exception e){
			logger.error(e.toString());
			_doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
			return e.toString();
		}

		_doPublishEvent(MessageType.TRANSFER_COMPLETED, body);
		return "Complete";
	}

}
