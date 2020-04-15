package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;

import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
	}

	public void processEvent(JsonObject body) {
		_doPublishEvent(MessageType.TRANSFER_COMPLETED, body);
	}

}
