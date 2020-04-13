package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferHttpVertical extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferHttpVertical.class);

	public TransferHttpVertical(Vertx vertx) {
		super(vertx);
	}

	public TransferHttpVertical(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_HTTP.getEventChannel();

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

			logger.info("Transfer task HTTP {} for source {} and dest {}", uuid, source, dest);
			processEvent(body);

			});
	}

	public void processEvent(JsonObject body) {
		_doPublishEvent(MessageType.TRANSFER_COMPLETED.getEventChannel(), body);
	}

}
