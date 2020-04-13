package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorTaskListener extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(ErrorTaskListener.class);
	protected String eventChannel = MessageType.TRANSFERTASK_ERROR.getEventChannel();

	public ErrorTaskListener(Vertx vertx) {
		super(vertx);
	}

	public ErrorTaskListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ERROR.getEventChannel();

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		//final String err ;
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();

			logger.error("Transfer task {} failed: {}: {}",
					body.getString("id"), body.getString("cause"), body.getString("message"));

			_doPublishEvent(MessageType.NOTIFICATION_TRANSFERTASK.getEventChannel(), body);

		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PARENT_ERROR.getEventChannel(), msg -> {
			JsonObject body = msg.body();

			logger.error("Transfer task {} failed to check it's parent task {} for copmletion: {}: {}",
					body.getString("id"), body.getString("parentTaskId"), body.getString("cause"), body.getString("message"));

		});
	}
}
