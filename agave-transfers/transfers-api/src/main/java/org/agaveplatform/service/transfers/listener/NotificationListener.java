package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.iplantc.service.notification.managers.NotificationManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NotificationListener extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(NotificationListener.class);

	protected String eventChannel = MessageType.NOTIFICATION.getEventChannel() + ".*";

	public NotificationListener(Vertx vertx) {
		super(vertx);
	}

	public NotificationListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.NOTIFICATION.getEventChannel() + ".*";

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();

		// poc listener to show propagated notifications that woudl be sent to users
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();

			logger.info("{} notification event raised for {} {}: {}",
					body.getString("event"),
					body.getString("type"),
					body.getString("uuid"),
					body.encodePrettily());

			NotificationManager.process( body.getString("id"), body.encode(), body.getString("owner"));
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCEL_COMPLETED.getEventChannel(), msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} completed.", body.getString("id"));

			getVertx().eventBus().publish(MessageType.NOTIFICATION_CANCELLED.getEventChannel(), body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_COMPLETED.getEventChannel(), msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} completed.", body.getString("id"));

			getVertx().eventBus().publish(MessageType.NOTIFICATION_COMPLETED.getEventChannel(), body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CREATED.getEventChannel(), msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} created.", body.getString("id"));

		});
	}
}
