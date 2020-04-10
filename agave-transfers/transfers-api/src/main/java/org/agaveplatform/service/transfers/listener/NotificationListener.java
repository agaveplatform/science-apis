package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.iplantc.service.notification.managers.NotificationManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NotificationListener extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(NotificationListener.class);

	protected String eventChannel = "notification.*";

	public NotificationListener(Vertx vertx) {
		super(vertx);
	}

	public NotificationListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = "notification.*";

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

		bus.<JsonObject>consumer("transfertask.cancel.completed", msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} completed.", body.getString("id"));

			getVertx().eventBus().publish("notification.cancelled", body);
		});

		bus.<JsonObject>consumer("transfertask.completed", msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} completed.", body.getString("id"));

			getVertx().eventBus().publish("notification.completed", body);
		});

		bus.<JsonObject>consumer("transfertask.created", msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} created.", body.getString("id"));

		});



	}
}
