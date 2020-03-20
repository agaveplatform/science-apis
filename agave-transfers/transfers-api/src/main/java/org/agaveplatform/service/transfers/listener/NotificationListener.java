package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.notification.dao.NotificationDao;
import org.iplantc.service.notification.exceptions.NotificationException;
import org.iplantc.service.notification.managers.NotificationManager;
import org.iplantc.service.notification.model.Notification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.persistence.Transient;

public class NotificationListener extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(NotificationListener.class);

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();

		// poc listener to show propagated notifications that woudl be sent to users
		bus.<JsonObject>consumer("notification.*", msg -> {
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
