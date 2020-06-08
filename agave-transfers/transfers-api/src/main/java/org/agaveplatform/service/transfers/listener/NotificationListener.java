package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.iplantc.service.notification.managers.NotificationManager;

import org.iplantc.service.notification.queue.messaging.NotificationMessageBody;
import org.iplantc.service.notification.queue.messaging.NotificationMessageContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class NotificationListener extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(NotificationListener.class);

	protected String eventChannel = MessageType.NOTIFICATION ;

	public NotificationListener(Vertx vertx) {
		super(vertx);
	}

	public NotificationListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.NOTIFICATION ;

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();

		// poc listener to show propagated notifications that woudl be sent to users
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();

			NotificationMessageContext messageBodyContext = new NotificationMessageContext(
					MessageType.TRANSFERTASK_CANCELED_COMPLETED, body.encode(), body.getString("uuid"));

			NotificationMessageBody messageBody = new NotificationMessageBody(
					body.getString("uuid"), body.getString("owner"), body.getString("tenantId"),
					messageBodyContext);

			logger.info("{} notification event raised for {} {}: {}",
					body.getString("event"),
					body.getString("type"),
					body.getString("uuid"),
					body.encodePrettily());

			// we publish all notifications to the same channel for consumers to subscribe to. Let them
			// gtet the event type from the body of the message rather than the channel to reduce the
			// complexity of their client apps and the documentation of our notification semantics.
			_doPublishEvent(MessageType.TRANSFERTASK_NOTIFICATION, messageBody);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
			JsonObject body = msg.body();
			//JsonObject notificationMessage = new NotificationMessageBody();

			logger.info("Transfer task {} completed.", body.getString("id"));

			_doPublishEvent(MessageType.NOTIFICATION_CANCELLED, body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_COMPLETED, msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} completed.", body.getString("id"));

			getVertx().eventBus().publish(MessageType.NOTIFICATION_COMPLETED, body);
		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CREATED, msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} created.", body.getString("id"));

		});


		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} created.", body.getString("id"));

		});
		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_ERROR, msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} created.", body.getString("id"));

		});

		bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PARENT_ERROR, msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} created.", body.getString("id"));

		});
	}

	protected boolean notificationEventProcess(JsonObject body) {
		logger.info(body.getString("id"), body.encode(), body.getString("owner"));
		NotificationManager.process(body.getString("id"), body.encode(), body.getString("owner"));
		return true;
	}

}
