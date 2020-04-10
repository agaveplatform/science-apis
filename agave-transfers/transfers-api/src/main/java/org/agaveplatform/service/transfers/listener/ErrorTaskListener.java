package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorTaskListener extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(ErrorTaskListener.class);
	protected String eventChannel = "transfertask.error";

	public ErrorTaskListener(Vertx vertx) {
		this(vertx, null);
	}

	public ErrorTaskListener(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
	}
	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		//final String err ;
		bus.<JsonObject>consumer("transfertask.error", msg -> {
			JsonObject body = msg.body();

			logger.error("Transfer task {} failed: {}: {}",
					body.getString("id"), body.getString("cause"), body.getString("message"));

			bus.publish("notification.transfertask", body);

		});

		bus.<JsonObject>consumer("transfertask.parent.error", msg -> {
			JsonObject body = msg.body();

			logger.error("Transfer task {} failed to check it's parent task {} for copmletion: {}: {}",
					body.getString("id"), body.getString("parentTaskId"), body.getString("cause"), body.getString("message"));

//			bus.publish("notification.transfertask", body);

		});
	}

	/**
	 * Sets the vertx instance for this listener
	 *
	 * @param vertx the current instance of vertx
	 */
	private void setVertx(Vertx vertx) {
		this.vertx = vertx;
	}

	/**
	 * @return the message type to listen to
	 */
	public String getEventChannel() {
		return eventChannel;
	}

	/**
	 * Sets the message type for which to listen
	 *
	 * @param eventChannel
	 */
	public void setEventChannel(String eventChannel) {
		this.eventChannel = eventChannel;
	}
}
