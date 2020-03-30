package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InteruptEventListener extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(InteruptEventListener.class);
	private String eventChannel = "transfertask.interupt.*.*.*.*";

	public InteruptEventListener(Vertx vertx) {
		this(vertx, null);
	}

	public InteruptEventListener(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();

	}





	public boolean interruptEvent( String uuid, String source, String username, String tenantId ){
		EventBus bus = vertx.eventBus();
		bus.consumer(getEventChannel());
		if ( bus.consumer("paused." + tenantId +"." + username + "." + uuid ).isRegistered()) {
			logger.info("Transfer task paused {} created: {} -> {}", tenantId, uuid, source);
			return true;
		}
		return false;
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
