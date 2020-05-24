package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Deprecated
public class InteruptEventListener extends AbstractTransferTaskListener {
	private final Logger logger = LoggerFactory.getLogger(InteruptEventListener.class);


	public InteruptEventListener(Vertx vertx) {
		super(vertx);
	}

	public InteruptEventListener(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_INTERUPTED;

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();
			String uuid = body.getString("uuid");
			String tenantId = body.getString("tenantId");
			String source = body.getString("source");
			logger.info("Transfer task paused {} created: {} -> {}",tenantId, uuid, source);
		});
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

}
