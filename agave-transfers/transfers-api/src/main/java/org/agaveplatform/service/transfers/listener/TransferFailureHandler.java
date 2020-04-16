package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferFailureHandler extends AbstractTransferTaskListener implements Handler<RoutingContext> {
	private final Logger logger = LoggerFactory.getLogger(TransferFailureHandler.class);

	public TransferFailureHandler(Vertx vertx) {
		super(vertx);
	}

	public TransferFailureHandler(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFER_FAILED;

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	public void handle(RoutingContext context){
		Throwable thrown = context.failure();
		recordError(thrown);
		context.response().setStatusCode(500).end();
	}

	private void recordError(Throwable throwable){
		logger.info("failed: {}", throwable.getMessage());
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		//final String err ;
		bus.<JsonObject>consumer(getEventChannel(), msg -> {
			JsonObject body = msg.body();

			logger.error("Transfer task {} failed: {}: {}",
					body.getString("id"), body.getString("cause"), body.getString("message"));

			_doPublishEvent(MessageType.TRANSFERTASK_FAILED, body);
			//_doPublishEvent(MessageType.TRANSFER_FAILED, body);

		});

	}
}
