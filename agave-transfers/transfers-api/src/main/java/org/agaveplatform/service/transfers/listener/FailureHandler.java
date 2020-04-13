package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailureHandler extends AbstractTransferTaskListener implements Handler<RoutingContext> {
	private final Logger logger = LoggerFactory.getLogger(FailureHandler.class);

	public FailureHandler(Vertx vertx) {
		super(vertx);
	}

	public FailureHandler(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_FAILED;

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

}
