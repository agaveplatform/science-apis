package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.ext.web.RoutingContext;

public class FailureHandler extends AbstractVerticle implements Handler<RoutingContext> {

	protected String eventChannel = "transfertask.error";

	public FailureHandler(Vertx vertx) {
		this(vertx, null);
	}

	public FailureHandler(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);
	}

	public void handle(RoutingContext context){
		Throwable thrown = context.failure();
		recordError(thrown);
		context.response().setStatusCode(500).end();
	}

	private void recordError(Throwable throwable){
		System.out.println("error");
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
