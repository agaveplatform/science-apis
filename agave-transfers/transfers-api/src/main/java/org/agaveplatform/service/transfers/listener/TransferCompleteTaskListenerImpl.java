package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferCompleteTaskListenerImpl extends AbstractVerticle implements TransferCompleteTaskListener {
	private final Logger logger = LoggerFactory.getLogger(TransferCompleteTaskListenerImpl.class);
	private String address = "*.transfer.complete";

	public TransferCompleteTaskListenerImpl(Vertx vertx) {
		this(vertx,null);
	}

	public TransferCompleteTaskListenerImpl(Vertx vertx, String address) {
		super();
		setVertx(vertx);
		setAddress(address);
	}

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.<JsonObject>consumer(getAddress(), msg -> {
			JsonObject body = msg.body();
			String transferTaskId = body.getString("id");
			String transferTaskStatus = body.getString("status");
			logger.info("{} transfer task {} completed with status {}", transferTaskId, transferTaskStatus);
		});
	}

	/**
	 * Sets the vertx instance for this listener
	 * @param vertx the current instance of vertx
	 */
	private void setVertx(Vertx vertx) {
		this.vertx = vertx;
	}

	/**
	 * @return the message type to listen to
	 */
	public String getAddress() {
		return address;
	}

	/**
	 * Sets the message type for which to listen
	 * @param address
	 */
	public void setAddress(String address) {
		this.address = address;
	}
}
