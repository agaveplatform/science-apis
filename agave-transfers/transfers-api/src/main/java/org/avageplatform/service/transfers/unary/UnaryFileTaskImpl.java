package org.avageplatform.service.transfers.unary;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;

import java.util.HashMap;
import java.util.stream.Collectors;


public class UnaryFileTaskImpl extends AbstractVerticle implements UnaryFileTask {
	private final HashMap<String, Double> lastValues = new HashMap<>();
	private String eventChannel = MessageType.TRANSFER_UNARY;

	public UnaryFileTaskImpl (Vertx vertx) {
		this(vertx, null);
	}

	public UnaryFileTaskImpl(Vertx vertx, String eventChannel) {
		super();
		setVertx(vertx);
		setEventChannel(eventChannel);

	}

	public void start() {
		vertx.eventBus().<JsonObject>consumer(MessageType.FILETRANSFER_SFTP, message -> {
			JsonObject json = message.body();
			lastValues.put(json.getString("id"), json.getDouble("temp"));
		});
	}

	@Override
	public void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler) {
		double avg = lastValues.values().stream()
				.collect(Collectors.averagingDouble(Double::doubleValue));
		jsonObject = new JsonObject().put("average", avg);
		handler.handle(Future.succeededFuture(jsonObject));
	}

	@Override
	public void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler) {
		double avg = lastValues.values().stream()
				.collect(Collectors.averagingDouble(Double::doubleValue));
		// todo

		//=========================================================
		JsonObject data = new JsonObject().put("average", avg);
		handler.handle(Future.succeededFuture(data));
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
	public String getEventChannel() {
		return eventChannel;
	}

	/**
	 * Sets the message type for which to listen
	 * @param eventChannel
	 */
	public void setEventChannel(String eventChannel) {
		this.eventChannel = eventChannel;
	}

}
