package org.avageplatform.service.transfers.unary;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.streaming.StreamingFileTask;

import java.util.HashMap;
import java.util.stream.Collectors;


public class UnaryFileTaskImpl extends AbstractVerticle implements UnaryFileTask {
	private final HashMap<String, Double> lastValues = new HashMap<>();
	private String address = "*.transfer.streaming";

	public UnaryFileTaskImpl (Vertx vertx) {
		this(vertx, null);
	}

	public Unary
	FileTaskImpl(Vertx vertx, String address) {
		super();
		setVertx(vertx);
		setAddress(address);

	}

	public void start() {
		vertx.eventBus().<JsonObject>consumer("filetransfer.sftp", message -> {
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
