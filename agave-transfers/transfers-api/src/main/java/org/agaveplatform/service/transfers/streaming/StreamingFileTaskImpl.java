package org.agaveplatform.service.transfers.streaming;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.listener.AbstractTransferTaskListener;

import java.util.HashMap;
import java.util.stream.Collectors;

public class StreamingFileTaskImpl extends AbstractTransferTaskListener implements StreamingFileTask {
	private final HashMap<String, Double> lastValues = new HashMap<>();

	public StreamingFileTaskImpl(Vertx vertx) {
		super(vertx);
	}

	public StreamingFileTaskImpl(Vertx vertx, String eventChannel) {
		super(vertx, eventChannel);
	}

	protected static final String EVENT_CHANNEL = "transfer.streaming";

	public String getDefaultEventChannel() {
		return EVENT_CHANNEL;
	}

	public void start() {
		vertx.eventBus().<JsonObject>consumer(getEventChannel(), message -> {
			JsonObject json = message.body();
			lastValues.put(json.getString("id"), json.getDouble("temp"));
		});
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

}
