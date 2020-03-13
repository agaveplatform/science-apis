package process;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.stream.Collectors;

public class ProcessFileTask extends AbstractVerticle {
	private final HashMap<String, Double> lastValues = new HashMap<>();
	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		bus.consumer("sftp", this::sftp);
		bus.consumer("s3", this::s3);
	}
	private void s3(Message<JsonObject> message) {
		JsonObject json = message.body();
		lastValues.put(json.getString("id"), json.getDouble("temp"));
	}
	private void sftp(Message<JsonObject> message) {
		double avg = lastValues.values().stream()
				.collect(Collectors.averagingDouble(Double::doubleValue));
		JsonObject json = new JsonObject().put("average", avg);
		message.reply(json);
	}
}
