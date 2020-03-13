package main.java.org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ErrorTaskListener extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(ErrorTaskListener.class);

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		//final String err ;
		bus.<JsonObject>consumer("error", msg -> {
			JsonObject body = msg.body();
			String systemId = body.getString("systemId");
			String err = body.getString("error");
			logger.info("Error %v,  %s", systemId, err);

			if (err == "retry"){
				bus.publish(msg);
			}
		});

	}
}
