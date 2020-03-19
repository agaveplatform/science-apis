package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NotificationListener extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(NotificationListener.class);

	@Override
	public void start() {
		EventBus bus = vertx.eventBus();
		//final String err ;
		bus.<JsonObject>consumer("transfertask.notification.*", msg -> {
			JsonObject body = msg.body();

			logger.info("Transfer task {} notification: {}",
					body.getString("id"), body.getString("message"));
			TransferTask bodyTask = new TransferTask(body);
			notification(bodyTask);
		});
	}

	public void notification(TransferTask transferTask){
		//TODO:  Write the nofication call here
	}
}
