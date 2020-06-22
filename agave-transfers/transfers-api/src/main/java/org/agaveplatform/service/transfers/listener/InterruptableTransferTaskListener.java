package org.agaveplatform.service.transfers.listener;

import io.vertx.core.json.JsonObject;


public interface InterruptableTransferTaskListener {

	void processInterrupt(String state, JsonObject body);
}
