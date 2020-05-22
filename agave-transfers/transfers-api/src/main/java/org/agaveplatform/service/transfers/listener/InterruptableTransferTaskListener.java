package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;


public interface InterruptableTransferTaskListener{//} extends TransferTaskListener {

	void processInterrupt(String state, JsonObject body);
}
