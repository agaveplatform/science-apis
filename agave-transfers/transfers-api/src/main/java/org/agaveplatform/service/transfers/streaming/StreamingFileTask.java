package org.agaveplatform.service.transfers.streaming;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

import java.util.HashMap;
import java.util.stream.Collectors;

public interface StreamingFileTask {
	static StreamingFileTask getSystem(Vertx vertx){
		return new StreamingFileTaskImpl(vertx);
	}
	static StreamingFileTask createProxy(Vertx vertx, String address) {
		return new StreamingFileTaskImpl(vertx, address);
	}
	//void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
	void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
}
