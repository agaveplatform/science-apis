package org.avageplatform.service.transfers.unary;

import io.vertx.core.AbstractVerticle;
		import io.vertx.core.AsyncResult;
		import io.vertx.core.Handler;
		import io.vertx.core.Vertx;
		import io.vertx.core.eventbus.EventBus;
		import io.vertx.core.eventbus.Message;
		import io.vertx.core.json.JsonObject;

		import java.util.HashMap;
		import java.util.stream.Collectors;

public interface UnaryFileTask {
	static UnaryFileTask getSystem(Vertx vertx){
		return new UnaryFileTaskImpl(vertx);
	}
	static UnaryFileTask createProxy(Vertx vertx, String address) {
		return new UnaryFileTaskImpl(vertx, address);
	}
	void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
	void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
}
