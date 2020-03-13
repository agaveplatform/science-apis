package main.java.org.agaveplatform.service.transfers.streaming;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface StreamingTask {

	static org.iplantc.vertx.fileTransfer.FileTrasferService getSystem(Vertx vertx){
		return new StreamingFileServiceImpl(vertx);
	}
	static org.iplantc.vertx.fileTransfer.FileTrasferService createProxy(Vertx vertx, String address) {
		return new StramingFileServiceVertxEBProxy(vertx, address);
	}
	void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
	void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
}
