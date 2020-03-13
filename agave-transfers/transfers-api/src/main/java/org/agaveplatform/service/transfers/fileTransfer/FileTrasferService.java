package main.java.org.agaveplatform.service.transfers.fileTransfer;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface FileTrasferService {

	static FileTrasferService getSystem(Vertx vertx){
		return new FileTransferServiceImpl(vertx);
	}
	static FileTrasferService createProxy(Vertx vertx, String address) {
		return new FileTransferServiceVertxEBProxy(vertx, address);
	}
	void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
	void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
}
