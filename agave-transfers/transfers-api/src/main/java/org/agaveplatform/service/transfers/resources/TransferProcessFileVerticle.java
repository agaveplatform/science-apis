package org.agaveplatform.service.transfers.resources;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

//@ProxyGen
public interface TransferProcessFileVerticle {

	static FileTransferService getSystem(Vertx vertx){
		return new FileTransferCreateServiceImpl(vertx);
	}
	//	static FileTransferService createProxy(Vertx vertx, String address) {
//		return new FileTransferServiceVertxEBProxy(vertx, address);
//	}
	void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
	void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
}
