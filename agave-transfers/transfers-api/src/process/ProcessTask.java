package process;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

@ProxyGen
public interface ProcessTask {

	static org.iplantc.vertx.fileTransfer.FileTrasferService getSystem(Vertx vertx){
		return new ProcessFileServiceImpl(vertx);
	}
	static org.iplantc.vertx.fileTransfer.FileTrasferService createProxy(Vertx vertx, String address) {
		return new ProcessFileServiceVertxEBProxy(vertx, address);
	}
	void createDir(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
	void createFile(JsonObject jsonObject, Handler<AsyncResult<JsonObject>> handler);
}
