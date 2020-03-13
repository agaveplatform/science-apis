package main.java.org.agaveplatform.service.transfers.process;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;

@ProxyGen
public interface RenameWorkerInt {
	static RenameWorkerVerticle getSystem(Vertx vertx){
		return new RenameWorkerVerticle(vertx);
	}
	static RenameWorkerVerticle createProxy(Vertx vertx, String address) {
		return new RenameWorkerVerticle(vertx, address);
	}
}
