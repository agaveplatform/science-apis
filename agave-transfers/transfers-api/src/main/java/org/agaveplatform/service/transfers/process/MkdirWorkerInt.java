package main.java.org.agaveplatform.service.transfers.process;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;

@ProxyGen
public interface MkdirWorkerInt {
	static MkdirWorkerVerticle getSystem(Vertx vertx){
		return new MkdirWorkerVerticle(vertx);
	}
	static MkdirWorkerVerticle createProxy(Vertx vertx, String address) {
		return new MkdirWorkerVerticle(vertx, address);
	}
}
