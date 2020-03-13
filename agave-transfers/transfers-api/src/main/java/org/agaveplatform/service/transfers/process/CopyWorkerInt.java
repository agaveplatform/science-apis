package main.java.org.agaveplatform.service.transfers.process;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;

@ProxyGen
public interface CopyWorkerInt {
	static CopyWorkerVerticle getSystem(Vertx vertx){
		return new CopyWorkerVerticle(vertx);
	}
	static CopyWorkerVerticle createProxy(Vertx vertx, String address) {
		return new CopyWorkerVerticle(vertx, address);
	}
}
