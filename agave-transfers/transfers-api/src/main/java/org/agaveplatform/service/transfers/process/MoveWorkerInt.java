package main.java.org.agaveplatform.service.transfers.process;


import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;

@ProxyGen
public interface MoveWorkerInt {
	static MoveWorkerVerticle getSystem(Vertx vertx){
		return new MoveWorkerVerticle(vertx);
	}
	static MoveWorkerVerticle createProxy(Vertx vertx, String address) {
		return new MoveWorkerVerticle(vertx, address);
	}
}