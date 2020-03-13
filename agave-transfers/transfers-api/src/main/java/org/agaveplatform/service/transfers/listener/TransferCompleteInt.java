package main.java.org.agaveplatform.service.transfers.listener;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;

@ProxyGen
public interface TransferCompleteInt {
	static TransferCompleteTaskListener getSystem(Vertx vertx){
		return new TransferCompleteTaskListener(vertx);
	}
	static TransferCompleteTaskListener createProxy(Vertx vertx, String address) {
		return new TransferCompleteTaskListener(vertx, address);
	}
}
