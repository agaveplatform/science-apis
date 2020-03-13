package main.java.org.agaveplatform.service.transfers.fileTransfer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskAssignerVerticle extends AbstractVerticle {
	private final Logger logger = LoggerFactory.getLogger(TaskAssignerVerticle.class);
	private long counter = 1;

	@Override
	public void start() {
		vertx.setPeriodic(5000, id -> {
			logger.info("tick");
		});
	}

	public static void main(String[] args) {
		VertxOptions vertxOptions = new VertxOptions(); //.setMetricsOptions(metricsOptions);
		Vertx vertx = Vertx.vertx(vertxOptions);

		DeploymentOptions deploymentOptions = new DeploymentOptions()
				.setWorker(true)
				.setInstances(25);
		vertx.deployVerticle("org.ipantc.io.vertx.verticle.FileTransferVerticle", res -> {
			if (res.succeeded()){
				System.out.println("Deployment id is " + res.result());
			}else{
				System.out.println("Deployment failed !");
			}
		});
		vertx.deployVerticle("org.ipantc.io.vertx.verticle.FileTransferVerticleImpl", res -> {
			if (res.succeeded()){
				System.out.println("Deployment id is " + res.result());
			}else{
				System.out.println("Deployment failed !");
			}
		});
		vertx.deployVerticle("org.ipantc.io.vertx.verticle.TaskAssignerVerticle", res -> {
			if (res.succeeded()){
				System.out.println("Deployment id is " + res.result());
			}else{
				System.out.println("Deployment failed !");
			}
		});
	}
}
