package main.java.org.agaveplatform.service.transfers.fileTransfer;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;


public class FileTransferVerticle extends AbstractVerticle {

	public static void main(String[] args) {
		VertxOptions vertxOptions = new VertxOptions(); //.setMetricsOptions(metricsOptions);
		Vertx vertx = Vertx.vertx(vertxOptions);

		DeploymentOptions deploymentOptions = new DeploymentOptions()
				.setWorker(true)
				.setInstances(5);
				//.setWorkerPoolName("the-specific-pool")
				//.setWorkerPoolSize(5);

		vertx.deployVerticle("org.ipantc.io.vertx.TransferTaskAssigner", res -> {
			if (res.succeeded()){
				System.out.println("TransferAssigner Deployment id is " + res.result());
			}else{
				System.out.println("TransferAssigner Deployment failed !");
			}
		});

		vertx.deployVerticle("org.ipantc.io.vertx.ProcessFileTask", res -> {
			if (res.succeeded()){
				System.out.println("Deployment id is " + res.result());
			}else{
				System.out.println("Deployment failed !");
			}
		});

		vertx.deployVerticle("org.ipantc.io.vertx.StreamingFileTask", res -> {
			if (res.succeeded()){
				System.out.println("StreamingTask Deployment id is " + res.result());
			}else{
				System.out.println("StreamingTask Deployment failed !");
			}
		});

		vertx.deployVerticle("org.ipantc.io.vertx.TransferCompleteTaskListener", res -> {
			if (res.succeeded()){
				System.out.println("TransferComplete Deployment id is " + res.result());
			}else{
				System.out.println("TransferComplete Deployment failed !");
			}
		});

		vertx.deployVerticle("org.ipantc.io.vertx.ErrorTaskListener", res -> {
			if (res.succeeded()){
				System.out.println("ErrorTask Deployment id is " + res.result());
			}else{
				System.out.println("ErrorTask Deployment failed !");
			}
		});

		vertx.deployVerticle("org.ipantc.io.vertx.FileTransferServiceImpl", res -> {
			if (res.succeeded()){
				System.out.println("FileTransferServiceImpl Deployment id is " + res.result());
			}else{
				System.out.println("FileTransferServiceImpl Deployment failed !");
			}
		});
	}

}