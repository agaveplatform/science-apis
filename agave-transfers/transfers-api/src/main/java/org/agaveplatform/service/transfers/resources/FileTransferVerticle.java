package org.agaveplatform.service.transfers.resources;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;


public class FileTransferVerticle extends AbstractVerticle {

	public static void main(String[] args) {
		VertxOptions vertxOptions = new VertxOptions(); //.setMetricsOptions(metricsOptions);
		Vertx vertx = Vertx.vertx(vertxOptions);
		int poolSize = 1;
		int instanceSize = 1;

		DeploymentOptions deploymentOptions = new DeploymentOptions()
				.setWorker(true)
				.setInstances(instanceSize);
				//.setWorkerPoolName("the-specific-pool")
				//.setWorkerPoolSize(poolSize);

		vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TaskAssignerVerticle",
				new DeploymentOptions()
					.setWorkerPoolName("transfer-task-assigner-pool")
					.setWorkerPoolSize(poolSize)
					.setInstances(instanceSize) 
					.setWorker(true),
				res -> {
					if (res.succeeded()){
						System.out.println("TransferAssigner Deployment id is " + res.result());
					}else{
						System.out.println("TransferAssigner Deployment failed !");
					}
				});

		vertx.deployVerticle("org.agaveplatform.service.transfers.resources.ProcessFileTask",
				new DeploymentOptions()
						.setWorkerPoolName("transfer-task-assigner-pool")
						.setWorkerPoolSize(poolSize)
						.setInstances(instanceSize) 
						.setWorker(true),
				res -> {
					if (res.succeeded()){
						System.out.println("Deployment id is " + res.result());
					}else{
						System.out.println("Deployment failed !");
					}
				});

		vertx.deployVerticle("org.agaveplatform.service.transfers.resources.StreamingFileTask",
				new DeploymentOptions()
						.setWorkerPoolName("streaming-task-worker-pool")
						.setWorkerPoolSize(poolSize)
						.setInstances(instanceSize) 
						.setWorker(true),
				res -> {
					if (res.succeeded()){
						System.out.println("StreamingTask Deployment id is " + res.result());
					}else{
						System.out.println("StreamingTask Deployment failed !");
					}
				});

		vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferCompleteTaskListener",
				new DeploymentOptions()
						.setWorkerPoolName("transfer-task-complete-pool")
						.setWorkerPoolSize(poolSize)
						.setInstances(instanceSize) 
						.setWorker(true),
				res -> {
					if (res.succeeded()){
						System.out.println("TransferComplete Deployment id is " + res.result());
					}else{
						System.out.println("TransferComplete Deployment failed !");
					}
				});

		vertx.deployVerticle("org.agaveplatform.service.transfers.resources.ErrorTaskListener",
				new DeploymentOptions()
						.setWorkerPoolName("transfer-task-error-pool")
						.setWorkerPoolSize(poolSize)
						.setInstances(instanceSize) 
						.setWorker(true),
				res -> {
					if (res.succeeded()){
						System.out.println("ErrorTask Deployment id is " + res.result());
					}else{
						System.out.println("ErrorTask Deployment failed !");
					}
				});

		vertx.deployVerticle("org.agaveplatform.service.transfers.resources.FileTransferServiceImpl",
				new DeploymentOptions()
						.setWorkerPoolName("sftp-transfer-task-worker-pool")
						.setWorkerPoolSize(poolSize)
						.setInstances(instanceSize) 
						.setWorker(true),
				res -> {
					if (res.succeeded()){
						System.out.println("FileTransferServiceImpl Deployment id is " + res.result());
					}else{
						System.out.println("FileTransferServiceImpl Deployment failed !");
					}
				});
	}

}