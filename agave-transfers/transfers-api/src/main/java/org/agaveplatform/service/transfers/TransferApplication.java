package org.agaveplatform.service.transfers;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.config.ConfigStoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferApplication {

    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(TransferApplication.class);
        Vertx vertx = Vertx.vertx();

        int poolSize = 1;
        int instanceSize = 1;

        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "resources/config.json"));

        ConfigStoreOptions sysPropsStore = new ConfigStoreOptions().setType("sys");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
                .addStore(fileStore)
                .addStore(sysPropsStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(json -> {
            if (json.succeeded()) {
                JsonObject config = json.result();
                log.debug("Starting the app with config: " + config.encodePrettily());

                vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferServiceVertical",
                        new DeploymentOptions(),
                        res -> {
                            if (res.succeeded()) {
                                System.out.println("TransferServiceVertical Deployment id is " + res.result());
                            } else {
                                System.out.println("TransferServiceVertical Deployment failed !");
                            }
                        });

                vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TaskAssignerVerticle",
                        new DeploymentOptions()
                                .setWorkerPoolName("transfer-task-assigner-pool")
                                .setWorkerPoolSize(poolSize)
                                .setInstances(instanceSize)
                                .setWorker(true),
                        res -> {
                            if (res.succeeded()) {
                                System.out.println("TransferAssigner Deployment id is " + res.result());
                            } else {
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
                            if (res.succeeded()) {
                                System.out.println("Deployment id is " + res.result());
                            } else {
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
                            if (res.succeeded()) {
                                System.out.println("StreamingTask Deployment id is " + res.result());
                            } else {
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
                            if (res.succeeded()) {
                                System.out.println("TransferComplete Deployment id is " + res.result());
                            } else {
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
                            if (res.succeeded()) {
                                System.out.println("ErrorTask Deployment id is " + res.result());
                            } else {
                                System.out.println("ErrorTask Deployment failed !");
                            }
                        });

                vertx.deployVerticle("org.agaveplatform.service.transfers.resources.FileTransferServiceImpl",
                        new DeploymentOptions()
                                .setConfig(config)
                                .setWorkerPoolName("sftp-transfer-task-worker-pool")
                                .setWorker(true),
                        res -> {
                            if (res.succeeded()) {
                                System.out.println("FileTransferServiceImpl Deployment id is " + res.result());
                            } else {
                                System.out.println("FileTransferServiceImpl Deployment failed !");
                            }
                        });
            } else {
                log.error("Error retrieving configuration.");
            }
        });
    }
}
