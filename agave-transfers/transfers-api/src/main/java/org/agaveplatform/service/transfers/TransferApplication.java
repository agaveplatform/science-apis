package org.agaveplatform.service.transfers;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.config.ConfigStoreOptions;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TransferApplication {

    public static void main(String[] args) {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");
        Logger log = LoggerFactory.getLogger(TransferApplication.class);
        Vertx vertx = Vertx.vertx();

        int poolSize = 1;
        int instanceSize = 1;

        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", "config.json"));

        ConfigStoreOptions envPropsStore = new ConfigStoreOptions().setType("env");

        ConfigRetrieverOptions options = new ConfigRetrieverOptions()
                .addStore(fileStore)
                .addStore(envPropsStore);

        ConfigRetriever retriever = ConfigRetriever.create(vertx, options);

        retriever.getConfig(json -> {
            if (json.succeeded()) {
                JsonObject config = json.result();
                log.debug("Starting the app with config: " + config.encodePrettily());

                Promise<String> dbVerticleDeployment = Promise.promise();
                vertx.deployVerticle(new TransferTaskDatabaseVerticle(), dbVerticleDeployment);

                dbVerticleDeployment.future().compose(id -> {

                    Promise<String> httpVerticleDeployment = Promise.promise();
                    vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferServiceVertical",
                            new DeploymentOptions().setConfig(config),
                            res -> {
                                if (res.succeeded()) {
                                    log.info("TransferServiceVertical ({}) started on port {}", res.result(), config.getInteger("HTTP_PORT"));
                                } else {
                                    System.out.println("TransferServiceVertical deployment failed !\n" + res.cause());
                                    res.cause().printStackTrace();
                                }
                            });

                    return httpVerticleDeployment.future();

                }).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("TransferServiceVertical ({}) started on port {}", ar.result(), config.getInteger("HTTP_PORT"));
                    } else {
                        System.out.println("TransferServiceVertical deployment failed !\n" + ar.cause());
                        ar.cause().printStackTrace();
                    }
                });



            } else {
                log.error("Error retrieving configuration.");
            }
        });

//        VertxOptions vertxOptions = new VertxOptions(); //.setMetricsOptions(metricsOptions);

//        DeploymentOptions deploymentOptions = new DeploymentOptions()
//                .setWorker(true)
//                .setInstances(instanceSize);
        //.setWorkerPoolName("the-specific-pool")
        //.setWorkerPoolSize(poolSize);

        vertx.deployVerticle("org.agaveplatform.service.transfers.resources.StreamingFileTaskImpl",
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

        vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferCompleteTaskListenerImpl",
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
