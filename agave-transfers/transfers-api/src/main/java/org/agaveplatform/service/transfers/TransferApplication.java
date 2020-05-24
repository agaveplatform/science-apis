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
                vertx.deployVerticle("org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle",
                        new DeploymentOptions().setConfig(config), dbVerticleDeployment);

                dbVerticleDeployment.future().compose(id -> {

                    Promise<String> httpVerticleDeployment = Promise.promise();
                    vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferAPIVertical",
                            new DeploymentOptions().setConfig(config),
                            res -> {
                                if (res.succeeded()) {
                                    log.info("TransferAPIVertical ({}) started on port {}", res.result(), config.getInteger("HTTP_PORT"));

                                    DeploymentOptions localOptions = new DeploymentOptions()
                                            .setWorkerPoolName("streaming-task-worker-pool")
                                            .setWorkerPoolSize(poolSize)
                                            .setInstances(instanceSize)
                                            .setWorker(true);

                                    //Deploy the TransferTaskAssignedListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferTaskAssignedListener",
                                    localOptions, res0 -> {
                                        if (res.succeeded()){
                                            System.out.println("TransferTaskAssignedListener Deployment id is " + res.result());
                                        }else{
                                            System.out.println("TransferTaskAssignedListener Deployment failed !");
                                        }
                                    });

                                    // Deployment TransferTaskCreatedListener verticle
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferTaskCreatedListener",
                                    localOptions,  res1 -> {
                                        if (res1.succeeded()){
                                            System.out.println("TransferTaskCreatedListener Deployment id is " + res.result());
                                        }else{
                                            System.out.println("TransferTaskCreatedListener Deployment failed !");
                                        }
                                    });

                                    // Deploy the TransferRetryListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferRetryListener",
                                            localOptions, res3 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferRetryListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferRetryListener Deployment failed !");
                                                }
                                            });

                                    // Deploy the TransferAllProtocolVertical vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical",
                                            localOptions, res4 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferAllProtocolVertical Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferAllProtocolVertical Deployment failed !");
                                                }
                                            });

                                    // Deploy the TransferCompleteTaskListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferCompleteTaskListener",
                                            localOptions, res5 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferCompleteTaskListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferCompleteTaskListener Deployment failed !");
                                                }
                                            });


                                    // Deploy the TransferErrorTaskListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferErrorTaskListener",
                                           localOptions, res6 -> {
                                                if (res.succeeded()){
                                                    System.out.println("ErrorTaskListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("ErrorTaskListener Deployment failed !");
                                                }
                                            });

                                    // Deploy the TransferFailureHandler vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferFailureHandler",
                                            localOptions, res7 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferFailureHandler Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferFailureHandler Deployment failed !");
                                                }
                                            });

                                    // Deploy the TransferTaskCancelListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferTaskCancelListener",
                                           localOptions, res8 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferTaskCancelListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferTaskCancelListener Deployment failed !");
                                                }
                                            });

                                    // Deploy the TransferTaskPausedListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferTaskPausedListener",
                                    localOptions,  res9 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferTaskPausedListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferTaskPausedListener Deployment failed !");
                                                }
                                            });

                                    // Deploy the NotificationListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.NotificationListener",
                                            localOptions, res10 -> {
                                                if (res.succeeded()){
                                                    System.out.println("NotificationListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("NotificationListener Deployment failed !");
                                                }
                                            });

                                    // Deploy the TransferErrorListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferErrorListener",
                                            localOptions, res11 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferErrorListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferErrorListener Deployment failed !");
                                                }
                                            });

                                    // Deploy the TransferHealthcheckListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferHealthcheckListener",
                                            localOptions, res12 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferHealthcheckListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferHealthcheckListener Deployment failed !");
                                                }
                                            });

                                    // Deploy the TransferWatchListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferWatchListener",
                                            localOptions, res13 -> {
                                                if (res.succeeded()){
                                                    System.out.println("TransferWatchListener Deployment id is " + res.result());
                                                }else{
                                                    System.out.println("TransferWatchListener Deployment failed !");
                                                }
                                            });

                                } else {
                                    System.out.println("TransferAPIVertical deployment failed !\n" + res.cause());
                                    res.cause().printStackTrace();
                                }
                            });

                    return httpVerticleDeployment.future();

                }).setHandler(ar -> {
                    if (ar.succeeded()) {
                        log.info("TransferApiVertical ({}) started on port {}", ar.result(), config.getInteger("HTTP_PORT"));
                    } else {
                        System.out.println("TransferApiVertical deployment failed !\n" + ar.cause());
                        ar.cause().printStackTrace();
                    }
                });
            } else {
                log.error("Error retrieving configuration.");
            }
        });






    }
}
