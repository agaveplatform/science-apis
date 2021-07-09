package org.agaveplatform.service.transfers;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.listener.*;
import org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_JDBC_MAX_POOL_SIZE;

public class TransferApplication {

    private static final Logger log = LoggerFactory.getLogger(TransferApplication.class);

    public static void main(String[] args) {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");

        Vertx vertx = Vertx.vertx();

        int poolSize = 10;
        int instanceSize = 5;
        int dbInstanceSize = 5;

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

                DeploymentOptions dbDeploymentOptions = new DeploymentOptions()
                        .setConfig(config)
                        .setMaxWorkerExecuteTimeUnit(TimeUnit.MILLISECONDS)
                        .setMaxWorkerExecuteTime(500)
                        .setInstances(Math.min(dbInstanceSize, config.getInteger(CONFIG_TRANSFERTASK_DB_JDBC_MAX_POOL_SIZE, 2*dbInstanceSize)));

                Promise<String> dbVerticleDeployment = Promise.promise();
                log.info("org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle");
                vertx.deployVerticle("org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle", dbDeploymentOptions, dbVerticleDeployment);

                dbVerticleDeployment.future().onFailure(resp -> {
                    log.error("Unable to start db verticle.", resp.getCause());
                    System.exit(1);
                }).compose(id -> {
                    Promise<String> httpVerticleDeployment = Promise.promise();
                    vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferAPIVertical", new DeploymentOptions().setConfig(config), deployApiVerticle -> {
                        if (deployApiVerticle.succeeded()) {
                            log.info("TransferAPIVertical ({}) started on port {}", deployApiVerticle.result(), config.getInteger("HTTP_PORT"));

                            DeploymentOptions localOptions = new DeploymentOptions()
//                                    .setWorkerPoolName("streaming-task-worker-pool")
//                                    .setWorkerPoolSize(poolSize)
//                                    .setInstances(instanceSize)
                                    .setConfig(config)
                                    .setWorker(false)
                                    .setMaxWorkerExecuteTimeUnit(TimeUnit.SECONDS)
                                    .setMaxWorkerExecuteTime(30);

                            // Deployment TransferTaskCreatedListener verticle
                            vertx.deployVerticle(TransferTaskCreatedListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskCreatedListener",
                                    localOptions, res1 -> {
                                        if (res1.succeeded()) {
                                            log.info("TransferTaskCreatedListener Deployment id is " + res1.result());
                                        } else {
                                            log.error("TransferTaskCreatedListener Deployment failed !");
                                        }
                                    });


                            //Deploy the TransferTaskAssignedListener vertical
                            vertx.deployVerticle(TransferTaskAssignedListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskAssignedListener",
                                    localOptions, res0 -> {
                                        if (res0.succeeded()) {
                                            log.info("TransferTaskAssignedListener Deployment id is " + res0.result());
                                        } else {
                                            log.error("TransferTaskAssignedListener Deployment failed !");
                                        }
                                    });

                            // Deploy the TransferRetryListener vertical
                            vertx.deployVerticle(TransferTaskRetryListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferRetryListener",
                                    localOptions, res3 -> {
                                        if (res3.succeeded()) {
                                            log.info("TransferRetryListener Deployment id is " + res3.result());
                                        } else {
                                            log.error("TransferRetryListener Deployment failed !");
                                        }
                                    });

                            DeploymentOptions workerOptions = new DeploymentOptions()
                                    .setWorkerPoolName("all-task-worker-pool")
                                    .setWorkerPoolSize(poolSize)
                                    .setInstances(instanceSize)
                                    .setConfig(config)
                                    .setWorker(true)
                                    .setMaxWorkerExecuteTimeUnit(TimeUnit.HOURS)
                                    .setMaxWorkerExecuteTime(12);

                            // Deploy the TransferAllProtocolVertical vertical
                            vertx.deployVerticle(TransferAllProtocolVertical.class.getName(), //"org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical",
                                    workerOptions, res4 -> {
                                        if (res4.succeeded()) {
                                            log.info("TransferAllProtocolVertical Deployment id is " + res4.result());
                                        } else {
                                            log.error("TransferAllProtocolVertical Deployment failed !");
                                        }
                                    });

                            // Deploy the TransferCompleteTaskListener vertical
                            vertx.deployVerticle(TransferTaskCompleteTaskListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferCompleteTaskListener",
                                    localOptions, res5 -> {
                                        if (res5.succeeded()) {
                                            log.info("TransferCompleteTaskListener Deployment id is " + res5.result());
                                        } else {
                                            log.error("TransferCompleteTaskListener Deployment failed !");
                                        }
                                    });


                            // Deploy the TransferErrorTaskListener vertical
                            vertx.deployVerticle(TransferTaskErrorListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferErrorTaskListener",
                                    localOptions, res6 -> {
                                        if (res6.succeeded()) {
                                            log.info("ErrorTaskListener Deployment id is " + res6.result());
                                        } else {
                                            log.info("ErrorTaskListener Deployment failed !");
                                        }
                                    });

                            // Deploy the TransferFailureHandler vertical
                            vertx.deployVerticle(TransferTaskErrorFailureHandler.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferFailureHandler",
                                    localOptions, res7 -> {
                                        if (res7.succeeded()) {
                                            log.info("TransferFailureHandler Deployment id is " + res7.result());
                                        } else {
                                            log.error("TransferFailureHandler Deployment failed !");
                                        }
                                    });

                            // Deploy the TransferTaskCancelListener vertical
                            vertx.deployVerticle(TransferTaskCancelListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskCancelListener",
                                    localOptions, res8 -> {
                                        if (res8.succeeded()) {
                                            log.info("TransferTaskCancelListener Deployment id is " + res8.result());
                                        } else {
                                            log.error("TransferTaskCancelListener Deployment failed !");
                                        }
                                    });

                            // Deploy the TransferTaskPausedListener vertical
                            vertx.deployVerticle(TransferTaskPausedListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskPausedListener",
                                    localOptions, res9 -> {
                                        if (res9.succeeded()) {
                                            log.info("TransferTaskPausedListener Deployment id is " + res9.result());
                                        } else {
                                            log.error("TransferTaskPausedListener Deployment failed !");
                                        }
                                    });

                            // Deploy the NotificationListener vertical
                            vertx.deployVerticle(TransferTaskNotificationListener.class.getName(), //"org.agaveplatform.service.transfers.listener.NotificationListener",
                                    localOptions, res10 -> {
                                        if (res10.succeeded()) {
                                            log.info("NotificationListener Deployment id is " + res10.result());
                                        } else {
                                            log.error("NotificationListener Deployment failed !");
                                        }
                                    });

                                    // Deploy the TransferErrorListener vertical
                                    vertx.deployVerticle("org.agaveplatform.service.transfers.listener.TransferErrorListener",
                                            localOptions, res11 -> {
                                                if (res11.succeeded()){
                                                    System.out.println("TransferErrorListener Deployment id is " + res11.result());
                                                }else{
                                                    System.out.println("TransferErrorListener Deployment failed !");
                                                }
                                            });

                            // Deploy the TransferHealthcheckListener vertical
                            vertx.deployVerticle(TransferTaskHealthcheckListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferHealthcheckListener",
                                    localOptions, res12 -> {
                                        if (res12.succeeded()) {
                                            log.info("TransferHealthcheckListener Deployment id is " + res12.result());
                                        } else {
                                            log.error("TransferHealthcheckListener Deployment failed !");
                                        }
                                    });

                            // Deploy the TransferWatchListener vertical
                            vertx.deployVerticle(TransferTaskWatchListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferWatchListener",
                                    localOptions, res13 -> {
                                        if (res13.succeeded()) {
                                            log.info("TransferWatchListener Deployment id is " + res13.result());
                                        } else {
                                            log.error("TransferWatchListener Deployment failed !");
                                        }
                                    });

                            // Deploy the TransferTaskUpdateListener vertical
                            vertx.deployVerticle(TransferTaskUpdateListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskUpdateListener",
                                    localOptions, res14 -> {
                                        if (res14.succeeded()) {
                                            log.info("TransferTaskUpdateListener Deployment id is " + res14.result());
                                        } else {
                                            log.error("TransferTaskUpdateListener Deployment failed !");
                                        }
                                    });

                            // Deploy the TransferTaskFinishedListener vertical
                            vertx.deployVerticle(TransferTaskFinishedListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskFinishedListener",
                                    localOptions, res15 -> {
                                        if (res15.succeeded()) {
                                            log.info("TransferTaskFinishedListener Deployment id is " + res15.result());
                                        } else {
                                            log.error("TransferTaskFinishedListener Deployment failed !");
                                        }
                                    });
                        }
                        else {
                            log.error("TransferAPIVertical deployment failed !\n" + deployApiVerticle.cause());
                            deployApiVerticle.cause().printStackTrace();
                        }
                    });

                    return httpVerticleDeployment.future();

                }).onComplete(ar -> {
                    if (ar.succeeded()) {
                        log.info("TransferApiVertical ({}) started on port {}", ar.result(), config.getInteger("HTTP_PORT"));
                    } else {
                        log.error("TransferApiVertical deployment failed !", ar.cause());
                        System.exit(1);
                    }
                });
            } else {
                log.error("Error retrieving service configuration.");
                System.exit(1);
            }
        });
    }

//    protected static void deployTTC(Vertx vertx, DeploymentOptions localOptions, Handler<AsyncResult<Boolean>> handler ){
//    // Deployment TransferTaskCreatedListener verticle
//    log.info("got to deployTTC");
//        vertx.deployVerticle(TransferTaskCreatedListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskCreatedListener",
//        localOptions,res1 -> {
//            if (res1.succeeded()) {
//                log.info("TransferTaskCreatedListener Deployment id is " + res1.result());
//                handler.handle(Future.succeededFuture(true));
//            } else {
//                log.error("TransferTaskCreatedListener Deployment failed !");
//                handler.handle(Future.succeededFuture(false));
//            }
//        });
//    }
}
