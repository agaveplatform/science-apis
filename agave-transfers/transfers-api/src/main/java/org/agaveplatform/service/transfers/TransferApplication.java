package org.agaveplatform.service.transfers;

import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.listener.*;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.protocol.TransferAllProtocolVertical;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class TransferApplication {

    private static final Logger log = LoggerFactory.getLogger(TransferApplication.class);
    public static final String DEFAULT_CONFIG_PATH = "config.json";

    public static void main(String[] args) {
        System.setProperty("vertx.logger-delegate-factory-class-name", "io.vertx.core.logging.SLF4JLogDelegateFactory");

        Vertx vertx = Vertx.vertx();

        int poolSize = 5;
        int instanceSize = 2;
        int dbInstanceSize = 2;

        String configPath = System.getenv("AGAVE_CONFIG_PATH");
        if (configPath == null) {
            configPath = DEFAULT_CONFIG_PATH;
        }

        ConfigStoreOptions fileStore = new ConfigStoreOptions()
                .setType("file")
                .setOptional(true)
                .setConfig(new JsonObject().put("path", configPath));

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
                        .setMaxWorkerExecuteTime(5000)
                        .setInstances(dbInstanceSize);

                Promise<String> dbVerticleDeployment = Promise.promise();
                log.info("org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle");
                vertx.deployVerticle("org.agaveplatform.service.transfers.database.TransferTaskDatabaseVerticle",
                        dbDeploymentOptions, dbVerticleDeployment);

                dbVerticleDeployment.future().compose(id -> {

                    Promise<String> httpVerticleDeployment = Promise.promise();
                    vertx.deployVerticle("org.agaveplatform.service.transfers.resources.TransferAPIVertical", new DeploymentOptions().setConfig(config), res -> {
                        if (res.succeeded()) {
                            log.info("TransferAPIVertical ({}) started on port {}", res.result(), config.getInteger("HTTP_PORT"));

                            DeploymentOptions localOptions = new DeploymentOptions()
                                    .setConfig(config)
                                    .setWorker(false)
                                    .setMaxWorkerExecuteTimeUnit(TimeUnit.SECONDS)
                                    .setMaxWorkerExecuteTime(30);

                            // Deployment TransferTaskCreatedListener verticle
                            vertx.deployVerticle(TransferTaskCreatedListener.class.getName(),
                                    localOptions, res30 -> {
                                        if (res30.succeeded()) {
                                            log.info("TransferTaskCreatedListener Deployment id is {}", res30.result());
                                        } else {
                                            log.error("TransferTaskCreatedListener Deployment failed ! {}", res30.result());
                                        }
                                    });


                            //Deploy the TransferTaskAssignedListener vertical
                            vertx.deployVerticle(TransferTaskAssignedListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskAssignedListener",
                                    localOptions, res0 -> {
                                        if (res0.succeeded()) {
                                            log.info("TransferTaskAssignedListener Deployment id is {}", res0.result());
                                        } else {
                                            log.error("TransferTaskAssignedListener Deployment failed ! {}", res0.result());
                                        }
                                    });

                            // Deploy the TransferRetryListener vertical
                            vertx.deployVerticle(TransferTaskRetryListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferRetryListener",
                                    localOptions, res3 -> {
                                        if (res3.succeeded()) {
                                            log.info("TransferRetryListener Deployment id is {}", res3.result());
                                        } else {
                                            log.error("TransferRetryListener Deployment failed ! ", res3.cause());
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
                                            log.info("TransferAllProtocolVertical Deployment id is " + res.result());
                                        } else {
                                            log.error("TransferAllProtocolVertical Deployment failed ! {}", res4.result());
                                        }
                                    });

                            // Deploy the TransferCompleteTaskListener vertical
                            vertx.deployVerticle(TransferTaskCompleteTaskListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferCompleteTaskListener",
                                    localOptions, res5 -> {
                                        if (res5.succeeded()) {
                                            log.info("TransferCompleteTaskListener Deployment id is " + res5.result());
                                        } else {
                                            log.error("TransferCompleteTaskListener Deployment failed ! {}",res5.result());
                                        }
                                    });


                            // Deploy the TransferTaskErrorListener vertical
                            vertx.deployVerticle(TransferTaskErrorListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferErrorTaskListener",
                                    localOptions, res6 -> {
                                        if (res6.succeeded()) {
                                            log.info("TransferTaskErrorTaskListener Deployment id is " + res6.result());
                                        } else {
                                            log.error("TransferTaskErrorTaskListener Deployment failed !{}", res6.result());
                                        }
                                    });

                            // Deploy the TransferFailureHandler vertical
                            vertx.deployVerticle(TransferTaskErrorFailureHandler.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferFailureHandler",
                                    localOptions, res7 -> {
                                        if (res7.succeeded()) {
                                            log.info("TransferFailureHandler Deployment id is " + res7.result());
                                        } else {
                                            log.error("TransferFailureHandler Deployment failed ! {}", res7.result());
                                        }
                                    });

                            // Deploy the TransferTaskCancelListener vertical
                            vertx.deployVerticle(TransferTaskCancelListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskCancelListener",
                                    localOptions, res8 -> {
                                        if (res8.succeeded()) {
                                            log.info("TransferTaskCancelListener Deployment id is " + res8.result());
                                        } else {
                                            log.error("TransferTaskCancelListener Deployment failed ! {}", res8.result());
                                        }
                                    });

                            // Deploy the TransferTaskPausedListener vertical
                            vertx.deployVerticle(TransferTaskPausedListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskPausedListener",
                                    localOptions, res9 -> {
                                        if (res9.succeeded()) {
                                            log.info("TransferTaskPausedListener Deployment id is " + res9.result());
                                        } else {
                                            log.error("TransferTaskPausedListener Deployment failed ! {}", res9.result());
                                        }
                                    });


                            DeploymentOptions notificationWorkerOptions = new DeploymentOptions()
                                    .setWorkerPoolName("notification-task-worker-pool")
                                    .setWorkerPoolSize(5)
                                    .setInstances(1)
                                    .setConfig(config)
                                    .setWorker(true)
                                    .setMaxWorkerExecuteTimeUnit(TimeUnit.HOURS)
                                    .setMaxWorkerExecuteTime(1);

                            // Deploy the NotificationListener vertical
                            vertx.deployVerticle(TransferTaskNotificationListener.class.getName(), //"org.agaveplatform.service.transfers.listener.NotificationListener",
                                    notificationWorkerOptions, res10 -> {
                                        if (res10.succeeded()) {
                                            log.info("NotificationListener Deployment id is " + res10.result());
                                        } else {
                                            log.error("NotificationListener Deployment failed ! {}", res10.result());
                                        }
                                    });

                            // Deploy the TransferErrorListener vertical
                            vertx.deployVerticle(TransferTaskErrorListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferErrorListener"
                                    localOptions, res11 -> {
                                        if (res11.succeeded()){
                                            log.info("TransferErrorListener Deployment id is " + res11.result());
                                        }else{
                                            log.error("TransferErrorListener Deployment failed ! {}", res11.result());
                                        }
                                    });

                            // Deploy the TransferHealthcheckListener vertical
                            vertx.deployVerticle(TransferTaskHealthcheckListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferHealthcheckListener",
                                    localOptions, res12 -> {
                                        if (res12.succeeded()) {
                                            log.info("TransferHealthcheckListener Deployment id is " + res12.result());
                                        } else {
                                            log.error("TransferHealthcheckListener Deployment failed ! {}", res12.result());
                                        }
                                    });
                            // Deploy the TransferHealthParentcheckListener vertical
                            vertx.deployVerticle(TransferTaskHealthcheckParentListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferHealthcheckListener",
                                    localOptions, res22 -> {
                                        if (res22.succeeded()) {
                                            log.info("TransferHealthcheckParentListener Deployment id is " + res22.result());
                                        } else {
                                            log.error("TransferHealthcheckParentListener Deployment failed ! {}", res22.result());
                                        }
                                    });
                            // Deploy the TransferWatchListener vertical
                            vertx.deployVerticle(TransferTaskWatchListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferWatchListener",
                                    localOptions, res13 -> {
                                        if (res13.succeeded()) {
                                            log.info("TransferWatchListener Deployment id is " + res13.result());
                                        } else {
                                            log.error("TransferWatchListener Deployment failed ! {}", res13.result());
                                        }
                                    });

                            // Deploy the TransferTaskUpdateListener vertical
                            vertx.deployVerticle(TransferTaskUpdateListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskUpdateListener",
                                    localOptions, res14 -> {
                                        if (res14.succeeded()) {
                                            log.info("TransferTaskUpdateListener Deployment id is " + res14.result());
                                        } else {
                                            log.error("TransferTaskUpdateListener Deployment failed ! {}", res14.result());
                                        }
                                    });

                            // Deploy the TransferTaskFinishedListener vertical
                            vertx.deployVerticle(TransferTaskFinishedListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskFinishedListener",
                                    localOptions, res15 -> {
                                        if (res15.succeeded()) {
                                            log.info("TransferTaskFinishedListener Deployment id is " + res15.result());
                                        } else {
                                            log.error("TransferTaskFinishedListener Deployment failed ! {}", res15.result());
                                        }
                                    });

                            // Deploy the Nats vertical
//                            vertx.deployVerticle(NatsListener.class.getName(), //"org.agaveplatform.service.transfers.listener.TransferTaskFinishedListener",
//                                    localOptions, res56 -> {
//                                        if (res56.succeeded()) {
//                                            log.info("Nats Deployment id is " + res56.result());
//                                        } else {
//                                            log.error("Nats Deployment failed !" + res56.result());
//                                        }
//                                    });
                        } else {
                            log.error("TransferAPIVertical deployment failed !\n ", res.cause());
                            res.cause().printStackTrace();
                        }
                    });

                    return httpVerticleDeployment.future();

                }).setHandler(ar -> {
                    NatsJetstreamMessageClient.disconnect();
                    if (ar.succeeded()) {
                        log.info("TransferApiVertical ({}) started on port {}", ar.result(), config.getInteger("HTTP_PORT"));

                    } else {
                        log.error("TransferApiVertical deployment failed !\n {}", ar.result());
                        ar.cause().printStackTrace();
                    }
                });
            } else {
                log.error("Error retrieving configuration.");
            }
        });
    }

}
