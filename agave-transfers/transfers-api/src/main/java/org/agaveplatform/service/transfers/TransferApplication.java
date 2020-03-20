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

            } else {
                log.error("Error retrieving configuration.");
            }
        });
    }
}
