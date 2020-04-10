package org.agaveplatform.service.transfers.resources;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.validation.HTTPRequestValidationHandler;

import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthHandlerImpl;

import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.TransferUpdate;
import org.agaveplatform.service.transfers.util.AgaveSchemaFactory;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.TransferRateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;

/**
 * This is the user-facing vertical providing the http interface to internal services.
 * It provides crud functionality and basic jwt authorization.
 */
public class TransferServiceUIVertical extends AbstractVerticle {

    private static Logger log = LoggerFactory.getLogger(TransferServiceUIVertical.class);

    private HttpServer server;
    private JWTAuth authProvider;
    private JsonObject config;
    private TransferTaskDatabaseService dbService;
    protected String eventChannel = "transfertask.db.queue";


    public TransferServiceUIVertical(Vertx vertx) {
        this(vertx, null);
    }

    public TransferServiceUIVertical(Vertx vertx, String eventChannel) {
        super();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }


    @Override
    public void start(Promise<Void> promise) {
        // set the config from the main vertical
        setConfig(config());

        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE, "transfertask.db.queue"); // <1>
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);


        // define our routes
        Router router = Router.router(vertx);
        router.route().handler(BodyHandler.create());

        // Bind "/" to our hello message - so we are still compatible.
        router.route("/").handler(routingContext -> {
            HttpServerResponse response = routingContext.response();
            JsonObject message = new JsonObject().put("message", "Hello");
            response
                    .putHeader("content-type", "application/json")
                    .end(message.encodePrettily());
        });

        // create a jwt auth provider and apply it as the first handler for all routes
        if (config().getBoolean(CONFIG_TRANSFERTASK_JWT_AUTH)) {
            router.route("/api/transfers*").handler(new AgaveJWTAuthHandlerImpl(getAuthProvider(), (String) null));
        }

        // define the service routes
        router.get("/api/transfers").handler(this::getAll);
        router.get("/api/transfers/:uuid").handler(this::getOne);
        router.delete("/api/transfers/:uuid").handler(this::deleteOne);

        // Accept post of a TransferTask, validates the request, and inserts into the db.
        router.post("/api/transfers")
                // Mount validation handler to ensure the posted json is valid prior to adding
                .handler(HTTPRequestValidationHandler.create().addJsonBodySchema(AgaveSchemaFactory.getForClass(TransferTask.class)))
                // Mount primary handler
                .handler(this::addOne);
        router.put("/api/transfers/:uuid")
                // Mount validation handler to ensure the posted json is valid prior to adding
                .handler(HTTPRequestValidationHandler.create().addJsonBodySchema(AgaveSchemaFactory.getForClass(TransferUpdate.class)))
                // Mount primary handler
                .handler(this::updateOne);


        int portNumber = config().getInteger(CONFIG_TRANSFERTASK_HTTP_PORT, 8080);
        server = vertx.createHttpServer();
        server
            .requestHandler(router)
            .listen(portNumber, ar -> {
                if (ar.succeeded()) {
                    log.info("HTTP server running on port " + portNumber);
                    promise.complete();
                } else {
                    log.error("Could not start a HTTP server", ar.cause());
                    promise.fail(ar.cause());
                }
            });
    }


    // ---- HTTP Actions ----

    /**
     * Fetches all {@link TransferTask} from the db. Results are added to the routing context.
     * TODO: add pagination and querying.
     *
     * @param routingContext the current rounting context for the request
     */
    private void getAll(RoutingContext routingContext) {
        String tenantId = routingContext.get("tenantId");
        dbService.getAll(tenantId, reply -> {
            if (reply.succeeded()) {
                routingContext.response().end(reply.result().encodePrettily());
            } else {
                routingContext.fail(reply.cause());
            }
        });
    }

    /**
     * Inserts a new {@link TransferTask} into the db. Validation happens in a previous handler,
     * so this method only needs to worry about running the insertion.
     *
     * @param routingContext the current rounting context for the request
     */
    private void addOne(RoutingContext routingContext) {
        TransferTask transferTask = routingContext.getBodyAsJson().mapTo(TransferTask.class);
        dbService.create(routingContext.get("tenantId"), transferTask, reply -> {
            if (reply.succeeded()) {
                vertx.eventBus().publish("transfertask.created", reply.result());
                routingContext.response().end(reply.result().encodePrettily());
            } else {
                routingContext.fail(reply.cause());
            }
        });
    }

    /**
     * Delete a {@link TransferTask} from the db.
     *
     * @param routingContext the current rounting context for the request
     */
    private void deleteOne(RoutingContext routingContext) {
        String tenantId = routingContext.get("tenantId");
        String uuid = routingContext.pathParam("uuid");
        dbService.delete(tenantId, uuid, reply -> {
            if (reply.succeeded()) {
                vertx.eventBus().publish("transfertask.deleted", reply.result());
                routingContext.response().setStatusCode(203).end();
            } else {
                routingContext.fail(reply.cause());
            }
        });
    }

    /**
     * Fetches a single {@link TransferTask} from the db.
     *
     * @param routingContext the current rounting context for the request
     */
    private void getOne(RoutingContext routingContext) {
        String tenantId = routingContext.get("tenantId");
        String uuid = routingContext.pathParam("uuid");
        dbService.getById(tenantId, uuid, reply -> {
            if (reply.succeeded()) {
                if (reply.result() == null) {
                    routingContext.fail(404);
                } else {
                    routingContext.response().end(reply.result().encodePrettily());
                }
            } else {
                routingContext.fail(reply.cause());
            }
        });
    }

    /**
     * Update an existing {@link TransferTask}. Validation is performed in a prior handler, so this
     * method needs only to update the db.
     *
     * @param routingContext the current rounting context for the request
     */
    private void updateOne(RoutingContext routingContext) {
        String tenantId = routingContext.get("tenantId");
        String uuid = routingContext.request().getParam("uuid");
        TransferUpdate transferUpdate = routingContext.getBodyAsJson().mapTo(TransferUpdate.class);

        dbService.getById(tenantId, uuid, reply -> {
            if (reply.succeeded()) {
                if (reply.result() == null) {
                    routingContext.fail(404);
                } else {
                    TransferTask tt = TransferRateHelper.updateSummaryStats(new TransferTask(reply.result()), transferUpdate);

                    dbService.update(tenantId, uuid, tt, reply2 -> {
                        if (reply2.succeeded()) {
                            vertx.eventBus().publish("transfertask.updated", reply2.result());
                            routingContext.response().end(reply2.result().encodePrettily());
                        } else {
                            routingContext.fail(reply2.cause());
                        }
                    });
                }
            } else {
                routingContext.fail(reply.cause());
            }
        });
    }


    // --------- Getters and Setters --------------

    public JWTAuth getAuthProvider() {
        if (authProvider == null) {
            JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
                    .addPubSecKey(new PubSecKeyOptions()
                            .setAlgorithm("RS256")
                            .setPublicKey(config.getString("publickey")));

            authProvider = JWTAuth.create(vertx, jwtAuthOptions);
        }

        return authProvider;
    }

    public void setAuthProvider(JWTAuth authProvider) {
        this.authProvider = authProvider;
    }

    public void setConfig(JsonObject config) {
        this.config = config;
    }

    /**
     * Sets the vertx instance for this listener
     *
     * @param vertx the current instance of vertx
     */
    private void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * @return the message type to listen to
     */
    public String getEventChannel() {
        return eventChannel;
    }

    /**
     * Sets the message type for which to listen
     *
     * @param eventChannel
     */
    public void setEventChannel(String eventChannel) {
        this.eventChannel = eventChannel;
    }

}
