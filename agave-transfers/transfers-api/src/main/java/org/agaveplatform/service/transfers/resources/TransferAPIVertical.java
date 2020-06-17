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
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.validation.HTTPRequestValidationHandler;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthHandlerImpl;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthProviderImpl;
import io.vertx.ext.web.handler.impl.Wso2JwtUser;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.model.TransferTaskRequest;
import org.agaveplatform.service.transfers.model.TransferUpdate;
import org.agaveplatform.service.transfers.util.AgaveSchemaFactory;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.agaveplatform.service.transfers.util.TransferRateHelper;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_DB_QUEUE;

/**
 * This is the user-facing vertical providing the http interface to internal services.
 * It provides crud functionality and basic jwt authorization.
 */
public class TransferAPIVertical extends AbstractVerticle {

    private final static Logger log = LoggerFactory.getLogger(TransferAPIVertical.class);

    private JWTAuth authProvider;
    private TransferTaskDatabaseService dbService;
    protected String eventChannel = TRANSFERTASK_DB_QUEUE;

    public TransferAPIVertical(){super();}

    public TransferAPIVertical(Vertx vertx) {
        super();
        setVertx(vertx);
    }

    /**
     * Overriding the parent with a null safe check for the verticle context
     * before continuing. This affords us the luxury of spying on this instance
     * with a mock call chain.
     *
     * @return the config if present, an empty JsonObject otherwise
     */
    public JsonObject config() {
        if (this.context == null) {
            return new JsonObject();
        } else {
            return this.context.config();
        }
    }


    @Override
    public void start(Promise<Void> promise) {
        // set the config from the main vertical

        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE, TRANSFERTASK_DB_QUEUE); // <1>
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
            router.route("/api/transfers*").handler(new AgaveJWTAuthHandlerImpl(getAuthProvider()));
        }

        // define the service routes
        router.get("/api/transfers").handler(this::getAll);
        router.get("/api/transfers/:uuid").handler(this::getOne);
        router.delete("/api/transfers/:uuid").handler(this::deleteOne);

        // Accept post of a TransferTask, validates the request, and inserts into the db.
        router.post("/api/transfers")
                // Mount validation handler to ensure the posted json is valid prior to adding
                .handler(HTTPRequestValidationHandler.create().addJsonBodySchema(AgaveSchemaFactory.getForClass(TransferTaskRequest.class)))
                // Mount primary handler
                .handler(this::addOne);
        router.put("/api/transfers/:uuid")
                // Mount validation handler to ensure the posted json is valid prior to adding
                .handler(HTTPRequestValidationHandler.create().addJsonBodySchema(AgaveSchemaFactory.getForClass(TransferUpdate.class)))
                // Mount primary handler
                .handler(this::updateOne);

        router.errorHandler(500, ctx -> ctx.response()
            .putHeader("content-type", "application/json")
            .end(
                AgaveResponseBuilder.getInstance(ctx)
                        .setStatus(AgaveResponse.RequestStatus.error)
                        .setMessage(ctx.failure().getMessage())
                        .build()
                        .toString()));

        int portNumber = config().getInteger(CONFIG_TRANSFERTASK_HTTP_PORT, 8080);
        HttpServer server = vertx.createHttpServer();
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
     * If the user does not have admin privileges, then the call is delegated to {@link #getAllForUser(RoutingContext)}
     * and only results for the user are returned.
     * TODO: add querying.
     *
     * @param routingContext the current rounting context for the request
     */
    private void getAll(RoutingContext routingContext) {
        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        int limit = getPageSize(routingContext);
        int offset = getOffset(routingContext);

        if (user.isAdminRoleExists()) {
            dbService.getAll(tenantId, limit, offset, reply -> {
                if (reply.succeeded()) {
                    routingContext.response()
                            .putHeader("content-type", "application/json")
                            .end(
                            AgaveResponseBuilder.getInstance(routingContext)
                                    .setResult(reply.result())
                                    .build()
                                    .toString());
                } else {
                    routingContext.fail(reply.cause());
                }
            });
        } else {
            getAllForUser(routingContext);
        }
    }

    /**
     * Fetches all {@link TransferTask} from the db for the authenticated user. Results are added to the routing context.
     * TODO: add querying.
     *
     * @param routingContext the current rounting context for the request
     * @see #getAll(RoutingContext)
     */
    private void getAllForUser(RoutingContext routingContext) {
        JsonObject principal = routingContext.user().principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");
        int limit = getPageSize(routingContext);
        int offset = getOffset(routingContext);

        dbService.getAllForUser(tenantId, username, limit, offset, reply -> {
            if (reply.succeeded()) {
                routingContext.response()
                        .putHeader("content-type", "application/json")
                        .end(
                        AgaveResponseBuilder.getInstance(routingContext)
                            .setResult(reply.result())
                            .build()
                            .toString());
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
    private void addOne(RoutingContext routingContext) { Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");

        JsonObject body = routingContext.getBodyAsJson();
        // request body was validated prior to this method being called
//        TransferTaskRequest transferTaskRequest = new TransferTaskRequest(body);
        TransferTask transferTask = new TransferTask();
        transferTask.setTenantId(tenantId);
        transferTask.setOwner(username);
        transferTask.setSource(body.getString("source"));
        transferTask.setDest(body.getString("dest"));

        dbService.create(tenantId, transferTask, reply -> {
            if (reply.succeeded()) {
                TransferTask tt = new TransferTask(reply.result());
                _doPublishEvent(MessageType.TRANSFERTASK_CREATED, tt.toJson());
                routingContext.response()
                        .putHeader("content-type", "application/json")
                            .setStatusCode(201)
                            .end(AgaveResponseBuilder.getInstance(routingContext)
                                    .setResult(tt.toJson())
                                    .build()
                                    .toString());
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
        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");
        String uuid = routingContext.pathParam("uuid");

        // lookup task to get the id
        dbService.getById(tenantId, uuid, getByIdReply -> {
            if (getByIdReply.succeeded()) {
                if (getByIdReply.result() == null) {
                    // not found
                    routingContext.fail(404);
                } else {
                    // if the current user is the owner or has admin privileges, allow the action
                    if (StringUtils.equals(username, getByIdReply.result().getString("owner")) ||
                            user.isAdminRoleExists()) {
                        dbService.delete(tenantId, uuid, deleteReply -> {
                            if (deleteReply.succeeded()) {
                                _doPublishEvent(MessageType.TRANSFERTASK_DELETED, deleteReply.result());
                                routingContext.response()
                                        .putHeader("content-type", "application/json")
                                        .setStatusCode(203).end();
                            } else {
                                // delete failed
                                routingContext.fail(deleteReply.cause());
                            }
                        });
                    } else {
                        // permission denied if they don't have access
                        routingContext.fail(403);
                    }
                }
            } else {
                // task lookup failed
                routingContext.fail(getByIdReply.cause());
            }
        });
    }

    /**
     * Fetches a single {@link TransferTask} from the db.
     *
     * @param routingContext the current rounting context for the request
     */
    private void getOne(RoutingContext routingContext) {
        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");
        String uuid = routingContext.pathParam("uuid");
        AgaveResponseBuilder responseBuilder = AgaveResponseBuilder.getInstance(routingContext);

        // lookup the transfer task regardless
        dbService.getById(tenantId, uuid, getByIdReply -> {
            if (getByIdReply.succeeded()) {
                if (getByIdReply.result() == null) {
                    // not found
                    routingContext.fail(404);
                } else {
                    // if the current user is the owner or has admin privileges, allow the action
                    if (StringUtils.equals(username, getByIdReply.result().getString("owner")) ||
                            user.isAdminRoleExists()) {

                        TransferTask transferTask = new TransferTask(getByIdReply.result());

                        // format and return the result
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .end(
                                responseBuilder
                                    .setResult(transferTask.toJson())
                                    .build()
                                    .toString());
                    } else {
                        routingContext.fail(403);
                    }
                }
            } else {
                // task lookup failed
                routingContext.fail(getByIdReply.cause());
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
        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");
        String uuid = routingContext.request().getParam("uuid");
        TransferUpdate transferUpdate = routingContext.getBodyAsJson().mapTo(TransferUpdate.class);

        dbService.getById(tenantId, uuid, getByIdReply -> {
            if (getByIdReply.succeeded()) {
                if (getByIdReply.result() == null) {
                    /// not found
                    routingContext.fail(404);
                } else {
                    // if the current user is the owner or has admin privileges, allow the action
                    if (StringUtils.equals(username, getByIdReply.result().getString("owner")) ||
                            user.isAdminRoleExists()) {

                        TransferTask tt = TransferRateHelper.updateSummaryStats(new TransferTask(getByIdReply.result()), transferUpdate);

                        // perform the update
                        dbService.update(tenantId, uuid, tt, updateReply -> {
                            if (updateReply.succeeded()) {
                                _doPublishEvent(MessageType.TRANSFERTASK_UPDATED, updateReply.result());
                                routingContext.response().end(
                                        AgaveResponseBuilder.getInstance(routingContext)
                                                .setResult(updateReply.result())
                                                .build()
                                                .toString());
                            } else {
                                // update failed
                                routingContext.fail(updateReply.cause());
                            }
                        });
                    } else {
                        // permission denied
                        routingContext.fail(403);
                    }
                }
            } else {
                // task lookup failure
                routingContext.fail(getByIdReply.cause());
            }
        });
    }


    // --------- Helper methods for events --------------

    /**
     * Publishes a message to the named event channel.
     * @param event the event channel to which the message will be published
     * @param message the message to publish
     */
    public void _doPublishEvent(String event, Object message) {
        log.debug("Publishing {} event: {}", event, message);
        getVertx().eventBus().publish(event, message);
    }

    // --------- Getters and Setters --------------

    /**
     * Returns an {@link JWTAuth} object configured to validate a JWT signed with RS256 algorithm using the public
     * key pointed to by the service configs as {@code "transfertask.jwt.public_key"}
     *
     * @return {@link io.vertx.ext.auth.AuthProvider} for validating JWT.
     */
    public JWTAuth getAuthProvider() {
        if (authProvider == null) {
            try {
                @SuppressWarnings("deprecation")
                JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
                        .setJWTOptions(new JWTOptions()
                                .setLeeway(30)
                                .setAlgorithm("RS256"))
                        .setPermissionsClaimKey("http://wso2.org/claims/role")
                        .addPubSecKey(new PubSecKeyOptions()
                                .setAlgorithm("RS256")
                                .setPublicKey(CryptoHelper.publicKey(config().getString("transfertask.jwt.public_key"))));

                authProvider = new AgaveJWTAuthProviderImpl(jwtAuthOptions);

            } catch (IOException e) {
                log.error("Failed to load public key from file.", e);
            }
        }

        return authProvider;
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
     * Parses out the page size from the {@code limit} query parameter. This is only used when querying the collection.
     * Note: the page size is bound by 0 and {@link Settings#MAX_PAGE_SIZE};
     * @param routingContext the current request context
     * @return the integer value of the {@code limit} query parameter or {@code Settings#DEFAULT_PAGE_SIZE} if not set.
     */
    protected int getPageSize(RoutingContext routingContext) {
        List<String> params = routingContext.queryParam("limit");
        if (params.isEmpty()) {
            return Settings.DEFAULT_PAGE_SIZE;
        } else {
            String limit = params.get(0);
            if (StringUtils.isBlank(limit)) {
                return Settings.DEFAULT_PAGE_SIZE;
            } else {
                int querySize = Integer.parseInt(limit);
                return Math.min(Math.max(0, querySize), Settings.MAX_PAGE_SIZE);
            }
        }
    }

    /**
     * Parses out the number of records to skip in the result set from the {@code offset} query parameter. This is only
     * used when querying the collection.
     * Note: the offset must be at least 0;
     * @param routingContext the current request context
     * @return the integer value of the {@code offset} query parameter or 0 if not set.
     */
    protected int getOffset(RoutingContext routingContext) {
        List<String> params = routingContext.queryParam("offset");
        if (params.isEmpty()) {
            return 0;
        } else {
            String offset = params.get(0);
            if (StringUtils.isBlank(offset)) {
                return 0;
            } else {
                int offsetSize = Integer.parseInt(offset);
                return Math.max(0, offsetSize);
            }
        }
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }
}
