package org.agaveplatform.service.transfers.resources;

import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.healthchecks.HealthCheckHandler;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.jwt.JWTOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthHandlerImpl;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthProviderImpl;
import io.vertx.ext.web.handler.impl.Wso2JwtUser;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.listener.AbstractNatsListener;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.model.TransferUpdate;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.agaveplatform.service.transfers.util.ServiceUtils;
import org.agaveplatform.service.transfers.util.TransferRateHelper;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.Settings;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.*;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_DB_QUEUE;

/**
 * This is the user-facing vertical providing the http interface to internal services.
 * It provides crud functionality and basic jwt authorization.
 */
public class TransferAPIVertical extends AbstractNatsListener {

    private final static Logger log = LoggerFactory.getLogger(TransferAPIVertical.class);

    private JWTAuth authProvider;
    private TransferTaskDatabaseService dbService;
    protected String eventChannel = TRANSFERTASK_DB_QUEUE;
    protected JWTAuth jwtAuth;

    public TransferAPIVertical() throws IOException, InterruptedException {super();}

    public TransferAPIVertical(Vertx vertx) throws IOException, InterruptedException {
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
        DateTimeZone.setDefault(DateTimeZone.forID("America/Chicago"));
        TimeZone.setDefault(TimeZone.getTimeZone("America/Chicago"));

        // set the config from the main vertical
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE, TRANSFERTASK_DB_QUEUE);
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

        // add health check handler
        router.get("/health").handler(initHealthCheckHandler());

        // generate a client jwt if enabled at startup
        router.get("/api/client").handler(routingContext -> {
            jwtAuth = getAuthProvider() ;
            HttpServerResponse response = routingContext.response();

            String token = this.makeTestJwt("testuser");
            JsonObject message = new JsonObject().put("JWT", token);
            response
                    .putHeader("content-type", "application/json")
                    .end(message.encodePrettily());
        });

        // define the service routes
        router.get("/api/transfers").handler(this::getAll);
        router.post("/api/transfers").handler(this::addOne);
        router.delete("/api/transfers").handler(this::deleteAll);

        router.get("/api/transfers/:uuid").handler(this::getOne);
        router.put("/api/transfers/:uuid").handler(this::updateOne);
        router.delete("/api/transfers/:uuid").handler(this::deleteOne);

        router.post("/api/transfers/:uuid/cancel").handler(this::cancelOne);

        // Accept post of a cancel TransferTask, validates the request, and inserts into the db.
        router.post("/api/transfers/cancel")
                .handler(this::cancelAll);

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

    /**
     * Creates a {@link HealthCheckHandler} and registers checks for db and message queue health.
     * @return the registered health check handler
     */
    private Handler<RoutingContext> initHealthCheckHandler() {
        // Register health checks for db and queue. If the endpoints are down, the health check won't respond anyway
        HealthCheckHandler healthCheckHandler = HealthCheckHandler.create(vertx);

        // db check
        healthCheckHandler.register("db-check", 2000, dbCheckPromise -> {
            dbService.ping(resp -> {
                if (resp.succeeded()) {
                    dbCheckPromise.complete(Status.OK());
                } else {
                    dbCheckPromise.complete(Status.KO());
                }
            });
        });

        // message queue check
        healthCheckHandler.register("queue-check", 2000, dbCheckPromise -> {
            // put call to nats health check here
            dbCheckPromise.complete(Status.OK());
//            if (natsClient.isAlive()) {
//                dbCheckPromise.complete(Status.OK());
//            } else {
//                dbCheckPromise.complete(Status.KO());
//            };
        });

        return healthCheckHandler;
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
    private void addOne(RoutingContext routingContext) {
        log.debug("addOne method");

        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");

        log.debug("username = {}", username);
        JsonObject body = routingContext.getBodyAsJson();
        String source = body.getString("source");
        String dest = body.getString("dest");

        // request body was validated prior to this method being called
        TransferTask transferTask = new TransferTask();
        transferTask.setCreated(Instant.now());
        transferTask.setTenantId(tenantId);
        transferTask.setOwner(username);
        try {

            final URI srcUri = URI.create(source);
            final URI destUri = URI.create(dest);

            //URLEncode paths
            transferTask.setSource(srcUri.toString());
            transferTask.setDest(destUri.toString());

            dbService.create(tenantId, transferTask, reply -> {
                if (reply.succeeded()) {
                    TransferTask tt = new TransferTask(reply.result());

                    String subject = createPushMessageSubject(
                            tt.getTenantId(), tt.getOwner(), srcUri.getHost(),
                            MessageType.TRANSFERTASK_CREATED);

                    _doPublishEvent(subject, tt.toJson(), createdResp -> {
                        if (createdResp.succeeded()) {
                            routingContext.response()
                                    .putHeader("content-type", "application/json")
                                    .setStatusCode(201)
                                    .end(AgaveResponseBuilder.getInstance(routingContext)
                                            .setResult(tt.toJson())
                                            .build()
                                            .toString());
                        } else {
                            routingContext.response()
                                    .putHeader("content-type", "application/json")
                                    .setStatusCode(500)
                                    .setStatusMessage("Failed to send created event due to internal error.").end();
                        }
                    });
                } else {
                    routingContext.fail(reply.cause());
                }
            });
        } catch (IllegalArgumentException e) {
            routingContext.response()
                    .setStatusCode(400)
                    .setStatusMessage("Invalid source or destination URL in the transfer request.").end();
        }
    }

    /**
     * Updates the transfertask to deleted {@link TransferTask} into the db. Validation happens in a previous handler,
     * so this method only needs to worry about running the deletion.
     *
     * @param routingContext the current rounting context for the request
     */
    private void cancelOne(RoutingContext routingContext) {
        log.debug("cancelOne method");
        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");
        String uuid = routingContext.pathParam("uuid");

        try {
            // lookup task to get the id
            dbService.getByUuid(tenantId, uuid, getByIdReply -> {
                if (getByIdReply.succeeded()) {
                    if (getByIdReply.result() == null) {
                        // not found
                        routingContext.fail(404);
                    } else {
                        // if the current user is the owner or has admin privileges, allow the action
                        if (StringUtils.equals(username, getByIdReply.result().getString("owner")) ||
                                user.isAdminRoleExists()) {
                            dbService.updateStatus(tenantId, uuid, TransferStatusType.CANCELLED.name(),deleteReply -> {
                                if (deleteReply.succeeded()) {
                                    TransferTask transferTask = new TransferTask(deleteReply.result());
                                    final URI srcUri = URI.create(transferTask.getSource());

                                    String subject = createPushMessageSubject(
                                            transferTask.getTenantId(), transferTask.getOwner(), srcUri.getHost(),
                                            MessageType.TRANSFERTASK_CANCELED);

                                    _doPublishEvent(subject, deleteReply.result(), cancelResp -> {
                                        if (cancelResp.succeeded()) {
                                            routingContext.response()
                                                    .putHeader("content-type", "application/json")
                                                    .setStatusCode(203).end();
                                        } else {
                                            routingContext.response()
                                                    .putHeader("content-type", "application/json")
                                                    .setStatusCode(500)
                                                    .setStatusMessage("Failed to send cancelled event due to internal error.").end();
                                        }
                                    });
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
        } catch (IllegalArgumentException e) {
            routingContext.response()
                    .setStatusCode(500)
                    .setStatusMessage("Internal error parsing the transfer task source and destination.").end();
        }
    }

    /**
     * Inserts a new {@link TransferTask} into the db. Validation happens in a previous handler,
     * so this method only needs to worry about running the insertion.
     *
     * @param routingContext the current rounting context for the request
     */
    private void cancelAll(RoutingContext routingContext)   {
        log.debug("cancelAll method");
        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");
        log.debug("username = {}", username);

        JsonObject body = routingContext.getBodyAsJson();
        String source = body.getString("source");
        String dest = body.getString("dest");
        // request body was validated prior to this method being called
//        TransferTaskRequest transferTaskRequest = new TransferTaskRequest(body);
        TransferTask transferTask = new TransferTask();
        transferTask.setCreated(Instant.now());
        transferTask.setTenantId(tenantId);
        transferTask.setOwner(username);
        try {
            final URI srcUri = URI.create(source);
            final URI destUri = URI.create(dest);

            //URLEncode paths
            transferTask.setSource(srcUri.toString());
            transferTask.setDest(destUri.toString());

            // lookup task to get the id
            dbService.getByUuid(tenantId, transferTask.getUuid(), getByIdReply -> {
                if (getByIdReply.succeeded()) {
                    if (getByIdReply.result() == null) {
                        // not found
                        routingContext.fail(404);
                    } else {
                        // if the current user is the owner or has admin privileges, allow the action
                        if (StringUtils.equals(username, getByIdReply.result().getString("owner")) ||
                                user.isAdminRoleExists()) {
                            dbService.cancelAll(tenantId, deleteReply -> {
                                if (deleteReply.succeeded()) {
                                    JsonObject jo = new JsonObject(String.valueOf(deleteReply.result()));

                                    // TODO: need to write the TransferTaskDeletedListener.  Then the TRANSFERTASK_DELETED message will be actied on;
                                    String subject = createPushMessageSubject(
                                            transferTask.getTenantId(), transferTask.getOwner(), srcUri.getHost(),
                                            MessageType.TRANSFERTASK_CANCELED);

                                    _doPublishEvent(subject, jo, pubResp -> {
                                        if (pubResp.succeeded()) {
                                            routingContext.response()
                                                    .putHeader("content-type", "application/json")
                                                    .setStatusCode(203).end();
                                        }
                                        else {
                                            routingContext.response()
                                                    .putHeader("content-type", "application/json")
                                                    .setStatusCode(500)
                                                    .setStatusMessage("Failed to send cancelled event due to internal error.").end();
                                        }
                                    });
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
        } catch (IllegalArgumentException e) {
            log.error("Invalid src URI in transfer task request.");
        }
    }

    /**
     * Delete a {@link TransferTask} from the db.
     *
     * @param routingContext the current routing context for the request
     */
    private void deleteOne(RoutingContext routingContext) {
        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");
        String uuid = routingContext.pathParam("uuid");

        try {
            // lookup task to get the id
            dbService.getByUuid(tenantId, uuid, getByIdReply -> {
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
                                    //delete returns empty result on successful delete
//                                    JsonObject jo = new JsonObject(String.valueOf(deleteReply.result()));

                                    TransferTask transferTask = new TransferTask(getByIdReply.result());
                                    final URI srcUri = URI.create(transferTask.getSource());

                                    String subject = createPushMessageSubject(
                                            transferTask.getTenantId(), transferTask.getOwner(), srcUri.getHost(),
                                            MessageType.TRANSFERTASK_DELETED);

                                    _doPublishEvent(subject, transferTask.toJson(), deleteResp -> {
                                        if (deleteResp.succeeded()) {
                                            routingContext.response()
                                                    .putHeader("content-type", "application/json")
                                                    .setStatusCode(203).end();
                                        } else {
                                            routingContext.response()
                                                    .putHeader("content-type", "application/json")
                                                    .setStatusCode(500)
                                                    .setStatusMessage("Failed to send deleted event due to internal error.").end();
                                        }
                                    });
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
        } catch (IllegalArgumentException e) {
            log.error("Invalid src URI in transfer task request.");
        }
    }

    /**
     * Delete a {@link TransferTask} from the db.
     *
     * @param routingContext the current routing context for the request
     */
    private void deleteAll(RoutingContext routingContext) {
        log.info("Got into deleteAll.");
        Wso2JwtUser user = (Wso2JwtUser)routingContext.user();
        JsonObject principal = user.principal();
        String tenantId = principal.getString("tenantId");
        String username = principal.getString("username");
        String uuid = routingContext.pathParam("uuid");

        JsonObject body = routingContext.getBodyAsJson();
        String source = body.getString("source");
        String dest = body.getString("dest");

        TransferTask transferTask = new TransferTask();
        transferTask.setCreated(Instant.now());
        transferTask.setTenantId(tenantId);
        transferTask.setOwner(username);
        try {
            final URI srcUri = URI.create(source);
            final URI destUri = URI.create(dest);

            //URLEncode paths
            transferTask.setSource(srcUri.toString());
            transferTask.setDest(destUri.toString());

            // lookup task to get the id
            dbService.getByUuid(tenantId, uuid, getByIdReply -> {
                if (getByIdReply.succeeded()) {
                    if (getByIdReply.result() == null) {
                        // not found
                        routingContext.fail(404);
                    } else {
                        // if the current user is the owner or has admin privileges, allow the action
                        if (StringUtils.equals(username, getByIdReply.result().getString("owner")) ||
                                user.isAdminRoleExists()) {
                            dbService.deleteAll(tenantId, deleteReply -> {
                                if (deleteReply.succeeded()) {
                                   // _doPublishEvent(MessageType.TRANSFERTASK_DELETED, deleteReply.result());
                                    //Todo need to write the TransferTaskDeletedListener.  Then the TRANSFERTASK_DELETED message will be actied on;
                                    JsonObject jo = new JsonObject(String.valueOf(deleteReply.result()));

                                    String subject = createPushMessageSubject(
                                            transferTask.getTenantId(), transferTask.getOwner(), srcUri.getHost(),
                                            MessageType.TRANSFERTASK_DELETED);

                                    _doPublishEvent(subject, jo, deleteResp -> {
                                        if (deleteResp.succeeded()) {
                                            routingContext.response()
                                                    .putHeader("content-type", "application/json")
                                                    .setStatusCode(203).end();
                                        } else {
                                            routingContext.response()
                                                    .putHeader("content-type", "application/json")
                                                    .setStatusCode(500)
                                                    .setStatusMessage("Failed to send deleted event due to internal error.").end();
                                        }
                                    });
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
        } catch (IllegalArgumentException e) {
            log.error("Invalid src URI in transfer task request.");
        }
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

        try {
            // lookup the transfer task regardless
            dbService.getByUuid(tenantId, uuid, getByIdReply -> {
                if (getByIdReply.succeeded()) {
                    if (getByIdReply.result() == null) {
                        // not found
                        routingContext.fail(404);
                    } else {
                        // if the current user is the owner or has admin privileges, allow the action
                        if (StringUtils.equals(username, getByIdReply.result().getString("owner")) ||
                                user.isAdminRoleExists()) {

                            TransferTask transferTaskReturn = new TransferTask(getByIdReply.result());

                            // format and return the result
                            routingContext.response()
                                    .putHeader("content-type", "application/json")
                                    .end(
                                    responseBuilder
                                        .setResult(transferTaskReturn.toJson())
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
        } catch (IllegalArgumentException e) {
            log.error("Invalid src URI in transfer task request.");
        }

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


        JsonObject body = routingContext.getBodyAsJson();
        String source = body.getString("source");
        String dest = body.getString("dest");

        TransferTask transferTask = new TransferTask();
        transferTask.setCreated(Instant.now());

        try {
            final URI srcUri = URI.create(source);
            final URI destUri = URI.create(dest);

            //URLEncode paths
            transferTask.setSource(srcUri.toString());
            transferTask.setDest(destUri.toString());

            dbService.getByUuid(tenantId, uuid, getByIdReply -> {
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
                                    try {
                                        String subject = createPushMessageSubject(
                                                transferTask.getTenantId(), transferTask.getOwner(), srcUri.getHost(),
                                                MessageType.TRANSFERTASK_UPDATED);

                                        _doPublishEvent(subject, updateReply.result(), updateResp -> {
                                            if (updateResp.succeeded()) {
                                                routingContext.response().end(
                                                        AgaveResponseBuilder.getInstance(routingContext)
                                                                .setResult(updateReply.result())
                                                                .build()
                                                                .toString());
                                            } else {
                                                // update failed
                                                routingContext.response().setStatusCode(500)
                                                        .setStatusMessage("Transfer task was updated, but an error occurred sending the updated event.")
                                                        .end();

                                            }
                                        });


                                    } catch (Exception e) {
                                        log.debug(e.getMessage());
                                    }
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
        } catch (IllegalArgumentException e) {
            routingContext.response().setStatusCode(400).setStatusMessage("Invalid URI in transfer task request.").end();
        }
    }

    // --------- Getters and Setters --------------

    /**
     * Returns an {@link JWTAuth} object configured to validate a JWT signed with RS256 algorithm using the public
     * key pointed to by the service configs as {@code "transfertask.jwt.public_key"}
     *
     * @return {@link io.vertx.ext.auth.AuthProvider} for validating JWT.
     */
    @SuppressWarnings("deprecation")
    public JWTAuth getAuthProvider() {
        if (authProvider == null) {
            try {
                JWTAuthOptions jwtAuthOptions = null;
                if (config().getBoolean(CONFIG_TRANSFERTASK_JWT_VERIFY)) {
                    jwtAuthOptions = new JWTAuthOptions()
                            .setJWTOptions(new JWTOptions()
                                    .setLeeway(30)
                                    .setAlgorithm("RS256"))
                            .setPermissionsClaimKey("http://wso2.org/claims/role")
                            .addPubSecKey(new PubSecKeyOptions()
                                    .setAlgorithm("RS256")
                                    .setPublicKey(CryptoHelper.publicKey(config().getString("transfertask.jwt.public_key")))
                                    .setSecretKey(CryptoHelper.privateKey(config().getString("transfertask.jwt.private_key"))));

                } else {
                    jwtAuthOptions = new JWTAuthOptions()
                            .setPermissionsClaimKey("http://wso2.org/claims/role");
                }

                authProvider = new AgaveJWTAuthProviderImpl(jwtAuthOptions);
                System.out.println(authProvider);
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
    protected void setVertx(Vertx vertx) {
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

    /**
     * Gets the current db service proxy
     * @return proxy instance of {@link TransferTaskDatabaseService}
     */
    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    /**
     * Sets the {@link TransferTaskDatabaseService} to use
     * @param dbService the dbService to set
     */
    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }

    /**
     * Generates a JWT token to authenticate to the service. Token is signed using the
     * test private key in the service config file.
     *
     * @param username Name of the test user
     * @param roles Comma separated list of roles to append to the jwt
     * @return signed jwt token
     */
    protected String makeTestJwt(String username, String roles) {
        // Add wso2 claims set
        String givenName = username.replace("user", "");
        JsonObject claims = new JsonObject()
                .put("http://wso2.org/claims/subscriber", username)
                .put("http://wso2.org/claims/applicationid", "-9999")
                .put("http://wso2.org/claims/applicationname", "agaveops")
                .put("http://wso2.org/claims/applicationtier", "Unlimited")
                .put("http://wso2.org/claims/apicontext", "/internal")
                .put("http://wso2.org/claims/version", Settings.SERVICE_VERSION)
                .put("http://wso2.org/claims/tier", "Unlimited")
                .put("http://wso2.org/claims/keytype", "PRODUCTION")
                .put("http://wso2.org/claims/usertype", "APPLICATION_USER")
                .put("http://wso2.org/claims/enduser", username)
                .put("http://wso2.org/claims/TenantId", "-9999")
                .put("http://wso2.org/claims/emailaddress", username + "@example.com")
                .put("http://wso2.org/claims/fullname", org.apache.commons.lang3.StringUtils.capitalize(givenName) + " User")
                .put("http://wso2.org/claims/givenname", givenName)
                .put("http://wso2.org/claims/lastname", "User")
                .put("http://wso2.org/claims/primaryChallengeQuestion", "N/A")
                //.put("http://wso2.org/claims/tenantId", "sandbox")
                .put("http://wso2.org/claims/role", ServiceUtils.explode(",", List.of("Internal/everyone,Internal/subscriber", roles)))
                .put("http://wso2.org/claims/title", "N/A");

        JWTOptions jwtOptions = (JWTOptions) new JWTOptions()
                .setAlgorithm("RS256")
                .setExpiresInMinutes(10_080) // 7 days
                .setIssuer("transfers-api-integration-tests")
                .setSubject(username);
        log.debug("JWT string: {}, {}, {}, {}",jwtOptions.getAlgorithm(), jwtOptions.getExpiresInSeconds(), jwtOptions.getIssuer(), jwtOptions.getSubject());
        return jwtAuth.generateToken(claims, jwtOptions);
    }

    /**
     * Generates a JWT token to authenticate to the service. Token is signed using the
     * test private key in the service config file. Only the default user roles are assigned
     * to this jwt.
     *
     * @param username Name of the test user
     * @return signed jwt token
     * @see #makeTestJwt(String, String)
     */
    protected String makeTestJwt(String username) {
        System.out.println("username = " + username);
        return makeTestJwt(username, "");
    }

}
