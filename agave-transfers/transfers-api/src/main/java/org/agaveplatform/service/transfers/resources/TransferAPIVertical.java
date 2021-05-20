package org.agaveplatform.service.transfers.resources;

import io.nats.client.JetStreamSubscription;
import org.agaveplatform.service.transfers.messaging.*;
import io.nats.client.Connection;
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
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthHandlerImpl;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthProviderImpl;
import io.vertx.ext.web.handler.impl.Wso2JwtUser;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.listener.AbstractNatsListener;
import org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.model.TransferUpdate;
import org.agaveplatform.service.transfers.util.CryptoHelper;
import org.agaveplatform.service.transfers.util.ServiceUtils;
import org.agaveplatform.service.transfers.util.TransferRateHelper;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.iplantc.service.common.Settings;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.MessagingException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static io.nats.client.Nats.connect;
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
    private RemoteDataClient sourceClient;
    private RemoteDataClient destClient;
    private NatsJetstreamMessageClient natsCleint;

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
        router.get("/api/transfers/:uuid").handler(this::getOne);

        router.delete("/api/transfers/deleteAll").handler(this::deleteAll);
        router.delete("/api/transfers/:uuid").handler(this::deleteOne);
        // Accept post of a TransferTask, validates the request, and inserts into the db.
        router.post("/api/transfers")
                // Mount validation handler to ensure the posted json is valid prior to adding
//                .handler(HTTPRequestValidationHandler.create().addJsonBodySchema(AgaveSchemaFactory.getForClass(TransferTaskRequest.class)))
                // Mount primary handler
                .handler(this::addOne);

        router.put("/api/transfers/:uuid")
                // Mount validation handler to ensure the posted json is valid prior to adding
//                .handler(HTTPRequestValidationHandler.create().addJsonBodySchema(AgaveSchemaFactory.getForClass(TransferUpdate.class)))
                // Mount primary handler
                .handler(this::updateOne);

        // Accept post of a cancel TransferTask, validates the request, and updates the db.
        router.post("/api/transfers/:uuid/cancel")
                // Mount primary handler
                .handler(this::cancelOne);

        // Accept post of a cancel TransferTask, validates the request, and inserts into the db.
        router.post("/api/transfers/cancel")
                // Mount validation handler to ensure the posted json is valid prior to adding
//                .handler(HTTPRequestValidationHandler.create().addJsonBodySchema(AgaveSchemaFactory.getForClass(TransferTaskRequest.class)))
                // Mount primary handler
                .handler(this::cancelAll);

//        Route cancelRoute = router.put("/api/cancelone");
//        cancelRoute.failureHandler(failureRoutingContext -> {
//            log.debug("error occurred {}, {}", failureRoutingContext.response().getStatusCode(), failureRoutingContext.response().getStatusMessage());
//
//            log.debug("failure url path params{}", failureRoutingContext.pathParams());
//            log.debug("failure url query params{}", failureRoutingContext.queryParams());
//            log.debug("failure body {}", failureRoutingContext.getBodyAsString());
//
//            int statusCode = failureRoutingContext.statusCode();
//            HttpServerResponse response = failureRoutingContext.response();
//            response.setStatusCode(statusCode).end("Sorry! Not today");
//        });


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
//        TransferTaskRequest transferTaskRequest = new TransferTaskRequest(body);
        TransferTask transferTask = new TransferTask();
        transferTask.setCreated(Instant.now());
        transferTask.setTenantId(tenantId);
        transferTask.setOwner(username);
        try {
            AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(source));
            sourceClient = getRemoteDataClient(transferTask.getTenantId(), transferTask.getOwner(), srcUri.get());

            //URLEncode paths
            transferTask.setSource(URI.create(body.getString("source")).toString());
            transferTask.setDest(URI.create(body.getString("dest")).toString());

            RemoteDataClient finalSrcClient = sourceClient;
            dbService.create(tenantId, transferTask, reply -> {
                if (reply.succeeded()) {
                    TransferTask tt = new TransferTask(reply.result());
                    try {
                        //transfers.$tenantid.$uid.$systemid.transfer.$protocol
                        srcUri.set(URI.create(source));
                        String messageName = _createConsumerName(streamName,"transfers", tt.getTenantId(), tt.getOwner(), sourceClient.getHost().toString(), MessageType.TRANSFERTASK_CREATED);
                        //_doPublishNatsJSEvent(messageName, tt.toJson());
                        natsCleint.push("DEV",messageName, tt.toJson().toString());
                        routingContext.response()
                            .putHeader("content-type", "application/json")
                                .setStatusCode(201)
                                .end(AgaveResponseBuilder.getInstance(routingContext)
                                        .setResult(tt.toJson())
                                        .build()
                                        .toString());
                    } catch(Exception e) {
                        log.debug(e.getMessage());
                    }
                } else {
                    routingContext.fail(reply.cause());
                }
            });
        } catch (SystemUnknownException | AgaveNamespaceException | RemoteCredentialException | PermissionException | FileNotFoundException | RemoteDataException  e) {
            log.debug(e.getMessage());
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
            AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(source));
            sourceClient = getRemoteDataClient(transferTask.getTenantId(), transferTask.getOwner(), srcUri.get());

            //URLEncode paths
            transferTask.setSource(URI.create(body.getString("source")).toString());
            transferTask.setDest(URI.create(body.getString("dest")).toString());

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
                                    try {

                                        String messageName = _createConsumerName("DEV", "transfers", transferTask.getTenantId(), transferTask.getOwner(), sourceClient.getHost().toString(),MessageType.TRANSFERTASK_CANCELED);
                                        natsCleint = new NatsJetstreamMessageClient(config().getString("NATS_URI"), streamName, messageName);
                                        //_doPublishNatsJSEvent( messageName, deleteReply.result());
                                        natsCleint.push("DEV", messageName, deleteReply.result().toString());

                                        routingContext.response()
                                                .putHeader("content-type", "application/json")
                                                .setStatusCode(203).end();
                                    } catch (Exception e) {
                                        log.debug(e.getMessage());
                                    }
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
        } catch (SystemUnknownException | AgaveNamespaceException | RemoteCredentialException | PermissionException | FileNotFoundException | RemoteDataException  e) {
            log.debug(e.getMessage());
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
            AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(source));
            sourceClient = getRemoteDataClient(transferTask.getTenantId(), transferTask.getOwner(), srcUri.get());

            //URLEncode paths
            transferTask.setSource(URI.create(body.getString("source")).toString());
            transferTask.setDest(URI.create(body.getString("dest")).toString());

            transferTask.setSource(body.getString("source"));
            transferTask.setDest(body.getString("dest"));

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
                                    JsonObject jo = null;
                                    if (deleteReply.result() == null){
                                        jo = new JsonObject();
                                    }else{
                                        jo = new JsonObject(String.valueOf(deleteReply.result()));
                                    }
                                    // _doPublishEvent(MessageType.TRANSFERTASK_DELETED, deleteReply.result());
                                    //Todo need to write the TransferTaskDeletedListener.  Then the TRANSFERTASK_DELETED message will be actied on;
                                    try {
                                        String messageName = _createConsumerName("DEV", "transfers", transferTask.getTenantId(), transferTask.getOwner(), sourceClient.getHost().toString(), MessageType.TRANSFERTASK_CANCELED);
                                        //_doPublishNatsJSEvent(messageName, jo);
                                        natsCleint.push("DEV", messageName, jo.toString());
                                    } catch (MessagingException e) {
                                        log.debug(e.getMessage());
                                    }

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


            dbService.getByUuid(tenantId, transferTask.getUuid(), reply -> {
                if (reply.succeeded()) {
                    TransferTask tt = new TransferTask(reply.result());
                    try {
                        String messageName = _createConsumerName("DEV", "transfers", transferTask.getTenantId(), transferTask.getOwner(), sourceClient.getHost().toString(),MessageType.TRANSFERTASK_CANCELED);
                       //_doPublishNatsJSEvent(messageName, jo);
                        natsCleint.push("DEV", messageName, tt.toString());
                        //_doPublishNatsJSEvent(messageName, tt.toJson());
                        routingContext.response()
                                .putHeader("content-type", "application/json")
                                .setStatusCode(201)
                                .end(AgaveResponseBuilder.getInstance(routingContext)
                                        .setResult(tt.toJson())
                                        .build()
                                        .toString());
                    } catch (Exception e) {
                        log.debug(e.getMessage());
                    }
                } else {
                    routingContext.fail(reply.cause());
                }
            });

        } catch (SystemUnknownException | AgaveNamespaceException | RemoteCredentialException | PermissionException | FileNotFoundException | RemoteDataException  e) {
            log.debug(e.getMessage());
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
        JsonObject body = routingContext.getBodyAsJson();
        String source = body.getString("source");
        String dest = body.getString("dest");

        TransferTask transferTask = new TransferTask();
        transferTask.setCreated(Instant.now());
        transferTask.setTenantId(tenantId);
        transferTask.setOwner(username);
        try {
            AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(source));
            sourceClient = getRemoteDataClient(transferTask.getTenantId(), transferTask.getOwner(), srcUri.get());

            //URLEncode paths
            transferTask.setSource(URI.create(body.getString("source")).toString());
            transferTask.setDest(URI.create(body.getString("dest")).toString());

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
                                    JsonObject jo = null;
                                    if (deleteReply.result() == null){
                                        jo = new JsonObject();
                                    }else{
                                        jo = new JsonObject(String.valueOf(deleteReply.result()));
                                    }
                                    try {
                                        //String messageName = _createMessageName("transfers", transferTask.getTenantId(), transferTask.getOwner(), sourceClient.getHost().toString(),MessageType.TRANSFERTASK_CANCELED);
                                        //_doPublishNatsJSEvent(messageName, jo);

                                        String messageName = _createConsumerName("DEV", "transfers", transferTask.getTenantId(), transferTask.getOwner(), sourceClient.getHost().toString(),MessageType.TRANSFERTASK_DELETED);
                                        natsCleint.push("DEV", messageName, jo.toString());

                                        natsCleint.push("DEV", messageName, jo.toString());
                                    } catch (MessagingException e) {
                                        log.debug(e.getMessage());
                                    }

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
        } catch (SystemUnknownException | AgaveNamespaceException | RemoteCredentialException | PermissionException | FileNotFoundException | RemoteDataException  e) {
            log.debug(e.getMessage());
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
            AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(source));
            sourceClient = getRemoteDataClient(transferTask.getTenantId(), transferTask.getOwner(), srcUri.get());

            //URLEncode paths
            transferTask.setSource(URI.create(body.getString("source")).toString());
            transferTask.setDest(URI.create(body.getString("dest")).toString());


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
                                    JsonObject jo = null;
                                    if (deleteReply.result() == null){
                                        jo = new JsonObject();
                                    }else{
                                        jo = new JsonObject(String.valueOf(deleteReply.result()));
                                    }
                                    try {
                                        String messageName = _createConsumerName("DEV", "transfers", transferTask.getTenantId(), transferTask.getOwner(), sourceClient.getHost().toString(),MessageType.TRANSFERTASK_DELETED);
                                        //_doPublishNatsJSEvent(messageName, jo);
                                        natsCleint.push("DEV", messageName, jo.toString());
                                    } catch (MessagingException e) {
                                        log.debug(e.getMessage());
                                    }

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

        } catch (SystemUnknownException | AgaveNamespaceException | RemoteCredentialException | PermissionException | FileNotFoundException | RemoteDataException  e) {
            log.debug(e.getMessage());
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

        JsonObject body = routingContext.getBodyAsJson();
        String source = body.getString("source");
        String dest = body.getString("dest");

        TransferTask transferTask = new TransferTask();
        transferTask.setCreated(Instant.now());
        transferTask.setTenantId(tenantId);
        transferTask.setOwner(username);

        try {
            AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(source));
            sourceClient = getRemoteDataClient(transferTask.getTenantId(), transferTask.getOwner(), srcUri.get());

            //URLEncode paths
            transferTask.setSource(URI.create(body.getString("source")).toString());
            transferTask.setDest(URI.create(body.getString("dest")).toString());


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
        } catch (SystemUnknownException | AgaveNamespaceException | RemoteCredentialException | PermissionException | FileNotFoundException | RemoteDataException  e) {
            log.debug(e.getMessage());
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
            AtomicReference<URI> srcUri = new AtomicReference<URI>(URI.create(source));
            sourceClient = getRemoteDataClient(transferTask.getTenantId(), transferTask.getOwner(), srcUri.get());

            //URLEncode paths
            transferTask.setSource(URI.create(body.getString("source")).toString());
            transferTask.setDest(URI.create(body.getString("dest")).toString());


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
                                    //_doPublishNatsJSEvent(MessageType.TRANSFERTASK_UPDATED, updateReply.result());

                                    String messageName = _createConsumerName("DEV", "transfers", transferTask.getTenantId(), transferTask.getOwner(), sourceClient.getHost().toString(),MessageType.TRANSFERTASK_UPDATED);
                                    //_doPublishNatsJSEvent(messageName, jo);
                                    natsCleint.push("DEV", messageName, updateReply.result().toString());

                                    routingContext.response().end(
                                            AgaveResponseBuilder.getInstance(routingContext)
                                                    .setResult(updateReply.result())
                                                    .build()
                                                    .toString());
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
        } catch (SystemUnknownException | AgaveNamespaceException | RemoteCredentialException | PermissionException | FileNotFoundException | RemoteDataException  e) {
            log.debug(e.getMessage());
        }
    }


    // --------- Helper methods for events --------------

    /**
     * Publishes a message to the named event channel.
     * @param event the event channel to which the message will be published
     * @param message the message to publish
     */
//    public void _doPublishEvent(String event, Object message) {
//        log.debug("Publishing {} event: {}", event, message);
//        getVertx().eventBus().publish(event, message);
//    }

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
                System.out.println(authProvider.toString());
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
    public void setVertx(Vertx vertx) {
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
        String jwtString = jwtAuth.generateToken(claims, jwtOptions);
        return jwtString;
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
        System.out.println("username = "+username);
        return makeTestJwt(username, "");
    }

    /**
     * Returns shortname for package containing a {@link RemoteDataClient}. This
     * allows us to determine the protocol used by that client quickly for logging
     * purposes. For example S3JCloud => s3, MaverickSFTP => sftp.
     *
     * @param clientClass class for which to get the protocol
     * @return data protocol shortname used by a client.
     */
    private Object getProtocolForClass(Class<? extends RemoteDataClient> clientClass) {
        String fullName = clientClass.getName();
        String[] tokens = fullName.split("\\.");
        return tokens[tokens.length - 2];
    }

    /**
     * Parses the hostname out of a URI. This is used to extract systemId info from
     * the TransferTask.rootTask.source and TransferTask.rootTask.dest fields and
     * create the child source and dest values.
     *
     * @param serializedUri
     * @return
     */
    private String getSystemId(String serializedUri) {
        URI uri = null;
        try {
            uri = URI.create(serializedUri);
            return uri.getHost();
        } catch (Exception e) {
            return null;
        }
    }

    protected RemoteDataClient getRemoteDataClient(String tenantId, String username, URI target) throws NotImplementedException, SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, FileNotFoundException, RemoteDataException {
        TenancyHelper.setCurrentTenantId(tenantId);
        return new RemoteDataClientFactory().getInstance(username, null, target);
    }

}
