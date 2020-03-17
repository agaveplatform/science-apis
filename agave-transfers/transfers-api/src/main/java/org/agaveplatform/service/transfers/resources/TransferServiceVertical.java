package org.agaveplatform.service.transfers.resources;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.SQLOptions;
import io.vertx.ext.sql.UpdateResult;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.api.validation.HTTPRequestValidationHandler;
import io.vertx.ext.web.api.validation.ValidationException;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.impl.AgaveJWTAuthHandlerImpl;

import java.nio.file.Paths;
import java.util.stream.Collectors;

import org.agaveplatform.service.transfers.model.SqlQuery;
import org.agaveplatform.service.transfers.model.TransferUpdate;
import org.agaveplatform.service.transfers.util.AgaveSchemaFactory;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.agaveplatform.service.transfers.util.TransferRateHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NoSuchElementException;

import static org.agaveplatform.service.transfers.util.ActionHelper.*;
import static org.agaveplatform.service.transfers.util.AuthHelper.authenticate;

/**
 * This is the user-facing vertical providing the http interface to internal services.
 * It provides crud functionality and basic jwt authorization.
 */
public class TransferServiceVertical extends AbstractVerticle {

    private static Logger log = LoggerFactory.getLogger(TransferServiceVertical.class);

    private HttpServer server;
    private JDBCClient jdbc;
    private JWTAuth authProvider;
    private JsonObject config;

    @Override
    public void start(Future<Void> fut) {
        // set the config from the main vertical
        setConfig(config());

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
        if (config.getBoolean("JWT_AUTH")) {
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


        // init our db connection from the pool
        jdbc = JDBCClient.createShared(vertx, config, "agave_io");
        connect()
                .compose(connection -> {
                    Future<Void> future = Future.future();
                    createTableIfNeeded(connection)
                            .compose(this::createSomeDataIfNone)
                            .setHandler(x -> {
                                connection.close();
                                future.handle(x.mapEmpty());
                            });
                    return future;
                })
                .compose(v -> createHttpServer(config, router))
                .setHandler(fut);
    }

    private Future<Void> createHttpServer(JsonObject config, Router router) {
        Future<Void> future = Future.future();
        int port = config.getInteger("HTTP_PORT", 8085);

        vertx
            .createHttpServer()
            .requestHandler(router::accept)
            .listen(
                port,
                res -> future.handle(res.mapEmpty())
            );

        return future;
    }

    private Future<SQLConnection> connect() {
        Future<SQLConnection> future = Future.future();
        jdbc.getConnection(ar ->
                future.handle(ar.map(c ->
                                c.setOptions(new SQLOptions().setAutoGeneratedKeys(true))
                        )
                )
        );
        return future;
    }

    private Future<TransferTask> insert(SQLConnection connection, TransferTask transferTask, boolean closeConnection) {
        Future<TransferTask> future = Future.future();
        String sql = "INSERT INTO TransferTasks " +
                "(\"attempts\", \"bytes_transferred\", \"created\", \"dest\", \"end_time\", \"event_id\", \"last_updated\", \"owner\", \"source\", \"start_time\", \"status\", \"tenant_id\", \"total_size\", \"transfer_rate\", \"parent_task\", \"root_task\", \"uuid\", \"total_files\", \"total_skipped\") " +
                "VALUES " +
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
        connection.updateWithParams(sql,
                new JsonArray()
                        .add(transferTask.getAttempts())
                        .add(transferTask.getBytesTransferred())
                        .add(transferTask.getCreated())
                        .add(transferTask.getDest())
                        .add(transferTask.getEndTime())
                        .add(transferTask.getEventId())
                        .add(transferTask.getLastUpdated())
                        .add(transferTask.getOwner())
                        .add(transferTask.getSource())
                        .add(transferTask.getStartTime())
                        .add(transferTask.getStatus())
                        .add(transferTask.getTenantId())
                        .add(transferTask.getTotalSize())
                        .add(transferTask.getTransferRate())
                        .add(transferTask.getParentTaskId())
                        .add(transferTask.getRootTaskId())
                        .add(transferTask.getUuid())
                        .add(transferTask.getTotalFiles())
                        .add(transferTask.getTotalSkippedFiles()),
                ar -> {
                    if (closeConnection) {
                        connection.close();
                    }
                    future.handle(
                            ar.map(res -> {
                                TransferTask t = new TransferTask(transferTask.getSource(), transferTask.getDest());
                                t.setId(res.getKeys().getLong(0));
                                return t;
                            })
                    );
                }
        );
        return future;
    }

    private Future<List<TransferTask>> query(SQLConnection connection) {
        Future<List<TransferTask>> future = Future.future();
        connection.query("SELECT * FROM transfertasks", result -> {
                    connection.close();
                    future.handle(
                            result.map(rs -> rs.getRows().stream().map(TransferTask::new).collect(Collectors.toList()))
                    );
                }
        );
        return future;
    }

    private Future<TransferTask> queryOne(SQLConnection connection, String uuid) {
        Future<TransferTask> future = Future.future();

        connection.queryWithParams(SqlQuery.GET_ONE, new JsonArray().add(uuid), result -> {
            connection.close();
            future.handle(
                    result.map(rs -> {
                        List<JsonObject> rows = rs.getRows();
                        if (rows.size() == 0) {
                            throw new NoSuchElementException("No transferTask with id " + uuid);
                        } else {
                            JsonObject row = rows.get(0);
                            return new TransferTask(row);
                        }
                    })
            );
        });
        return future;
    }

    private Future<TransferTask> update(SQLConnection connection, String uuid, TransferUpdate transferUpdate) {
        Future<TransferTask> future = Future.future();
        String sql = "SELECT * FROM transfertasks WHERE \"uuid\" = ?";
        connection.queryWithParams(sql, new JsonArray().add(uuid), result -> {
//            connection.close();
            List<JsonObject> rows = result.result().getRows();
            if (rows.size() == 0) {
                throw new NoSuchElementException("No transferTask with id " + uuid);
            } else {
                JsonObject row = rows.get(0);
                final TransferTask transferTask =
                        TransferRateHelper.updateSummaryStats(new TransferTask(row), transferUpdate);

                String updateSql = "UPDATE transfertasks " +
                        "SET \"attempts\" = ?, " +
                        "    \"bytes_transferred\" = ?, " +
                        "    \"end_time\" = ?, " +
                        "    \"start_time\" = ?, " +
                        "    \"status\" = ?, " +
                        "    \"tenant_id\" = ?, " +
                        "    \"total_size\" = ?, " +
                        "    \"transfer_rate\" = ?, " +
                        "    \"total_files\" = ? " +
                        "    \"total_skipped\" = ? " +
                        "WHERE \"uuid\" = ?";

                connection.updateWithParams(updateSql, new JsonArray()
                                .add(transferTask.getAttempts())
                                .add(transferTask.getBytesTransferred())
                                .add(transferTask.getEndTime())
                                .add(transferTask.getOwner())
                                .add(transferTask.getStartTime())
                                .add(transferTask.getStatus())
                                .add(transferTask.getTenantId())
                                .add(transferTask.getTotalSize())
                                .add(transferTask.getTransferRate())
                                .add(transferTask.getTotalFiles())
                                .add(transferTask.getTotalSkippedFiles())
                                .add(uuid),
                        ar -> {
                            connection.close();
                            if (ar.failed()) {
                                future.fail(ar.cause());
                            } else {
                                UpdateResult ur = ar.result();
                                if (ur.getUpdated() == 0) {
                                    future.fail(new NoSuchElementException("No transferTask with id " + uuid));
                                } else {
                                    future.complete(transferTask);
                                }
                            }
                        });
            }

        });

        return future;
    }

    private Future<Void> delete(SQLConnection connection, String uuid) {
        Future<Void> future = Future.future();
        String sql = "DELETE FROM transfertasks WHERE \"uuid\" = ?";
        connection.updateWithParams(sql,
                new JsonArray().add(Integer.valueOf(uuid)),
                ar -> {
                    connection.close();
                    if (ar.failed()) {
                        future.fail(ar.cause());
                    } else {
                        if (ar.result().getUpdated() == 0) {
                            future.fail(new NoSuchElementException("Unknown transfer task " + uuid));
                        } else {
                            future.complete();
                        }
                    }
                }
        );
        return future;
    }

    /**
     * Runs initial migration on the db
     * @param connection the active db connection
     * @return empty future for the sqlconnection
     */
    private Future<SQLConnection> createTableIfNeeded(SQLConnection connection) {
        Future<SQLConnection> future = Future.future();
        vertx.fileSystem().readFile("tables.sql", ar -> {
            if (ar.failed()) {
                future.fail(ar.cause());
            } else {
                connection.execute(ar.result().toString(),
                        ar2 -> future.handle(ar2.map(connection))
                );
            }
        });
        return future;
    }

    /**
     * Populates the db with some sample data
     * @param connection the active db connection
     * @return empty future for the sqlconnection
     */
    private Future<SQLConnection> createSomeDataIfNone(SQLConnection connection) {
        Future<SQLConnection> future = Future.future();
        connection.query("SELECT * FROM transfertasks", select -> {
            if (select.failed()) {
                future.fail(select.cause());
            } else {
                if (select.result().getResults().isEmpty()) {
                    TransferTask transferTask1= new TransferTask("agave://sftp//etc/hosts", "agave://sftp//tmp/hosts1", "testuser", null, null);
                    TransferTask transferTask2 = new TransferTask("agave://sftp//etc/hosts", "agave://sftp//tmp/hosts2", "testuser", null, null);
                    TransferTask transferTask3 = new TransferTask("agave://sftp//etc/hosts", "agave://sftp//tmp/hosts3", "testuser", null, null);
                    Future<TransferTask> insertion1 = insert(connection, transferTask1, false);
                    Future<TransferTask> insertion2 = insert(connection, transferTask2, false);
                    Future<TransferTask> insertion3 = insert(connection, transferTask3, false);
                    CompositeFuture.all(insertion1, insertion2)
                            .setHandler(r -> future.handle(r.map(connection)));
                } else {
                    future.complete(connection);
                }
            }
        });
        return future;
    }

    // ---- HTTP Actions ----

    /**
     * Fetches all {@link TransferTask} from the db. Results are added to the routing context.
     * TODO: add pagination and querying.
     *
     * @param routingContext the current rounting context for the request
     */
    private void getAll(RoutingContext routingContext) {
        connect()
                .compose(this::query)
                .setHandler(ok(routingContext));
    }

    /**
     * Inserts a new {@link TransferTask} into the db. Validation happens in a previous handler,
     * so this method only needs to worry about running the insertion.
     *
     * @param routingContext the current rounting context for the request
     */
    private void addOne(RoutingContext routingContext) {
        TransferTask transferTask = routingContext.getBodyAsJson().mapTo(TransferTask.class);
        connect()
                .compose(connection -> insert(connection, transferTask, true))
                .setHandler(tt -> {
                    if (tt.succeeded()) {
                        vertx.eventBus().publish("transfertask.created", tt.result().toJSON());
                    }
                })
                .setHandler(created(routingContext));
    }

    /**
     * Delete a {@link TransferTask} from the db.
     *
     * @param routingContext the current rounting context for the request
     */
    private void deleteOne(RoutingContext routingContext) {

        String uuid = routingContext.pathParam("uuid");
        connect()
                .compose(connection -> delete(connection, uuid))
                .setHandler(noContent(routingContext));
    }

    /**
     * Fetches a single {@link TransferTask} from the db.
     *
     * @param routingContext the current rounting context for the request
     */
    private void getOne(RoutingContext routingContext) {
        String uuid = routingContext.pathParam("uuid");
        connect()
                .compose(connection -> queryOne(connection, uuid))
                .setHandler(ok(routingContext));
    }

    /**
     * Update an existing {@link TransferTask}. Validation is performed in a prior handler, so this
     * method needs only to update the db.
     *
     * @param routingContext the current rounting context for the request
     */
    private void updateOne(RoutingContext routingContext) {
        String uuid = routingContext.request().getParam("uuid");
        TransferUpdate transferUpdate = routingContext.getBodyAsJson().mapTo(TransferUpdate.class);

        connect()
            .compose(connection ->
                queryOne(connection, uuid)
                    .compose(transferTask -> {
                        TransferTask tt = TransferRateHelper.updateSummaryStats(transferTask, transferUpdate);
                        return update(connection, uuid, transferUpdate);
                    })

            )
            .setHandler(ok(routingContext));
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
}
