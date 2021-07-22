package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_FINISHED;

public class TransferTaskFinishedListener extends AbstractNatsListener {
    private final static Logger log = LoggerFactory.getLogger(TransferTaskFinishedListener.class);
    protected static final String EVENT_CHANNEL = TRANSFERTASK_FINISHED;

    private TransferTaskDatabaseService dbService;
    protected List<String> parentList = new ArrayList();
    public Connection nc;

    public TransferTaskFinishedListener() throws IOException, InterruptedException {
        super();
    }

    public TransferTaskFinishedListener(Vertx vertx) throws IOException, InterruptedException {
        super();
        setVertx(vertx);
    }

    public TransferTaskFinishedListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

//    public Connection getConnection(){return nc;}

    @Override
    public void start() throws IOException, InterruptedException, TimeoutException {
        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        try {
            //group subscription so each message only processed by this vertical type once
            subscribeToSubjectGroup(EVENT_CHANNEL, this::handleMessage);
        } catch (Exception e) {
            log.error("TRANSFER_ALL - Exception {}", e.getMessage());
        }

//        Dispatcher d = getConnection().createDispatcher((msg) -> {});
//        //bus.<JsonObject>consumer(getEventChannel(), msg -> {
//        Subscription s = d.subscribe(TRANSFERTASK_FINISHED, msg -> {
//        //msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
//        String response = new String(msg.getData(), StandardCharsets.UTF_8);
//        JsonObject body = new JsonObject(response) ;
//
//            this.processBody(body, processBodyResult -> {
//                if (processBodyResult.succeeded()) {
//                    String uuid = body.getString("uuid");
//
//                    log.info("Transfer task finished: {}", body);
//
//                    try {
//                        this.processEvent(body, result -> {
//                            if (result.succeeded()) {
//                                log.trace("Succeeded with the processing the transfer finished event for transfer task {}", uuid);
//                                //msg.reply(TransferTaskFinishedListener.class.getName() + " completed.");
//                            } else {
//                                log.error("Error with return from update event {}", uuid);
//                                try {
//                                    _doPublishEvent(TRANSFERTASK_ERROR, body);
//                                } catch (Exception e) {
//                                    log.debug(e.getMessage());
//                                }
//                            }
//                        });
//                    } catch (IOException | InterruptedException e) {
//                        log.debug(e.getMessage());
//                    }
//                } else {
//                    log.debug("Error with retrieving Transfer Task {}", body.getString("id"));
//                    try {
//                        _doPublishEvent( TRANSFERTASK_ERROR, body);
//                    } catch (Exception e) {
//                        log.debug(e.getMessage());
//                    }
//                }
//            });
//        });
//        d.subscribe(TRANSFERTASK_FINISHED);
//        getConnection().flush(Duration.ofMillis(500));

    }

    protected void handleMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);
            getVertx().<Boolean>executeBlocking(
                promise -> {
                    try {
                        processEvent(body, repl -> {
                            if (repl.succeeded()) {
                                promise.complete(repl.result());
                            } else {
                                promise.fail(repl.cause());
                            }
                        });
                    } catch (IOException e) {
                        log.debug(e.getMessage());
                    } catch (InterruptedException e) {
                        log.debug(e.getMessage());
                    }
                },
                resp -> {
                    if (resp.succeeded()) {
                        log.debug("Finished processing health check for transfer task {}", uuid);
                    } else {
                        log.debug("Failed  processing health check for transfer task {}", uuid);
                    }
                });
        } catch (DecodeException e) {
            log.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            log.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }

    public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException {
        log.debug("Processing finished task, sending notification");

        try {
            log.debug("Sending notification event");
//          _doPublishEvent(TRANSFERTASK_NOTIFICATION, body);
            handler.handle(Future.succeededFuture(true));
        } catch (Exception e) {
            log.debug(TransferTaskFinishedListener.class.getName() + " - exception caught");
            doHandleError(e, e.getMessage(), body, handler);
        }
    }


    /**
     * Process {@code body} to handle both partial and complete {@link TransferTask} objects
     *
     * @param body {@link JsonObject} containing either an ID or {@link TransferTask} object
     * @param handler  the handler to resolve with {@link JsonObject} of a {@link TransferTask}
     */
    public void processBody(JsonObject body, Handler<AsyncResult<JsonObject>> handler) {
        TransferTask transfer = null;
        try {
            transfer = new TransferTask(body);
            handler.handle(Future.succeededFuture(transfer.toJson()));
        } catch (Exception e) {
            getDbService().getById(body.getString("id"), result -> {
                if (result.succeeded()) {
                    handler.handle(Future.succeededFuture(result.result()));
                } else {
                    handler.handle((Future.failedFuture(result.cause())));
                }
            });
        }
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }


}
