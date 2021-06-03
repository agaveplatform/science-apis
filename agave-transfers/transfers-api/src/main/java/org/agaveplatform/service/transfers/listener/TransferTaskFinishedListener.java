package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Options;
import io.nats.client.Subscription;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.TransferTaskConfigProperties;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;

public class TransferTaskFinishedListener extends AbstractNatsListener {
    private final static Logger log = LoggerFactory.getLogger(TransferTaskFinishedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_FINISHED;

    private TransferTaskDatabaseService dbService;
    protected List<String> parentList = new ArrayList();
    public Connection nc;

    public TransferTaskFinishedListener() throws IOException, InterruptedException {
        super();
        setConnection();
    }

    public TransferTaskFinishedListener(Vertx vertx) throws IOException, InterruptedException {
        super();
        setVertx(vertx);
        setConnection();
    }

    public TransferTaskFinishedListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
        super(vertx, eventChannel);
        setConnection();
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    public Connection getConnection(){return nc;}

    public void setConnection() throws IOException, InterruptedException {
        try {
            nc = _connect(config().getString(TransferTaskConfigProperties.NATS_URL));
        } catch (IOException e) {
            //use default URL
            nc = _connect(Options.DEFAULT_URL);
        }
    }

    @Override
    public void start() throws IOException, InterruptedException, TimeoutException {
        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        Dispatcher d = getConnection().createDispatcher((msg) -> {});
        //bus.<JsonObject>consumer(getEventChannel(), msg -> {
        Subscription s = d.subscribe(MessageType.TRANSFERTASK_FINISHED, msg -> {
        //msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
        String response = new String(msg.getData(), StandardCharsets.UTF_8);
        JsonObject body = new JsonObject(response) ;

            this.processBody(body, processBodyResult -> {
                if (processBodyResult.succeeded()) {
                    String uuid = body.getString("uuid");

                    log.info("Transfer task finished: {}", body);

                    try {
                        this.processEvent(body, result -> {
                            if (result.succeeded()) {
                                log.trace("Succeeded with the processing the transfer finished event for transfer task {}", uuid);
                                //msg.reply(TransferTaskFinishedListener.class.getName() + " completed.");
                            } else {
                                log.error("Error with return from update event {}", uuid);
                                try {
                                    _doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
                                } catch (Exception e) {
                                    log.debug(e.getMessage());
                                }
                            }
                        });
                    } catch (IOException | InterruptedException e) {
                        log.debug(e.getMessage());
                    }
                } else {
                    log.debug("Error with retrieving Transfer Task {}", body.getString("id"));
                    try {
                        _doPublishEvent( MessageType.TRANSFERTASK_ERROR, body);
                    } catch (Exception e) {
                        log.debug(e.getMessage());
                    }
                }
            });
        });
        d.subscribe(MessageType.TRANSFERTASK_FINISHED);
        getConnection().flush(Duration.ofMillis(500));

    }

    public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException {
        log.debug("Processing finished task, sending notification");

        try {
            log.debug("Sending notification event");
//          _doPublishEvent(MessageType.TRANSFERTASK_NOTIFICATION, body);
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
