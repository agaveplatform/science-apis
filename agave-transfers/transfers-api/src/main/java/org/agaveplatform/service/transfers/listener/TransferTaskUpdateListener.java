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

public class TransferTaskUpdateListener extends AbstractNatsListener {
    private final static Logger logger = LoggerFactory.getLogger(org.agaveplatform.service.transfers.listener.TransferTaskUpdateListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_UPDATED;

    private TransferTaskDatabaseService dbService;
    protected List<String> parentList = new ArrayList();
    public Connection nc;

    public TransferTaskUpdateListener() throws IOException, InterruptedException {
        super();
        setConnection();
    }

    public TransferTaskUpdateListener(Vertx vertx) throws IOException, InterruptedException {
        super();
        setVertx(vertx);
        setConnection();
    }

    public TransferTaskUpdateListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
        super(vertx, eventChannel);
        setConnection();
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    public Connection getConnection(){return nc;}

    public void setConnection() throws IOException, InterruptedException {
        try {
            nc = _connect(CONNECTION_URL);
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

        //EventBus bus = vertx.eventBus();
        //bus.<JsonObject>consumer(getEventChannel(), msg -> {
        //Connection nc = _connect();
        Dispatcher d = getConnection().createDispatcher((msg) -> {});
        //bus.<JsonObject>consumer(getEventChannel(), msg -> {
        Subscription s = d.subscribe(MessageType.TRANSFERTASK_UPDATED, "transfer-task-update-queue", msg -> {
            //msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");

            //msg.reply(TransferTaskUpdateListener.class.getName() + " received.");
            TransferTask tt = new TransferTask(body);

            logger.info("Transfer task {} updated: {} -> {}", uuid, source, dest);

            this.processEvent(body, result -> {
                if (result.succeeded()) {
                    logger.info("Succeeded with the processing transfer update event for transfer task {}", uuid);
                } else {
                    logger.error("Error with return from update event {}", uuid);
                    try {
                        _doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
                    } catch (IOException e) {
                        logger.debug(e.getMessage());
                    } catch (InterruptedException e) {
                        logger.debug(e.getMessage());
                    }
                }
            });
        });
        d.subscribe(MessageType.TRANSFERTASK_UPDATED);
        getConnection().flush(Duration.ofMillis(500));

    }

    public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        //TransferTask bodyTask = new TransferTask(body);
//        body.put("status", TRANSFERRING);
        String tenantId = body.getString("tenant_id");
        String uuid = body.getString("uuid");
        String status = body.getString("status");
        String parentTaskId = body.getString("parentTask");
        logger.debug("Updating status of transfer task {} to {}", uuid, status);

        try {
            logger.trace("handling updating in-flight transfer progress...");
            handler.handle(Future.succeededFuture(true));

            //TODO: cache transfer progress instead of directly updating the db for performance
            // writing update to db may occur after transfer is completed because of slow speeds
//            getDbService().updateStatus(tenantId, uuid, status, reply -> {
//                if (reply.succeeded()) {
//                    logger.debug("Transfer task {} status updated to {}", uuid, status);
//                    handler.handle(Future.succeededFuture(true));
//                } else {
//                    String msg = String.format("Failed to set status of transfer task %s to %s. error: %s",
//                            uuid, status, reply.cause().getMessage());
//                    doHandleError(reply.cause(), msg, body, handler);
//                }
//            });
        } catch (Exception ex) {
            try {
                doHandleError(ex, ex.getMessage(), body, handler);
            } catch (IOException e) {
                logger.debug(e.getMessage());
            } catch (InterruptedException e) {
                logger.debug(e.getMessage());
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
