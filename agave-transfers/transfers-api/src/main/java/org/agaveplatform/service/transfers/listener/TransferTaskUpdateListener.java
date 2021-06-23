package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.iplantc.service.common.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_PAUSED;

public class TransferTaskUpdateListener extends AbstractNatsListener {
    private final static Logger logger = LoggerFactory.getLogger(TransferTaskUpdateListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_UPDATED;

    private TransferTaskDatabaseService dbService;
    protected List<String> parentList = new ArrayList();
    public Connection nc;

    public TransferTaskUpdateListener() throws IOException, InterruptedException {
        super();
    }

    public TransferTaskUpdateListener(Vertx vertx) throws IOException, InterruptedException {
        super();
        setVertx(vertx);
    }

    public TransferTaskUpdateListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
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
            logger.error("TRANSFER_ALL - Exception {}", e.getMessage());
        }

    }

    protected void handleMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            logger.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);
            getVertx().<Boolean>executeBlocking(
                    promise -> {
                        try {
                            processEvent(body, promise);
                        } catch (Exception e) {
                            logger.error(e.getCause().toString());
                        }
                    },
                    resp -> {
                        if (resp.succeeded()) {
                            logger.debug("Finished processing {} for transfer task {}", MessageType.TRANSFERTASK_UPDATED, uuid);
                        } else {
                            logger.debug("Failed  processing {} for transfer task {}", MessageType.TRANSFERTASK_UPDATED, uuid);
                        }
                    });
        } catch (DecodeException e) {
            logger.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            logger.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }

    public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        //TransferTask bodyTask = new TransferTask(body);
//        body.put("status", TRANSFERRING);
        String tenantId = body.getString("tenant_id");
        String uuid = body.getString("uuid");
        String status = body.getString("status");
        String parentTaskId = body.getString("parentTask");
        logger.debug("Updating status of transfer task {} to {}", uuid, status);


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
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }


}
