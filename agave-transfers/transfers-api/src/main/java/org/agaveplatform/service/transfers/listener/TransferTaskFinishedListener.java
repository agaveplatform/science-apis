package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.reactivex.ext.unit.Async;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;

public class TransferTaskFinishedListener extends AbstractTransferTaskListener {
    private final static Logger logger = LoggerFactory.getLogger(TransferTaskFinishedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_FINISHED;

    private TransferTaskDatabaseService dbService;
    protected List<String> parentList = new ArrayList();

    public TransferTaskFinishedListener() {
        super();
    }

    public TransferTaskFinishedListener(Vertx vertx) {
        setVertx(vertx);
    }

    public TransferTaskFinishedListener(Vertx vertx, String eventChannel) {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() {
        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            msg.reply(TransferTaskFinishedListener.class.getName() + " received.");

            JsonObject body = msg.body();
            this.processBody(body, processBodyResult -> {
                if (processBodyResult.succeeded()) {
                    String uuid = body.getString("uuid");

                    logger.info("Transfer task {} finished.");

                    this.processEvent(body, result -> {
                        if (result.succeeded()) {
                            logger.trace("Succeeded with the processing the transfer finished event for transfer task {}", uuid);
                            msg.reply(TransferTaskFinishedListener.class.getName() + " completed.");
                        } else {
                            logger.error("Error with return from update event {}", uuid);
                            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
                        }
                    });
                } else {
                    logger.error("Error with retrieving Transfer Task {}", body.getString("id"));
                    _doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);
                }
            });

        });
    }

    public void processEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        String tenantId = body.getString("tenant_id");
        String uuid = body.getString("uuid");
        String status = body.getString("status");
        String parentTaskId = body.getString("parentTask");
        String rootTaskId = body.getString("rootTask");
        String eventId = body.getString("eventId");
//        body.put("type", status);
//        body.put("event", eventId);
        logger.debug("Processing finished task, sending notification");

        try {
            logger.debug("Sending notification event");
//          _doPublishEvent(MessageType.TRANSFERTASK_NOTIFICATION, body);
            handler.handle(Future.succeededFuture(true));
        } catch (Exception e) {
            logger.debug(TransferTaskFinishedListener.class.getName() + " - exception caught");
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
