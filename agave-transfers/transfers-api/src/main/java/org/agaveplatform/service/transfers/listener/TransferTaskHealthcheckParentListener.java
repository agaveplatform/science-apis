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
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.common.messaging.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK_PARENT;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.CANCELED_ERROR;

/**
 * Listens for parent {@link TransferTask} {@link MessageType#TRANSFERTASK_HEALTHCHECK_PARENT} events and
 * deals with timeouts and empty directories that have not been updated to completed.
 * TODO: This does not currently doing what we want it to do. There is no way to detect empty directory vs file
 *      transfertasks via database query, so the {@link TransferTaskWatchListener#processParentEvent(Handler)}
 *      method would degrade to sending every active non-root transfer task to this listener. We need a better
 *      way to detect broken and timed out transfertasks and restore them. That is likely the job of our state
 *      machine, and not a zombie task. This should probably only detect transfer tasks that have been running
 *      far too long and try to pause and resume those if possible.
 */
@Deprecated
public class TransferTaskHealthcheckParentListener extends AbstractNatsListener {
    private final static Logger logger = LoggerFactory.getLogger(TransferTaskHealthcheckParentListener.class);

    private TransferTaskDatabaseService dbService;
    protected List<String> parentList = new ArrayList<String>();
    public Connection nc;
    protected static final String EVENT_CHANNEL = TRANSFERTASK_HEALTHCHECK_PARENT;

    public TransferTaskHealthcheckParentListener() throws IOException, InterruptedException {
        super();
    }

    public TransferTaskHealthcheckParentListener(Vertx vertx) throws IOException, InterruptedException {
        super(vertx);
    }

    public TransferTaskHealthcheckParentListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
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
        setDbService(TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue));

        try {
            //group subscription so each message only processed by this vertical type once
            subscribeToSubjectGroup(EVENT_CHANNEL, this::handleMessage);
        } catch (Exception e) {
            logger.error("TRANSFER_ALL - Exception {}", e.getMessage());
        }

//
//        // listen for healthcheck events to determine if a task is complete
//        // before its transfertask_completed event was received.
//        //getVertx().eventBus().<JsonObject>consumer(TRANSFERTASK_HEALTHCHECK_PARENT, msg -> {
//        //Connection nc = _connect();
//        Dispatcher d = getConnection().createDispatcher((msg) -> {});
//        //bus.<JsonObject>consumer(getEventChannel(), msg -> {
//        Subscription s = d.subscribe(EVENT_CHANNEL, msg -> {
//            //msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
//            String response = new String(msg.getData(), StandardCharsets.UTF_8);
//            JsonObject body = new JsonObject(response) ;
//            String uuid = body.getString("uuid");
//            String source = body.getString("source");
//            String dest = body.getString("dest");
//           // msg.reply(TransferTaskHealthcheckParentListener.class.getName() + " received.");
//
//            String id = body.getValue("id").toString();
//            logger.info("Performing healthcheck parent on transfer tasks ");
//
//            processChildrenActiveAndExceedTimeEvent(body, resp -> {
//                if (resp.succeeded()) {
//                    logger.info("Succeeded with the processing parent transfer created event for transfer task {}", id);
//                } else {
//                    logger.error("Error with return from processing parent transfer tasks. ");
//                }
//            });
//        });
//        d.subscribe(EVENT_CHANNEL);
//        getConnection().flush(Duration.ofMillis(500));

    }

    protected void handleMessage(Message message) {
        try {
            JsonObject body = new JsonObject(message.getMessage());
            String uuid = body.getString("uuid");
            getVertx().<Boolean>executeBlocking(
                promise -> {
                    processChildrenActiveAndExceedTimeEvent(body, repl -> {
                        if (repl.succeeded()) {
                            promise.complete(repl.result());
                        } else {
                            promise.fail(repl.cause());
                        }
                    });
                },
                resp -> {
                    if (resp.succeeded()) {
                        logger.debug("Finished processing health check for transfer task {}", uuid);
                    } else {
                        logger.debug("Failed  processing health check for transfer task {}", uuid);
                    }
                });
        } catch (DecodeException e) {
            logger.error("Unable to parse message {} body {}. {}", message.getId(), message.getMessage(), e.getMessage());
        } catch (Throwable t) {
            logger.error("Unknown exception processing message message {} body {}. {}", message.getId(), message.getMessage(), t.getMessage());
        }
    }

    /**
     * Set all active tasks that have exceeded the time event to {@link org.agaveplatform.service.transfers.enumerations.TransferStatusType#CANCELED_ERROR}
     * Use the ID value that was retrieved from the TransferTaskWatchListener
     *
     * @param body {@link JsonObject} containing the id of a parent task with active children
     * @param handler
     */
    public void processChildrenActiveAndExceedTimeEvent(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        String id = body.getLong("id").toString();

        logger.info("getDbService.getAllParentsCanceledOrCompleted result: {} , id:{}", body, id);

        TransferTask parentTransferTask = new TransferTask(body);
        if (parentTransferTask.getRootTaskId() == null && parentTransferTask.getParentTaskId() == null){
            //this is the root task - don't cancel yet
            logger.debug("Do not timeout the root task, individual transfers shouldn't take as long but the root task " +
                    "has to wait for all children to finish which may take longer");
            handler.handle(Future.succeededFuture(true));
        } else {
            getDbService().updateById(id, CANCELED_ERROR.name(), updateStatus -> {
                logger.trace("Got into getDBService.updateStatus(complete) ");
                if (updateStatus.succeeded()) {
                    JsonObject updatedJson = updateStatus.result();
                    logger.info("Transfer task {} updated to completed.", updatedJson.getString("uuid"));
                    //parentList.remove(uuid);
                    _doPublishEvent(MessageType.TRANSFERTASK_FINISHED, updatedJson, handler);
                } else {
                    logger.error("Task completed, but unable to update status: {}",
                            id, updateStatus.cause());
                    JsonObject json = new JsonObject()
                            .put("cause", updateStatus.cause().getClass().getName())
                            .put("message", updateStatus.cause().getMessage())
                            .mergeIn(body);
                    _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json, errorResp -> {
                        // TODO: why is this a positive response?3
                        handler.handle(Future.succeededFuture(true));
                    });
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
