package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.globus.ftp.app.Transfer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_HEALTHCHECK_PARENT;
import static org.agaveplatform.service.transfers.enumerations.TransferStatusType.CANCELED_ERROR;

public class TransferTaskHealthcheckParentListener extends AbstractTransferTaskListener {
    private final static Logger logger = LoggerFactory.getLogger(TransferTaskHealthcheckParentListener.class);

    private TransferTaskDatabaseService dbService;
    protected List<String> parentList = new ArrayList<String>();

    protected static final String EVENT_CHANNEL = TRANSFERTASK_HEALTHCHECK_PARENT;

    public TransferTaskHealthcheckParentListener() {
        super();
    }

    public TransferTaskHealthcheckParentListener(Vertx vertx) {
        super(vertx);
    }

    public TransferTaskHealthcheckParentListener(Vertx vertx, String eventChannel) {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return this.EVENT_CHANNEL;
    }



    @Override
    public void start() {

        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        setDbService(TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue));

        // listen for healthcheck events to determine if a task is complete
        // before its transfertask_completed event was received.
        getVertx().eventBus().<JsonObject>consumer(TRANSFERTASK_HEALTHCHECK_PARENT, msg -> {
            msg.reply(TransferTaskHealthcheckParentListener.class.getName() + " received.");

            JsonObject body = msg.body();
            String id = body.getString("id");
            logger.info("Performing healthcheck parent on transfer tasks ");

            processChildrenActiveAndExceedTimeEvent(body, resp -> {
                if (resp.succeeded()) {
                    logger.info("Succeeded with the processing parent transfer created event for transfer task {}", id);
                } else {
                    logger.error("Error with return from processing parent transfer tasks. ");
                }
            });
        });
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

        getDbService().updateById(id, CANCELED_ERROR.name(), updateStatus -> {
            logger.trace("Got into getDBService.updateStatus(complete) ");
            if (updateStatus.succeeded()) {
                TransferTask transferTask = new TransferTask(updateStatus.result());
                logger.info("[{}] Transfer task {} updated to completed.", transferTask.getTenantId(), transferTask.getUuid());
                //parentList.remove(uuid);
                _doPublishEvent(MessageType.TRANSFERTASK_FINISHED, updateStatus.result());
                //promise.handle(Future.succeededFuture(Boolean.TRUE));
            } else {
                logger.error("[{}] Task completed, but unable to update status: {}",
                        id, updateStatus.cause());
//                            logger.error("[{}] Task {} completed, but unable to update status: {}",
//                                    tenantId, uuid, reply.cause());
                JsonObject json = new JsonObject()
                        .put("cause", updateStatus.cause().getClass().getName())
                        .put("message", updateStatus.cause().getMessage())
                        .mergeIn(body);
                _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                //promise.handle(Future.failedFuture(updateStatus.cause()));
            }
        });

        handler.handle(Future.succeededFuture(true));
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }


}
