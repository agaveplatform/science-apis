package org.iplantc.service.io.manager;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.iplantc.service.io.dao.LogicalFileDao;
import org.iplantc.service.io.model.LogicalFile;
import org.iplantc.service.io.model.enumerations.StagingTaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class to listen and handle events sent from agave-transfers
 * and update the corresponding LogicalFile accordingly
 */
public class FilesTransferListener extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(FilesTransferListener.class);
    protected static final String COMPLETED_EVENT_CHANNEL = "transfer.completed";
    protected static final String FAILED_EVENT_CHANNEL = "transfertask.failed";

    protected String eventChannel;

    public FilesTransferListener() { super(); }
//    public FilesTransferListener(Vertx vertx) {
//        super(vertx);
//    }
//    public FilesTransferListener(Vertx vertx, String eventChannel) {
//        super(vertx, eventChannel);
//    }

    public String getFailedEventChannel() {
        return FAILED_EVENT_CHANNEL;
    }

    public String getCompletedEventChannel() {
        return COMPLETED_EVENT_CHANNEL;
    }


    public void start() {
        EventBus bus = vertx.eventBus();

        bus.<JsonObject>consumer(getFailedEventChannel(), msg -> {
            logger.debug("Received failed notification from transfertask event");

            // Parse data
            JsonObject body = msg.body();
            String transferUuid = body.getString("uuid");

            processTransferNotification(StagingTaskStatus.STAGING_FAILED, msg.body(), result -> {
                if (result.failed()) {
                    logger.debug("Unable to update status for Logical file {}", transferUuid);
                }
                msg.reply(result.result());
            });

        });

        bus.<JsonObject>consumer(getCompletedEventChannel(), msg -> {
            logger.debug("Received completed notification from transfertask event");

            // Parse data
            JsonObject body = msg.body();
            String transferUuid = body.getString("uuid");

            processTransferNotification(StagingTaskStatus.STAGING_COMPLETED, msg.body(), result -> {
                if (result.failed()) {
                    logger.debug("Unable to update status for Logical file {}", transferUuid);
                }
                msg.reply(result.result());
            });
        });

    }

    public void processTransferNotification(StagingTaskStatus status, JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        // Parse data
        String srcUrl = body.getString("source");
        String createdBy = body.getString("owner");
        String transferUuid = body.getString("uuid");

        // Retrieve current logical file
        LogicalFile file = LogicalFileDao.findByTransferUuid(transferUuid);
        if (file != null) {
            try {
                // Update logical file
                LogicalFileDao.updateTransferStatus(file, status, createdBy);
                handler.handle(Future.succeededFuture(true));
            } catch (Exception e) {
                logger.debug("Unable to update transfer status");
                handler.handle(Future.failedFuture(e));
            }
        } else {
            //
            handler.handle(Future.failedFuture("No existing file found matching transfer " + transferUuid));
        }
    }
}
