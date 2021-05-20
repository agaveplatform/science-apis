package org.iplantc.service.io.manager;

import io.vertx.core.AbstractVerticle;
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
 *
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
        return COMPLETED_EVENT_CHANNEL;
    }
    public String getCompletedEventChannel() {
        return FAILED_EVENT_CHANNEL;
    }


    public void start(){
        EventBus bus = vertx.eventBus();

        bus.<JsonObject>consumer(getFailedEventChannel(), msg -> {
            logger.debug("Received failed notification from transfertask event");

            // Parse data
            JsonObject body = msg.body();
            String srcUrl = body.getString("src");
            String createdBy = body.getString("owner");
            String transferUuid = body.getString("uuid");

            // Retrieve current logical file
            LogicalFile file = LogicalFileDao.findBySourceUrl(srcUrl);
            if (file.getTransferUuid() == transferUuid) {
                // Update logical file
                LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_FAILED, createdBy);
            }

        });

        bus.<JsonObject>consumer(getCompletedEventChannel(), msg -> {
            logger.debug("Received completed notification from transfertask event");

            // Parse data
            JsonObject body = msg.body();
            String srcUrl = body.getString("src");
            String createdBy = body.getString("owner");
            String transferUuid = body.getString("uuid");

            // Retrieve current logical file
            LogicalFile file = LogicalFileDao.findBySourceUrl(srcUrl);
            if (file.getTransferUuid() == transferUuid) {
                // Update logical file
                LogicalFileDao.updateTransferStatus(file, StagingTaskStatus.STAGING_COMPLETED, createdBy);
            }
        });
    }
}
