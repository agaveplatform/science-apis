package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.agaveplatform.service.transfers.exception.InterruptableTransferTaskException;
import java.net.URI;

public class TransferTaskCreatedListener extends AbstractTransferTaskListener {
    private final Logger logger = LoggerFactory.getLogger(TransferTaskCreatedListener.class);

    public TransferTaskCreatedListener(){super();}
    public TransferTaskCreatedListener(Vertx vertx) {
        this(vertx, null);
    }

    public TransferTaskCreatedListener(Vertx vertx, String eventChannel) {
        super();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }

    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_CREATED;

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() {
        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            logger.info("Transfer task {} created: {} -> {}", uuid, source, dest);
            try {
                this.assignTransferTask(body);
            } catch (InterruptableTransferTaskException e) {
                e.printStackTrace();
            }
        });

        // cancel tasks
        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} cancel detected", uuid);
            //this.interruptedTasks.add(uuid);
            super.processInterrupt("add", body);

        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
            super.processInterrupt("remove", body);
            //this.interruptedTasks.remove(uuid);
        });

        // paused tasks
        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} paused detected", uuid);
            super.processInterrupt("add", body);
        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            logger.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
            super.processInterrupt("remove", body);
        });

    }

    public String assignTransferTask(JsonObject body) throws InterruptableTransferTaskException {
        String uuid = body.getString("uuid");
        String source = body.getString("source");
//		String dest =  body.getString("dest");
        String username = body.getString("owner");
        String tenantId = body.getString("tenantId");
        String protocol = "";
        TransferTask bodyTask = new TransferTask(body);

        try {
            URI srcUri;
            try {
                srcUri = URI.create(source);

                protocol = srcUri.getScheme();
            } catch (Exception e) {
                String msg = String.format("Unable to parse source uri %s for transfertask %s: %s",
											source, uuid, e.getMessage());
                throw new RemoteDataSyntaxException(msg, e);
            }

            if (RemoteDataClientFactory.isSchemeSupported(srcUri)) {

                if (srcUri.getScheme().equalsIgnoreCase("agave")) {
                    RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
                    protocol = srcSystem.getStorageConfig().getProtocol().toString();
                }
                if ( ! isTaskInterrupted(bodyTask)) {
                    logger.info("TransferTaskAssigned works. The value of the body = {}", body);
                    String assignmentChannel = MessageType.TRANSFERTASK_ASSIGNED;
                    _doPublishEvent(assignmentChannel, body);
                    return protocol;
                }
            } else {
                String msg = String.format("Unknown source schema %s for the transfertask %s",
											srcUri.getScheme(), uuid);
                //throw new RemoteDataSyntaxException(msg);
                logger.error(msg);
                JsonObject json = new JsonObject()
                        .put("cause", RemoteDataSyntaxException.class.getName())
                        .put("message", msg)
                        .mergeIn(body);
                _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
                return json.toString();
            }
        } catch (Exception e) {
            //logger.info(e.getMessage());
            JsonObject json = new JsonObject()
                    .put("cause", e.getClass().getName())
                    .put("message", e.getMessage())
                    .mergeIn(body);

            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
            return json.toString();
        } catch (Throwable t){
            //logger.info(e.getMessage());
            JsonObject json = new JsonObject()
                    .put("cause", t.getClass().getName())
                    .put("message", t.getMessage())
                    .mergeIn(body);
            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
            throw new InterruptableTransferTaskException(t.getMessage());
        }
        return null;
    }

}
