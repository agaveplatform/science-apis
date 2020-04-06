package org.agaveplatform.service.transfers.listener;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.iplantc.service.systems.dao.SystemDao;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;

public class TransferTaskCreatedListener extends AbstractVerticle {
    private final Logger logger = LoggerFactory.getLogger(TransferTaskCreatedListener.class);
    private String eventChannel = "transfertask.created";

    public TransferTaskCreatedListener(Vertx vertx) {
        this(vertx, null);
    }

    public TransferTaskCreatedListener(Vertx vertx, String eventChannel) {
        super();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }

    @Override
    public void start() {
        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("id");
            String source = body.getString("source");
            String dest = body.getString("dest");
            logger.info("Transfer task {} created: {} -> {}", uuid, source, dest);
            this.assignTransferTask(body);
        });
    }

    protected String assignTransferTask(JsonObject body) {
        String uuid = body.getString("id");
        String source = body.getString("source");
//		String dest =  body.getString("dest");
        String username = body.getString("owner");
        String tenantId = body.getString("tenantId");
        String protocol = null;

        try {

            URI srcUri;
            try {
                srcUri = URI.create(source);
            } catch (Exception e) {
                String msg = String.format("Unable to parse source uri %s for transfertask %s: %s",
											source, uuid, e.getMessage());
                throw new RemoteDataSyntaxException(msg, e);
            }

            if (RemoteDataClientFactory.isSchemeSupported(srcUri)) {

                if (srcUri.getScheme().equalsIgnoreCase("agave")) {
                    RemoteSystem srcSystem = new SystemDao().findBySystemId(srcUri.getHost());
                    protocol = srcSystem.getStorageConfig().getProtocol().toString();
                } else {
                    protocol = srcUri.getScheme();
                }

                String assignmentChannel = "transfertask.assigned." +
                        tenantId +
                        "." + protocol +
                        "." + srcUri.getHost() +
                        "." + username;
                getVertx().eventBus().publish(assignmentChannel, body);

            } else {
                String msg = String.format("Unknown source schema %s for the transfertask %s",
											srcUri.getScheme(), uuid);
                //throw new RemoteDataSyntaxException(msg);
                logger.error(msg);
                JsonObject json = new JsonObject()
                        .put("cause", new RemoteDataSyntaxException(msg).getClass().getName())
                        .put("message", msg)
                        .mergeIn(body);
                getVertx().eventBus().publish("transfertask.error", json);
            }
        } catch (Exception e) {
            logger.error(e.getMessage());
            JsonObject json = new JsonObject()
                    .put("cause", e.getClass().getName())
                    .put("message", e.getMessage())
                    .mergeIn(body);

            getVertx().eventBus().publish("transfertask.error", json);
        }

        return protocol;
    }

    /**
     * Sets the vertx instance for this listener
     *
     * @param vertx the current instance of vertx
     */
    private void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * @return the message type to listen to
     */
    public String getEventChannel() {
        return eventChannel;
    }

    /**
     * Sets the message type for which to listen
     *
     * @param eventChannel
     */
    public void setEventChannel(String eventChannel) {
        this.eventChannel = eventChannel;
    }
}
