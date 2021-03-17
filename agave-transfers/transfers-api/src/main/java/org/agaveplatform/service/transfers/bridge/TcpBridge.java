package org.agaveplatform.service.transfers.bridge;

import io.vertx.core.Vertx;
import io.vertx.ext.bridge.BridgeOptions;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.eventbus.bridge.tcp.TcpEventBusBridge;
import org.agaveplatform.service.transfers.bridge.tcp.impl.TcpEventBusBridgeImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

/**
 *
 */
public class TcpBridge {
    private static final Logger log = LoggerFactory.getLogger(TcpEventBusBridgeImpl.class);

    public void setUp(Vertx vertx) {

        TcpEventBusBridge bridge = TcpEventBusBridge.create(
                vertx,
                new BridgeOptions()
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CREATED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_ASSIGNED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CANCELED_SYNC))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CANCELED_COMPLETED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CANCELED_ACK))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CANCELED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PAUSED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PAUSED_SYNC))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PAUSED_COMPLETED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PAUSED_ACK))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFER_COMPLETED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_FINISHED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_ERROR))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PARENT_ERROR))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_INTERUPTED))
                        .addInboundPermitted(new PermittedOptions().setAddress(NOTIFICATION))
                        .addInboundPermitted(new PermittedOptions().setAddress(NOTIFICATION_CANCELED))
                        .addInboundPermitted(new PermittedOptions().setAddress(NOTIFICATION_COMPLETED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_DB_QUEUE))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_DELETED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_DELETED_ACK))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_UPDATED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFER_STREAMING))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_HEALTHCHECK))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_HEALTHCHECK_PARENT))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFER_FAILED))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFER_RETRY))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFER_ALL))
                        .addInboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_NOTIFICATION))
                        .addInboundPermitted(new PermittedOptions().setAddress(UrlCopy))

                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CREATED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_ASSIGNED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CANCELED_SYNC))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CANCELED_COMPLETED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CANCELED_ACK))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_CANCELED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PAUSED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PAUSED_SYNC))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PAUSED_COMPLETED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PAUSED_ACK))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFER_COMPLETED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_FINISHED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_ERROR))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_PARENT_ERROR))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_INTERUPTED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(NOTIFICATION))
                        .addOutboundPermitted(new PermittedOptions().setAddress(NOTIFICATION_CANCELED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(NOTIFICATION_COMPLETED))

                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_DB_QUEUE))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_DELETED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_DELETED_ACK))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_UPDATED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFER_STREAMING))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_HEALTHCHECK))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_HEALTHCHECK_PARENT))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFER_FAILED))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFER_RETRY))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFER_ALL))
                        .addOutboundPermitted(new PermittedOptions().setAddress(TRANSFERTASK_NOTIFICATION))
                        .addOutboundPermitted(new PermittedOptions().setAddress(UrlCopy))
        );

        bridge.listen(7000, res -> {
            if (res.succeeded()) {
                log.info("Passed the listener");
            } else {
                log.debug("Failed to start the listener");
                log.debug(res.cause().toString());
            }
        });
    }
}