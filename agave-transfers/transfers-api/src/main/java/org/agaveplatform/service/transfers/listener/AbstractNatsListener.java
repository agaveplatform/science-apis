package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Nats;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.TRANSFERTASK_MAX_ATTEMPTS;

public class AbstractNatsListener extends AbstractTransferTaskListener {
    private static final Logger log = LoggerFactory.getLogger(AbstractNatsListener.class);

    public AbstractNatsListener(Vertx vertx) {
        this(vertx, null);
    }
    public AbstractNatsListener(Vertx vertx, String eventChannel) {
        this();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }
    public AbstractNatsListener() {
            super();
        }
    @Override
    public String getDefaultEventChannel() {
        return null;
    }

    public void _doPublishNatsEvent(String eventName, JsonObject body) {
        log.info(super.getClass().getName() + ": _doPublishEvent({}, {})", eventName, body);
 //       getRetryRequestManager().request(eventName, body, config().getInteger(TRANSFERTASK_MAX_ATTEMPTS, 0));

        Connection nc = null;
        try {
            nc = _connect();
        } catch (IOException e) {
            log.debug(e.getMessage());
        } catch (InterruptedException e) {
            log.debug(e.getMessage());
        }
        log.debug("NATS connection made.");
        nc.publish(MessageType.TRANSFERTASK_ASSIGNED, body.toString().getBytes(StandardCharsets.UTF_8));
        log.debug("Message sent.");
    }

    @Override
    public void _doPublishEvent(String eventName, JsonObject body) {
        this._doPublishNatsEvent(eventName, body);
    }


    public Connection _connect() throws IOException, InterruptedException {
        Connection nc = Nats.connect();
        return nc;
    }

    public void _closeConnection(Connection nc){
        try {
            nc.close();
        } catch (InterruptedException e) {
            log.debug(e.getMessage());
        }
    }
}
