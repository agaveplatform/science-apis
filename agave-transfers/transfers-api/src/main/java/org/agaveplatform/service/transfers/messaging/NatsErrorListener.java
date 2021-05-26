package org.agaveplatform.service.transfers.messaging;

import io.nats.client.Connection;
import io.nats.client.Consumer;
import io.nats.client.ErrorListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic error listener for NATS connections
 */
public class NatsErrorListener implements ErrorListener {
    private static final Logger log = LoggerFactory.getLogger(NatsErrorListener.class);

    @Override
    public void slowConsumerDetected(Connection conn, Consumer consumer) {
        log.info("NATS connection slow consumer detected");
    }
    @Override
    public void exceptionOccurred(Connection conn, Exception exp) {
        log.info("NATS connection exception occurred");
        log.debug(exp.getMessage());
    }
    @Override
    public void errorOccurred(Connection conn, String error) {
        log.info("NATS connection error occurred: " + error);
    }
}