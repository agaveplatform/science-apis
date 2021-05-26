package org.agaveplatform.service.transfers.messaging;

import io.nats.client.Connection;
import io.nats.client.ConnectionListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Collectors;

/**
 * Generic connection listener for NATS connections
 */
public class NatsConnectionListener implements ConnectionListener {
    private static final Logger log = LoggerFactory.getLogger(NatsConnectionListener.class);

    /**
     * Connection related events that occur asynchronously in the client code are
     * sent to a ConnectionListener via a single method. The ConnectionListener can
     * use the event type to decide what to do about the problem.
     *
     * @param conn the connection associated with the error
     * @param type the type of event that has occurred
     */
    @Override
    public void connectionEvent(Connection conn, io.nats.client.ConnectionListener.Events type) {
        if (log.isDebugEnabled()) {
            log.debug("NATS server event: " + type.toString());
            if (type == io.nats.client.ConnectionListener.Events.DISCOVERED_SERVERS) {
                log.debug("Known servers: " + conn.getServers().stream().collect(Collectors.joining(",")));
            }
        }
    }
}