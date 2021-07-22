package org.agaveplatform.service.transfers.handler;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.listener.AbstractNatsListener;
import org.iplantc.service.common.exceptions.MessagingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;

/**
 * Class is used to retry failed message deliveries. This is left over from the behavior of the {@link Vertx#eventBus()}
 * behavior. With the {@link org.agaveplatform.service.transfers.messaging.NatsJetstreamMessageClient}, retry delivery
 * is built into the client, so this is now just a shell wrapper that we will back out over time.
 */
public class RetryRequestManager extends AbstractNatsListener {
    private static final Logger log = LoggerFactory.getLogger(RetryRequestManager.class);
    private Vertx vertx;

    public RetryRequestManager() throws IOException, InterruptedException {
        super();
    }

    /**
     * Constructs a RetryRequest that will attempt to make a request to the event bus and, upon failure, retry the
     * messsage up to {@code maxRetries} times.
     * @param vertx instance of vertx
     */
    public RetryRequestManager(Vertx vertx) throws IOException, InterruptedException {
        super();
        log.info("RetryRequestManager starting");
        setVertx(vertx);
    }

    /**
     * Attempts to make a request to the event bus and, upon failure, retry the message up to {@code maxAttempts} times.
     * @param address the address to which the message will be sent
     * @param body the message to send
     * @param maxAttempts the maximum times to retry delivery of the message
     * @deprecated 
     * @see #request(String, JsonObject)
     */
    public void request(final String address, final JsonObject body, final int maxAttempts) throws IOException, InterruptedException {
       request(address, body);
    }

    /**
     * Attempts to make a request to the event bus. Retry is handled by the client connection
     * @param eventName the address to which the message will be sent
     * @param body the message to send
     */
    public void request(final String eventName, final JsonObject body) throws IOException, InterruptedException {
        try {
            URI target = null;
            String host = null;

            try { target = URI.create(body.getString("dest")); } catch (IllegalArgumentException ignored) {}

            if (target == null || target.getScheme().equalsIgnoreCase("file")) {
                try { target = URI.create(body.getString("source")); } catch (IllegalArgumentException ignored) {}
            }

            if (target == null || target.getScheme().equalsIgnoreCase("file")) {
                host = "internal";
            }
            else {
                host = target.getHost();
            }

            String subject = createPushMessageSubject(body.getString("tenant_id"),
                    body.getString("owner"), host, eventName);

            getMessageClient().push(subject, body.toString());
        }
        catch (MessagingException e){
            log.error("Unable to send {} event for transfer task {}. {}", eventName, body.getString("uuid"), e.getMessage());
        }
    }

    public Vertx getVertx() {
        return vertx;
    }

    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }
}
