package org.agaveplatform.service.transfers.handler;

import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_NOTIFICATION;

public class RetryRequestManager  {
    private static final Logger log = LoggerFactory.getLogger(RetryRequestManager.class);
    private Vertx vertx;

    /**
     * Constructs a RetryRequest that will attempt to make a request to the event bus and, upon failure, retry the
     * messsage up to {@code maxRetries} times.
     * @param vertx instance of vertx
     */
    public RetryRequestManager(Vertx vertx) {
        log.info("RetryRequestManager starting");
        setVertx(vertx);
    }

    /**
     * Attempts to make a request to the event bus and, upon failure, retry the message up to {@code maxAttempts} times.
     * @param address the address to which the message will be sent
     * @param body the message to send
     * @param maxAttempts the maximum times to retry delivery of the message
     */
    public void request(final String address, final JsonObject body, final int maxAttempts) {
        log.info("Got into the RetryRequestManager.request method.");
        getVertx().eventBus().request(address, body, new DeliveryOptions(), new Handler<AsyncResult<Message<JsonObject>>>() {
                private int attempts = 0;

                /**
                 * Something has happened, so handle it.
                 *
                 * @param event the event to handle
                 */
                @Override
                public void handle (AsyncResult < Message < JsonObject >> event) {
                    log.info("Got into the RetryReqestManager.handle method.");
                if (event.failed()) {
                    if (attempts < maxAttempts) {
                        log.error("Unable to send {} event for transfer task {} after {} attempts. No further attempts will be made.",
                                address, body.getString("uuid"), attempts);
                        attempts += 1;
                        getVertx().eventBus().request(address, body, new DeliveryOptions(), this);
                    } else {
                        log.error("Unable to send {} event for transfer task {} after {} attempts for message {}. \"{}.\" No further attempts will be made.",
                                address, body.getString("uuid"), attempts, body.encode(), event.cause().getMessage());
                    }
                } else {
                    log.debug("Successfully sent {} event for transfer task {}", event, body.getString("uuid"));
                    log.debug("Sending notification event for transfer task {} with status {}",
                            body.getString("uuid"), body.getString("status"));

                    // now send notification of the event...this kinda feels wrong, but we can evaluate later.
                    getVertx().eventBus().request(TRANSFERTASK_NOTIFICATION, body, reply -> {
                        if (reply.succeeded()) {
                            log.debug("Successfully sent {} event for transfer task {}", TRANSFERTASK_NOTIFICATION, body.getString("uuid"));
                        } else {
                            log.error("Unable to send {} event for transfer task {} after {} attempts for message {}. \"{}.\" No further attempts will be made.",
                                    TRANSFERTASK_NOTIFICATION, body.getString("uuid"), attempts, event.result().body().encode(), event.cause().getMessage());
                        }
                    });
                }
            }
        });
    }

    public Vertx getVertx() {
        return vertx;
    }

    public void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }
}
