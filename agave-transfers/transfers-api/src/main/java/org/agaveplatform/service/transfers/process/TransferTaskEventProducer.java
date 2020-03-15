package org.agaveplatform.service.transfers.process;

import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import org.agaveplatform.service.transfers.model.TransferTaskEvent;
import org.iplantc.service.transfer.model.enumerations.TransferTaskEventType;

public class TransferTaskEventProducer {

    /**
     * Adds an event to the history of this transfer task. This will automatically
     * be saved with the transfer task when the transfer task is persisted.
     *
     * @param event
     */
    public void addEvent(TransferTaskEvent event) {
        // broadcast event to channel
        EventBus eb = Vertx.vertx().eventBus();
        eb.publish(event.getStatus(), event);
    }
}
