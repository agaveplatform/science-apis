package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Optional;

public abstract class AbstractTransferTaskListener extends AbstractVerticle implements InterruptableTransferTaskListener {
    private final Logger logger = LoggerFactory.getLogger(AbstractTransferTaskListener.class);

    String address;
    String eventChannel;
    public HashSet<String> interruptedTasks = new HashSet<String>();

    public AbstractTransferTaskListener() {
        super();
    }

    public AbstractTransferTaskListener(Vertx vertx) {
        this(vertx, null);
    }

    public AbstractTransferTaskListener(Vertx vertx, String eventChannel) {
        super();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }

    /**
     * Overriding the parent with a null safe check for the verticle context
     * before continuing. This affords us the luxury of spying on this instance
     * with a mock call chain.
     *
     * @return the config if present, an empty JsonObject otherwise
     */
    @Override
    public JsonObject config() {
        if (this.context == null) {
            return new JsonObject();
        } else {
            return this.context.config();
        }
    }

    public void _doPublishEvent(String event, Object body) {
        logger.debug("Publishing {} event: {}", event, body);
        getVertx().eventBus().publish(event, body);
    }


    /**
     * Sets the state of the process interupt flag
     * @param state string value "add" or "remove"
     * @param body Json object
     */
    public void processInterrupt(String state, JsonObject body) {
        try {
            String uuid = body.getString("uuid");
            if (state.equalsIgnoreCase("add")) {
                interruptedTasks.add(uuid);
            } else if (state.equalsIgnoreCase("remove")){
                interruptedTasks.remove(uuid);
            }
        } catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    /**
     * Checks whether the transfer task or any of its children exist in the list of
     * interrupted tasks.
     *
     * @param transferTask the current task being checked from the running task
     * @return true if the transfertask's uuid, parentTaskId, or rootTaskId are in the {@link #isTaskInterrupted(TransferTask)} list
     */
    public boolean isTaskInterrupted( TransferTask transferTask ){
        return this.interruptedTasks.contains(transferTask.getUuid()) ||
                this.interruptedTasks.contains(transferTask.getParentTaskId()) ||
                this.interruptedTasks.contains(transferTask.getRootTaskId());
    }

    /**
     * Sets the vertx instance for this listener
     * @param vertx the current instance of vertx
     */
    protected void setVertx(Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * Sets the eventChannel on which to listen
     * @param eventChannel
     */
    public void setEventChannel(String eventChannel) {
        this.eventChannel = eventChannel;
    }

    /**
     * @return the message type to listen to
     */
    public String getEventChannel() {
        return eventChannel == null ? getDefaultEventChannel() : eventChannel;
    }

    public abstract String getDefaultEventChannel();

}
