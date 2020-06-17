package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;

import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_NOTIFICATION;

public abstract class AbstractTransferTaskListener extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTransferTaskListener.class);

    String address;
    String eventChannel;
    public HashSet<String> interruptedTasks = new HashSet<String>();
    private Object InterruptableTransferTaskListener;

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

        getVertx().eventBus().send(TRANSFERTASK_NOTIFICATION, body);
    }

//    /**
//     * Wrapper method for all message processing verticles. This will catch and swallow all {@link Throwable} exceptions
//     * and ensure we log and have visibility into errors not caught by the standard processing task.
//     *
//     * @param messageHandler the message body handler
//     * @return a boolean future indicating success of the processing task
//     */
//    protected void processMessage(Future<Boolean> messageHandler) {
//        try {
//            messageHandler.setHandler(reply -> {
//                if (reply.succeeded()) {
//                    logger.info("Completed processing {} message for {}", getEventChannel());
//                } else {
//                    logger.error("Failed to process " + getEventChannel() + " message for " );
//                }
//            });
//        } catch(Throwable t) {
//            logger.error("Failed to process " + getEventChannel() + " message for " );
//        }
//    }
//
//    /**
//     * Concrete implementation of the message handler. We can
//     * @param body
//     * @return handler a f
//     */
//    protected abstract Future<Boolean> _doProcessMessage(JsonObject body);

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

//    /**
//     * Checks whether the transfer task or any of its children exist in the list of
//     * interrupted tasks.
//     *
//     * @param transferTask the current task being checked from the running task
//     * @return true if the transfertask's uuid, parentTaskId, or rootTaskId are in the {@link #checkTaskInterrupted(TransferTask)} list
//     */
//    public void checkTaskInterrupted2(TransferTask transferTask )throws InterruptableTransferTaskException {
//
//        if (this.interruptedTasks.contains(transferTask.getUuid()) ||
//                this.interruptedTasks.contains(transferTask.getParentTaskId()) ||
//                this.interruptedTasks.contains(transferTask.getRootTaskId())) {
//            String msg = "Transfer was Canceled or Paused";
//            logger.info("Transfer task {} interrupted due to cancel event", transferTask.getUuid());
//            JsonObject json = new JsonObject()
//                    .put("message", msg);
//            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
//
//            // tell everyone else that you killed this task
//            throw new InterruptableTransferTaskException(
//                    String.format("Transfer task %s interrupted due to cancel event", transferTask.getUuid()));
//        }
//    }

    /**
     * Checks whether the {@code transferTask} has been interrupted by looking for the transfer task or any of its
     * children exist in the list of interrupted tasks.
     *
     * @param transferTask the current task being checked from the running task
     * @return false if the transfertask's uuid, parentTaskId, or rootTaskId are in the {@link #interruptedTasks} list
     */
    public boolean taskIsNotInterrupted(TransferTask transferTask ) {

        if (this.interruptedTasks.contains(transferTask.getUuid()) ||
                this.interruptedTasks.contains(transferTask.getParentTaskId()) ||
                this.interruptedTasks.contains(transferTask.getRootTaskId())) {
            String msg = "Transfer was Canceled or Paused";
            logger.info("Transfer task {} interrupted due to cancel event", transferTask.getUuid());
            JsonObject json = new JsonObject()
                    .put("message", msg);
            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
            return true;
//            // tell everyone else that you killed this task
//            throw new InterruptableTransferTaskException(
//                    String.format("Transfer task %s interrupted due to cancel event", transferTask.getUuid()));
        }
        return true;
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
