package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.TRANSFERTASK_MAX_ATTEMPTS;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_ERROR;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFER_FAILED;

public abstract class AbstractTransferTaskListener extends AbstractVerticle {
    private static final Logger logger = LoggerFactory.getLogger(AbstractTransferTaskListener.class);

    String eventChannel;
    final public ConcurrentHashSet<String> cancelledTasks = new ConcurrentHashSet<>();
    final public ConcurrentHashSet<String> pausedTasks = new ConcurrentHashSet<>();
    private RetryRequestManager retryRequestManager;


    public AbstractTransferTaskListener() {
        super();
    }

    public AbstractTransferTaskListener(Vertx vertx) {
        this(vertx, null);
    }
    public AbstractTransferTaskListener(Vertx vertx, String eventChannel) {
        this();
        setVertx(vertx);
        setEventChannel(eventChannel);
    }

    public ConcurrentHashSet<String> getCancelledTasks() {return cancelledTasks;}
    public ConcurrentHashSet<String> getPausedTasks() {return pausedTasks;}

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

    /**
     * Handles event creation and delivery across the existing event bus. Retry is handled by the
     * {@link RetryRequestManager} up to 3 times. The call will be made asynchronously, so this method
     * will return immediately.
     *
     * @param eventName the name of the event. This doubles as the address in the request invocation.
     * @param body the message of the body. Currently only {@link JsonObject} are supported.
     */
    public void _doPublishEvent(String eventName, JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        logger.info(super.getClass().getName() + ": _doPublishEvent({}, {})", eventName, body);
        try {
            getRetryRequestManager().request(eventName, body, config().getInteger(TRANSFERTASK_MAX_ATTEMPTS, 0));
            if (handler != null) {
                handler.handle(Future.succeededFuture(true));
            }
        } catch (Exception e) {
            logger.error("Error with _doPublishEvent:  {}", e.getMessage());
            if (handler != null) {
                handler.handle(Future.failedFuture(e));
            }
        }
    }

    /**
     * Convenience method to handles generation of failed transfer messages, raising of failed event, and calling of handler with the
     * passed exception.
     * @param throwable the exception that was thrown
     * @param failureMessage the human readable message to send back
     * @param originalMessageBody the body of the original message that caused that failed
     * @param handler the callback to pass a {@link Future#failedFuture(Throwable)} with the {@code throwable}
     */
    protected void doHandleFailure(Throwable throwable, String failureMessage, JsonObject originalMessageBody, Handler<AsyncResult<Boolean>> handler) {
        JsonObject json = new JsonObject()
                .put("cause", throwable.getClass().getName())
                .put("message", failureMessage)
                .mergeIn(originalMessageBody);

        _doPublishEvent(TRANSFER_FAILED, json, handler);
    }

    /**
     * Convenience method to handles generation of errored out transfer messages, raising of error event, and calling of handler with the
     * passed exception.
     * @param throwable the exception that was thrown
     * @param failureMessage the human readable message to send back
     * @param originalMessageBody the body of the original message that caused that failed
     * @param handler the callback to pass a {@link Future#failedFuture(Throwable)} with the {@code throwable}
     */
    protected void doHandleError(Throwable throwable, String failureMessage, JsonObject originalMessageBody, Handler<AsyncResult<Boolean>> handler) {
        JsonObject json = new JsonObject()
                .put("cause", throwable.getClass().getName())
                .put("message", failureMessage)
                .mergeIn(originalMessageBody);

        _doPublishEvent(TRANSFERTASK_ERROR, json, handler);
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
     * Pushes a new {@link TransferTask} uuid onto the threadsafe set of cancelled tasks
     * @param uuid the cancelled task uuid
     */
    public synchronized void addCancelledTask(String uuid) {
        getCancelledTasks().add(uuid);
        getCancelledTasks().contains(uuid);
    }

    /**
     * Removes a {@link TransferTask} uuid from a threadsafe set of cancelled tasks.
     * @param uuid the cancelled task uuid
     * @return true if the uuid existed and was removed, false otherwise
     */
    public synchronized boolean removeCancelledTask(String uuid) {
        return getCancelledTasks().remove(uuid);
    }

    /**
     * Pushes a new {@link TransferTask} uuid onto the threadsafe set of cancelled tasks
     * @param uuid the cancelled task uuid
     */
    public synchronized void addPausedTask(String uuid) {
        getPausedTasks().add(uuid);
    }

    /**
    * Check for the existence of a uuid in the paused task
     * @param uuid then
    */
    public synchronized boolean checkPausedTask(String uuid) {
       return getPausedTasks().contains(uuid);
    }

    /**
     * Removes a {@link TransferTask} uuid from a threadsafe set of cancelled tasks.
     * @param uuid the cancelled task uuid
     * @return true if the uuid existed and was removed, false otherwise
     */
    public synchronized boolean removePausedTask(String uuid) {
        return getPausedTasks().remove(uuid);
    }


//    /**
//     * Sets the state of the process interupt flag
//     * @param state string value "add" or "remove"
//     * @param body Json object
//     */
//    public void processInterrupt(String state, JsonObject body) {
//        try {
//            String uuid = body.getString("uuid");
//            if (state.equalsIgnoreCase("add")) {
//                interruptedTasks.add(uuid);
//            } else if (state.equalsIgnoreCase("remove")){
//                interruptedTasks.remove(uuid);
//            }
//        } catch (Exception e){
//            logger.error(e.getMessage());
//        }
//    }

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
     * children exist in the lists of paused and cancelled tasks.
     *
     * @param transferTask the current task being checked from the running task
     * @return false if the transfertask's uuid, parentTaskId, or rootTaskId are in the {@link #cancelledTasks} or
     *          {@link #pausedTasks} list.
     */
    public boolean taskIsNotInterrupted(TransferTask transferTask) {
        if (transferTask.getParentTaskId() != null && transferTask.getRootTaskId() != null) {
            final List<String> uuids = List.of(transferTask.getUuid(), transferTask.getParentTaskId(), transferTask.getRootTaskId());
            return getCancelledTasks().stream().noneMatch(uuids::contains) && getPausedTasks().stream().noneMatch(uuids::contains);
        }
        return true;
    }

    /**
     * Publishes a {@link MessageType#TRANSFERTASK_CANCELED} message for the given transfer task. This was refactored
     * from the {@link #taskIsNotInterrupted(TransferTask)} method to allow for quick checks up front, followed by
     * async propagation of the cancelled event, which was blocking.
     * @param transferTask the task to report cancelled
     * @param handler the handler to call upon completion.
     */
    protected void publishTaskInterruptedEvent(TransferTask transferTask, Handler<AsyncResult<Boolean>> handler) {
        String msg = "Transfer was Canceled or Paused";
        logger.info("Transfer task {} interrupted due to cancel event", transferTask.getUuid());
        JsonObject json = new JsonObject()
                .put("message", msg);
        _doPublishEvent(MessageType.TRANSFERTASK_CANCELED, json, resp -> {
            if (resp.succeeded()) {
                if (handler != null) {
                    handler.handle(Future.succeededFuture(true));
                }
            } else {
                logger.error("taskIsNotInterrupted Error.  {}", resp.cause().getMessage());
                if (handler != null) {
                    // why do we swallow this? We just failed to propagate the transfer error.
                    handler.handle(Future.failedFuture(resp.cause()));
                }
            }
        });
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
     * @param eventChannel the default channel this vertical listens to
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

    public RetryRequestManager getRetryRequestManager() throws IOException, InterruptedException {
        logger.trace("Got into the getRetryRequestManager call");
        if (retryRequestManager == null) {
            logger.info("getRetryRequestManager check for null");
            retryRequestManager = new RetryRequestManager(getVertx());
        }
        return retryRequestManager;
    }

    public void setRetryRequestManager(RetryRequestManager retryRequestManager) {
        this.retryRequestManager = retryRequestManager;
    }

    /**
     * Checks for a supported schema in the URI.
     * @param uri the uri to check
     * @return true if supported, false otherwise
     * @see RemoteDataClientFactory#isSchemeSupported(URI);
     */
    protected boolean uriSchemeIsNotSupported(URI uri) {
        return ! RemoteDataClientFactory.isSchemeSupported(uri);
    }
}
