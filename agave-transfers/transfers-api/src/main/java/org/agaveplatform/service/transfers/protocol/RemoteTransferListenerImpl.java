package org.agaveplatform.service.transfers.protocol;

import io.vertx.core.Context;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.handler.RetryRequestManager;
import org.agaveplatform.service.transfers.listener.TransferTaskAssignedListener;
import org.iplantc.service.transfer.AbstractRemoteTransferListener;
import org.iplantc.service.transfer.RemoteTransferListener;
import org.iplantc.service.transfer.exceptions.TransferException;
import org.iplantc.service.transfer.model.TransferTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.TRANSFERTASK_UPDATED;

public class RemoteTransferListenerImpl extends AbstractRemoteTransferListener {
    private static final Logger log = LoggerFactory.getLogger(RemoteTransferListenerImpl.class);

    protected RetryRequestManager retryRequestManager;

    protected TransferTaskDatabaseService dbService;

    /**
     * Vertx reference of calling class used to locate the event bus to throw messages
     */
    protected final Vertx vertx;

    public RemoteTransferListenerImpl(TransferTask transferTask, RetryRequestManager retryRequestManager) {
        super(transferTask);
        this.retryRequestManager = retryRequestManager;
        this.vertx = retryRequestManager.getVertx();
    }

    public RemoteTransferListenerImpl(TransferTask transferTask, Vertx vertx, RetryRequestManager retryRequestManager) {
        super(transferTask);
        this.vertx = vertx;
        this.retryRequestManager = retryRequestManager;

        getVertx().eventBus().<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
            msg.reply(TransferTaskAssignedListener.class.getName() + " received.");

            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            log.info("Transfer task {} cancel detected", uuid);
            log.info("Child task {} cancel detected", transferTask.getUuid());
            org.agaveplatform.service.transfers.model.TransferTask transfer = (org.agaveplatform.service.transfers.model.TransferTask)transferTask;
            final List<String> uuids = List.of(transfer.getParentTaskId(), transfer.getRootTaskId());
            if (uuid != null && uuids.contains(uuid)) {
                    cancel();
            }
        });
    }

    /**
     * Returns a {@link RetryRequestManager} to ensure messages are retried before failure.
     * @return a retry request manager.
     */
    protected RetryRequestManager getRetryRequestManager() {
        return retryRequestManager;
    }

    /**
     * @param retryRequestManager the retryRequestManager to set
     */
    protected void setRetryRequestManager(RetryRequestManager retryRequestManager) {
        this.retryRequestManager = retryRequestManager;
    }

    /**
     * Handles event creation and delivery across the existing event bus. Retry is handled by the
     * {@link RetryRequestManager} up to 3 times. The call will be made asynchronously, so this method
     * will return immediately.
     *
     * @param eventName the name of the event. This doubles as the address in the request invocation.
     * @param body the message of the body. Currently only {@link JsonObject} are supported.
     */
    public void _doPublishEvent(String eventName, JsonObject body) throws IOException, InterruptedException {
        log.debug("_doPublishEvent({}, {})", eventName, body);
        getRetryRequestManager().request(eventName, body, 2);
    }

    /**
     * Publishes a {@link org.agaveplatform.service.transfers.enumerations.MessageType#TRANSFERTASK_UPDATED} event
     * for the given transfertask
     * @param transferTask
     */
    public synchronized void setTransferTask(TransferTask transferTask) {
        try {
            _doPublishEvent(TRANSFERTASK_UPDATED, ((org.agaveplatform.service.transfers.model.TransferTask)transferTask).toJson());
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        this.transferTask = transferTask;
    }

    /**
     * The instance of Vertx associated with this transfer
     * @return
     */
    public Vertx getVertx() {
        return vertx;
    }

    @Override
    public synchronized boolean isCancelled()
    {
        return hasChanged() ||
            (getTransferTask() != null &&
                    ((org.agaveplatform.service.transfers.model.TransferTask)getTransferTask()).getStatus().isCancelled());
    }

    /**
     * Creates a new child transfer task at the given source and destination paths with this listener's
     * {@link #transferTask} as the parent. This is called from within recursive operations in each
     * @link RemoteDataClient} class and allows child {@link TransferTask}s to be created independent of the
     * concrete implementation. In this manner we get portability between legacy and new transfer packages.
     *
     * note: This should not be called with the current transfer task event architecture as directory
     *       processing and thus child transfer task creation is handled prior to individual transfers
     *       being made.
     *
     * @param sourcePath the source of the child {@link TransferTask}
     * @param destPath the dest of the child {@link TransferTask}
     * @return the persisted {@link TransferTask}
     * @throws TransferException if the cild transfer task cannot be saved
     */
    public TransferTask createAndPersistChildTransferTask(String sourcePath, String destPath) throws TransferException {
        final org.agaveplatform.service.transfers.model.TransferTask parentTask =
                (org.agaveplatform.service.transfers.model.TransferTask) getTransferTask();
        final org.agaveplatform.service.transfers.model.TransferTask childTask =
                new org.agaveplatform.service.transfers.model.TransferTask(sourcePath,
                destPath,
                parentTask.getOwner(),
                parentTask.getRootTaskId(),
                parentTask.getParentTaskId());

        // Here we use a countdown latch in combination with an blocking future to implement what is effectively an
        // await allowing for us to call the async database service from a synchronous context.
        CountDownLatch latch = new CountDownLatch(1);
        final JsonObject childTaskObject  = new JsonObject();
        getVertx().<JsonObject>executeBlocking(fut -> {
            // save the child task if not already present. The avoids us doubling up on retries. This code
            // runs in a wrapped blocking thread. Nothing will proceed until
            getDbService().createOrUpdateChildTransferTask(parentTask.getTenantId(), childTask, childResult -> {
                // we pass back the result to the async handler represented as the second argument to the
                // execute blocking method.
                if (childResult.succeeded()) {
                    fut.complete(childResult.result());
                } else {
                    fut.fail(childResult.cause());
                }
            });
        }, r -> {
            if (r.succeeded()) {
                // copy the saved JsonObject fields to the final childTaskObject so we can return it to the
                // client
                r.result().stream().forEach(entry -> { childTaskObject.put(entry.getKey(), entry.getValue()); });
            } else {
                childTaskObject.put("error", r.cause());
            }
            // decrementing this frees up the await statement
            latch.countDown();
        });

        try {
            // this blocks until the latch counts down to zero. At that point the childTaskObject either has
            // the marshalled saved child transfer task or a Throwable representing the error from the db service
            latch.await();
        } catch (InterruptedException e) {
            throw new TransferException("Failed to persist child transfer task.");
        }

        // if the db call failed, the response will have an error that we will throw back to the calling method
        if (childTaskObject.containsKey("error")) {
            throw new TransferException("Failed to persist child transfer task.", ((Throwable)childTaskObject.getValue("error")));
        } else {
            // otherwise, we can marshall the object back to a concrete implementation of the TransferTask and return
            return new org.agaveplatform.service.transfers.model.TransferTask(childTaskObject);
        }
    }

    /**
     * Creates a new concrete {@link RemoteTransferListener} using the context of current object and the
     * paths of the child.
     *
     * @param childSourcePath the source of the child {@link TransferTask}
     * @param childDestPath   the dest of the child {@link TransferTask}
     * @return the persisted {@link RemoteTransferListener}
     * @throws TransferException if the child remote transfer task listener cannot be saved
     */
    @Override
    public RemoteTransferListener createChildRemoteTransferListener(String childSourcePath, String childDestPath) throws TransferException {
        TransferTask childTransferTask = createAndPersistChildTransferTask(childSourcePath, childDestPath);
        return new RemoteTransferListenerImpl(childTransferTask, getVertx(), getRetryRequestManager());
    }

    /**
     * Creates a new concrete {@link RemoteTransferListener} using the context of current object and the
     * child {@link TransferTask}.
     *
     * @param childTransferTask the the child {@link TransferTask}
     * @return the persisted {@link RemoteTransferListener}
     */
    @Override
    public RemoteTransferListener createChildRemoteTransferListener(TransferTask childTransferTask) {
        return new RemoteTransferListenerImpl(childTransferTask, getVertx(), getRetryRequestManager());
    }

    /**
     * Look up the context of the current vertical
     * @return the current config or an empty {@link JsonObject}
     */
    protected JsonObject getCurrentConfig() {
        //
        Context context = getVertx().getOrCreateContext();
        JsonObject config = context.config();
        if (config == null) {
            config = new JsonObject();
        }

        return config;
    }

    /**
     * Gets an instance of a DB service or instantiates one if not already created.
     * @return an instance of the {@link TransferTaskDatabaseService} service.
     */
    public TransferTaskDatabaseService getDbService() {
        if (dbService == null) {

            String dbServiceQueue = getCurrentConfig().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
            dbService = TransferTaskDatabaseService.createProxy(getVertx(), dbServiceQueue);
        }

        return dbService;
    }

    /**
     * @param dbService the dbService to set
     */
    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }
}
