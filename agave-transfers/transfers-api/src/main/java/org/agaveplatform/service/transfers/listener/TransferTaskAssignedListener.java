package org.agaveplatform.service.transfers.listener;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.core.streams.Pipe;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.iplantc.service.common.exceptions.AgaveNamespaceException;
import org.iplantc.service.common.exceptions.PermissionException;
import org.iplantc.service.common.persistence.TenancyHelper;
import org.iplantc.service.systems.exceptions.RemoteCredentialException;
import org.iplantc.service.systems.exceptions.SystemUnknownException;
import org.iplantc.service.systems.model.RemoteSystem;
import org.iplantc.service.transfer.RemoteDataClient;
import org.iplantc.service.transfer.RemoteDataClientFactory;
import org.iplantc.service.transfer.RemoteFileInfo;
import org.iplantc.service.transfer.exceptions.RemoteDataException;
import org.iplantc.service.transfer.exceptions.RemoteDataSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.net.URI;
import java.util.List;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

public class TransferTaskAssignedListener extends AbstractTransferTaskListener {
    private static final Logger log = LoggerFactory.getLogger(TransferTaskAssignedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ASSIGNED;
    private TransferTaskDatabaseService dbService;

    public TransferTaskAssignedListener() {super();}
    public TransferTaskAssignedListener(Vertx vertx) {
        super(vertx);
    }
    public TransferTaskAssignedListener(Vertx vertx, String eventChannel) {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() {
        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        EventBus bus = vertx.eventBus();

        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);

            processTransferTask(body, resp -> {
                if (resp.succeeded()) {
                    log.debug("Succeeded with the procdessTransferTask in the assigning of the event {}", uuid);
                    // TODO: codify our notification behavior here. Do we rewrap? How do we ensure ordering? Do we just
                    //   throw it over the fence to Camel and forget about it? Boy, that would make things easier,
                    //   thought not likely faster.
                    // TODO: This seems like the correct pattery. Handler sent to the processing function, then
                    //   only send the notification on success. We can add a failure and error notification to the
                    //   respective listeners in the same way.
                    _doPublishEvent(MessageType.NOTIFICATION_TRANSFERTASK, body);
                } else {
                    log.error("Error with return from creating the event {}", uuid);
                    _doPublishEvent(MessageType.TRANSFERTASK_ERROR, body);

                    msg.reply(resp.cause());
                }
            });
        });

        // cancel tasks
        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            log.info("Transfer task {} cancel detected", uuid);
            super.processInterrupt("add", body);
        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            log.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
            super.processInterrupt("remove", body);
        });

        // paused tasks
        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            log.info("Transfer task {} paused detected", uuid);
            super.processInterrupt("add", body);
        });

        bus.<JsonObject>consumer(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");

            log.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
            super.processInterrupt("remove", body);
        });
    }

    protected void processTransferTask(JsonObject body, Handler<AsyncResult<Boolean>> handler) {
        //Promise<Boolean> promise = Promise.promise();
        String uuid = body.getString("uuid");
        String source = body.getString("source");
		String dest =  body.getString("dest");
        String username = body.getString("owner");
        String tenantId = body.getString("tenantId");
        String protocol = null;
        TransferTask assignedTransferTask = new TransferTask(body);

        RemoteDataClient srcClient = null;
        RemoteDataClient destClient = null;

        try {
            URI srcUri;
            URI destUri;
            try {
                srcUri = URI.create(source);
                destUri = URI.create(dest);
            } catch (Exception e) {
                String msg = String.format("Unable to parse source uri %s for transfertask %s: %s",
                        source, uuid, e.getMessage());
                throw new RemoteDataSyntaxException(msg, e);
            }

            // check for task interruption
            if (taskIsNotInterrupted(assignedTransferTask)) {

                // basic sanity check on uri again
                if (RemoteDataClientFactory.isSchemeSupported(srcUri)) {
                    // if it's an "agave://" uri, look up the connection info, get a rdc, and process the remote
                    // file item
                    if (srcUri.getScheme().equalsIgnoreCase("agave")) {
                        // get a remote data client for the source target
                        srcClient = getRemoteDataClient(tenantId, username, srcUri);
                        // get a remote data client for the dest target
                        destClient = getRemoteDataClient(tenantId, username, destUri);

                        // stat the remote path to check its type and existence
                        RemoteFileInfo fileInfo = srcClient.getFileInfo(srcUri.getPath());

                        // if the path is a file, then we can move it directly, so we raise an event telling the protocol
                        // listener to move the file item
                        if (fileInfo.isFile()) {
                            // write to the catchall transfer event channel. Nothing to update in the transfer task
                            // as the status will be updated when the transfer begins.
                            _doPublishEvent(TRANSFER_ALL, body);
                        }
                        // the path is a directory, so walk the first level of the directory, spawning new child transfer
                        // tasks for every file item found. folders will be put back on the created queue for further
                        // traversal in depth. files will be forwarded to the transfer channel for immediate processing
                        else {
                            // list the remote directory
                            List<RemoteFileInfo> remoteFileInfoList = srcClient.ls(srcUri.getPath());

                            // if the directory is emnpty, mark as complete and exit
                            if (remoteFileInfoList.isEmpty()) {
                                _doPublishEvent(TRANSFER_COMPLETED, body);
                            }
                            // if there are contents, walk the first level, creating directories on the remote side
                            // as we go to ensure that out of order processing by worker tasks can still succeed.
                            else {
                                // create the remote directory to ensure it's present when the transfers begin. This
                                // also allows us to check for things like permissions ahead of time and save the
                                // traversal in the event it's not allowed.
                                destClient.mkdirs(destUri.getPath());

                                // if the created transfertask does not have a rootTask, it is the rootTask of all
                                // the child tasks we create processing the directory listing
                                final String rootTaskId = StringUtils.isEmpty(body.getString("rootTask")) ?
                                        body.getString("uuid") : body.getString("rootTaskId");

//                                for (RemoteFileInfo childFileItem : remoteFileInfoList) {
                                MutableBoolean ongoing = new MutableBoolean(true);
                                remoteFileInfoList.stream().takeWhile(t -> ongoing.booleanValue()).forEach(childFileItem -> {
                                    // build the child paths
                                    String childSource = source + "/" + childFileItem.getName();
                                    String childDest = dest + "/" + childFileItem.getName();

                                    try {
                                        // if the assigned or ancestor transfer task were cancelled while this was running,
                                        // skip the rest.
                                        if (taskIsNotInterrupted(assignedTransferTask)) {

                                            TransferTask childTransferTask = new TransferTask(childSource, childDest, tenantId);
                                            childTransferTask.setTenantId(tenantId);
                                            childTransferTask.setOwner(username);
                                            childTransferTask.setParentTaskId(uuid);
                                            childTransferTask.setRootTaskId(rootTaskId);
                                            childTransferTask.setStatus(TransferStatusType.QUEUED);

                                            getDbService().createOrUpdateChildTransferTask(tenantId, childTransferTask, childResult -> {
                                                String fileItemType = childFileItem.isFile() ? "file" : "directory";
                                                if (childResult.succeeded()) {
                                                    String childMessageType = childFileItem.isFile() ? TRANSFER_ALL : TRANSFERTASK_CREATED;

                                                    log.debug("Finished processing child {} transfer tasks for {}: {} => {}",
                                                            fileItemType,
                                                            childResult.result().getString("uuid"),
                                                            childSource,
                                                            childDest);

                                                    _doPublishEvent(childMessageType, childResult.result());
                                                }
                                                // we couldn't create a new task and none previously existed for this child, so we must
                                                // fail the transfer for this transfertask. The decision about whether to delete the entire
                                                // root transfer task will be made based on the TransferPolicy assigned to the root task in
                                                // the failed handler.
                                                else {
                                                    // this will break the stream processing and exit the loop without completing the
                                                    // remaining RemoteFileItem in the listing.
                                                    ongoing.setFalse();

                                                    String message = String.format("Error creating new child file transfer task for %s: %s -> %s. %s",
                                                            uuid, childSource, childDest, childResult.cause().getMessage());

                                                    doHandleFailure(childResult.cause(), message, body, handler);
                                                }
                                            });
                                        } else {
                                            // interrupt happened while processing children. skip the rest.
                                            // TODO: How do we know it wasn't a pause?
                                            log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", uuid);
                                            _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, body);

                                            // this will break the stream processing and exit the loop without completing the
                                            // remaining RemoteFileItem in the listing.
                                            ongoing.setFalse();
                                        }
                                    } catch (Throwable t) {
                                        // this will break the stream processing and exit the loop without completing the
                                        // remaining RemoteFileItem in the listing.
                                        ongoing.setFalse();

                                        String message = String.format("Failed processing child file transfer task for %s: %s -> %s. %s",
                                                uuid, childSource, childDest, t.getMessage());

                                        doHandleFailure(t, message, body, handler);
                                    }
                                });
                            }
                        }
                    }
                    // it's not an agave uri, so we forward on the raw uri as we know that we can
                    // handle it from the wrapping if statement check
                    else {
                        // we might wnat to fold this into the above conditional as we could get public s3 or ftp
                        // url passed in. Both of these support directory operations, so this could be an incorrect
                        // assumption. We'll see.
                        _doPublishEvent(TRANSFER_ALL, body);
                    }
                } else {
                    String msg = String.format("Unknown source schema %s for the transfertask %s",
                            srcUri.getScheme(), uuid);
                    throw new RemoteDataSyntaxException(msg);
                }
                handler.handle(Future.succeededFuture(true));
            } else {
                // task was interrupted, so don't attempt a retry
                log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", uuid);
                _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, body);
                handler.handle(Future.succeededFuture(false));
            }
        }
        catch (RemoteDataSyntaxException e) {
            String message = String.format("Failing transfer task %s due to invalid source syntax. %s", uuid, e.getMessage());
            doHandleFailure(e, message, body, handler);
        }
        catch (Exception e) {
            log.error(e.getMessage());
            doHandleError(e, e.getMessage(), body, handler);
        }
        finally {
            // cleanup the remote data client connections
            try { if (srcClient != null) srcClient.disconnect(); } catch (Exception ignored) {}
            try { if (destClient != null) destClient.disconnect(); } catch (Exception ignored) {}
        }

        handler.handle(Future.succeededFuture(true));
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

        _doPublishEvent(TRANSFERTASK_FAILED, json);

        // propagate the exception back to the calling method
        if (handler != null) {
            handler.handle(Future.failedFuture(throwable));
        }
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

        _doPublishEvent(TRANSFERTASK_ERROR, json);

        // propagate the exception back to the calling method
        if (handler != null) {
            handler.handle(Future.failedFuture(throwable));
        }
    }

    public TransferTaskDatabaseService getDbService() {
        return dbService;
    }

    public void setDbService(TransferTaskDatabaseService dbService) {
        this.dbService = dbService;
    }

    /**
     * Obtains a new {@link RemoteDataClient} for the given {@code uri}. The schema and hostname are used to identify
     * agave {@link RemoteSystem} URI vs externally accesible URI. Tenancy is honored.
     * @param tenantId the tenant whithin which any system lookups should be made
     * @param username the user for whom the system looks should be made
     * @param target the uri from which to parse the system info
     * @return a new instance of a {@link RemoteDataClient} for the given {@code target}
     * @throws SystemUnknownException if the sytem is unknown
     * @throws AgaveNamespaceException if the URI does match any known agave uri pattern
     * @throws RemoteCredentialException if the credentials for the system represented by the URI cannot be found/refreshed/obtained
     * @throws PermissionException when the user does not have permission to access the {@code target}
     * @throws FileNotFoundException when the remote {@code target} does not exist
     * @throws RemoteDataException when a connection cannot be made to the {@link RemoteSystem}
     * @throws NotImplementedException when the schema is not supported
     */
    protected RemoteDataClient getRemoteDataClient(String tenantId, String username, URI target) throws NotImplementedException, SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, FileNotFoundException, RemoteDataException {
        TenancyHelper.setCurrentTenantId(tenantId);
        return new RemoteDataClientFactory().getInstance(username, null, target);
    }

}
