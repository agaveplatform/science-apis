package org.agaveplatform.service.transfers.listener;

import io.nats.client.Connection;
import io.nats.client.Dispatcher;
import io.nats.client.Subscription;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.database.TransferTaskDatabaseService;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.enumerations.TransferStatusType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.NotImplementedException;
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
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.CONFIG_TRANSFERTASK_DB_QUEUE;
import static org.agaveplatform.service.transfers.TransferTaskConfigProperties.FLUSH_DELAY_NATS;
import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

public class TransferTaskAssignedListener extends AbstractNatsListener {
    private static final Logger log = LoggerFactory.getLogger(TransferTaskAssignedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ASSIGNED;
    private TransferTaskDatabaseService dbService;
    public final Connection nc = _connect();

    public TransferTaskAssignedListener() throws IOException, InterruptedException {super();}
    public TransferTaskAssignedListener(Vertx vertx) throws IOException, InterruptedException {
        super(vertx);
    }
    public TransferTaskAssignedListener(Vertx vertx, String eventChannel) throws IOException, InterruptedException {
        super(vertx, eventChannel);
    }

    public String getDefaultEventChannel() {
        return EVENT_CHANNEL;
    }

    @Override
    public void start() throws IOException, InterruptedException, TimeoutException {
        // init our db connection from the pool
        String dbServiceQueue = config().getString(CONFIG_TRANSFERTASK_DB_QUEUE);
        dbService = TransferTaskDatabaseService.createProxy(vertx, dbServiceQueue);

        //EventBus bus = vertx.eventBus();
        //Connection nc = _connect();
        Dispatcher d = nc.createDispatcher((msg) -> {});
        //bus.<JsonObject>consumer(getEventChannel(), msg -> {
        Subscription s = d.subscribe(MessageType.TRANSFERTASK_ASSIGNED, msg -> {
            //msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);

            try {
                processTransferTask(body, resp -> {
                    if (resp.succeeded()) {
                        log.debug("Succeeded with the processTransferTask in the assigning of the event {}", uuid);
                        // TODO: codify our notification behavior here. Do we rewrap? How do we ensure ordering? Do we just
                        //   throw it over the fence to Camel and forget about it? Boy, that would make things easier,
                        //   thought not likely faster.
                        // TODO: This seems like the correct pattern. Handler sent to the processing function, then
                        //   only send the notification on success. We can add a failure and error notification to the
                        //   respective listeners in the same way.
                        body.put("event", this.getClass().getName());
                        body.put("type", getEventChannel());
                        try {
                            _doPublishEvent(MessageType.TRANSFERTASK_NOTIFICATION, body);
                        } catch (IOException e) {
                            log.debug(e.getMessage());
                        } catch (InterruptedException e) {
                            log.debug(e.getMessage());
                        }
                    } else {
                        //error handled in processTransferTask
                    }
                });
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_ASSIGNED);
        nc.flush(Duration.ofMillis(500));
        //d.unsubscribe(MessageType.TRANSFERTASK_ASSIGNED);


        s = d.subscribe(MessageType.TRANSFERTASK_CANCELED_SYNC, msg -> {
            //msg.reply(TransferTaskAssignedListener.class.getName() + " received.");
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            JsonObject body = new JsonObject(response) ;
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.debug("response is {}", response);

            log.info("Transfer task {} cancel detected", uuid);
            if (uuid != null) {
                addCancelledTask(uuid);
                checkPausedTask(uuid);
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_CANCELED_SYNC);
        nc.flush(Duration.ofMillis(500));
        //d.unsubscribe(MessageType.TRANSFERTASK_CANCELED_SYNC);

        s = d.subscribe(MessageType.TRANSFERTASK_CANCELED_COMPLETED, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            log.debug("response is {}", response);
            JsonObject body = new JsonObject(response) ;

            String uuid = body.getString("uuid");

            log.info("Transfer task {} cancel completion detected. Updating internal cache.", uuid);
            if (uuid != null) {
                removeCancelledTask(uuid);
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_CANCELED_COMPLETED);
        nc.flush(Duration.ofMillis(500));
        //d.unsubscribe(MessageType.TRANSFERTASK_CANCELED_COMPLETED);

        s = d.subscribe(MessageType.TRANSFERTASK_PAUSED_SYNC, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            log.debug("response is {}", response);
            JsonObject body = new JsonObject(response) ;

            String uuid = body.getString("uuid");

            log.info("Transfer task {} paused detected", uuid);
            if (uuid != null) {
                addPausedTask(uuid);
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_PAUSED_SYNC);
        nc.flush(Duration.ofMillis(500));
        //d.unsubscribe(MessageType.TRANSFERTASK_PAUSED_SYNC);

        s = d.subscribe(MessageType.TRANSFERTASK_PAUSED_COMPLETED, msg -> {
            String response = new String(msg.getData(), StandardCharsets.UTF_8);
            log.debug("response is {}", response);
            JsonObject body = new JsonObject(response) ;

            String uuid = body.getString("uuid");

            log.info("Transfer task {} paused completion detected. Updating internal cache.", uuid);
            if (uuid != null) {
                addPausedTask(uuid);
            }
        });
        d.subscribe(MessageType.TRANSFERTASK_PAUSED_COMPLETED);
        nc.flush(Duration.ofMillis(500));
        //d.unsubscribe(MessageType.TRANSFERTASK_PAUSED_COMPLETED);
    }

    protected void processTransferTask(JsonObject body, Handler<AsyncResult<Boolean>> handler) throws IOException, InterruptedException {
        log.debug("Got into TransferTaskAssignedListener.processTransferTask");
        //Promise<Boolean> promise = Promise.promise();
        String uuid = body.getString("uuid");
        String source = body.getString("source");
		String dest =  body.getString("dest");
        String username = body.getString("owner");
        String tenantId = (body.getString("tenant_id"));
        String protocol = null;
        TransferTask assignedTransferTask = new TransferTask(body);
        log.debug("Body of JsonObject: {}", body.toString());
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
                log.error(msg);
                throw new RemoteDataSyntaxException(msg, e);
            }

            // check to see if the parentTaskId or the rootTaskId are null.
            // If either one of them are then skipp the processing
            // otherwise process the task
            log.info("check to be sure root and parent are not null");
           // if (assignedTransferTask.getRootTaskId() != null && assignedTransferTask.getParentTaskId() != null) {
                // check for task interruption
                log.info("Check for task interruption TransferTaskAssignedListener.processTransferTask");
                log.info("Root task {}, Parent task {}", assignedTransferTask.getRootTaskId(), assignedTransferTask.getParentTaskId());
                if (taskIsNotInterrupted(assignedTransferTask)) {
                    log.trace("got past taskIsNotInterrupted");
                    // basic sanity check on uri again
                    // basic sanity check on uri again
                    if (uriSchemeIsNotSupported(srcUri)) {
                        String msg = String.format("Failing transfer task %s due to invalid scheme in source URI, %s",
                                assignedTransferTask.getUuid(), assignedTransferTask.getSource());
                        throw new RemoteDataSyntaxException(msg);
                    } else if (uriSchemeIsNotSupported(destUri)) {
                        String msg = String.format("Failing transfer task %s due to invalid scheme in destination URI, %s",
                                assignedTransferTask.getUuid(), assignedTransferTask.getDest());
                        throw new RemoteDataSyntaxException(msg);
                    } else {
                        log.trace("got into srcClient & destCleint");
                        // get a remote data client for the source target
                        srcClient = getRemoteDataClient(tenantId, username, srcUri);
                        // get a remote data client for the dest target
                        destClient = getRemoteDataClient(tenantId, username, destUri);

                        // stat the remote path to check its type and existence
                        log.info("stat the remote path to check its type and existence");
                        RemoteFileInfo srcFileInfo = srcClient.getFileInfo(srcUri.getPath());
                        log.info("Remote path was checked.");
                        // if the path is a file, then we can move it directly, so we raise an event telling the protocol
                        // listener to move the file item
                        if (srcFileInfo.isFile()) {
                            log.info("srcFileInfo is a file.");
                            // but first, we udpate the transfer task status to ASSIGNED
                            assignedTransferTask.setStatus(TransferStatusType.ASSIGNED);
                            _doPublishEvent(TRANSFER_ALL, assignedTransferTask.toJson() );


                        }
                        // the path is a directory, so walk the first level of the directory, spawning new child transfer
                        // tasks for every file item found. folders will be put back on the created queue for further
                        // traversal in depth. files will be forwarded to the transfer channel for immediate processing
                        else {
                            // list the remote directory
                            log.info("list the remote directory");
                            List<RemoteFileInfo> remoteFileInfoList = srcClient.ls(srcUri.getPath());
                            log.debug(String.valueOf(remoteFileInfoList.size()));
                            // if the directory is empty, the listing will only contain the target path as the "." folder.
                            // mark as complete and wrap it up.
                            if (remoteFileInfoList.size() <= 1) {
                                // but first, we udpate the transfer task status to ASSIGNED
                                getDbService().updateStatus(tenantId, uuid, TransferStatusType.COMPLETED.name(), updateResult -> {
                                    if (updateResult.succeeded()) {
                                        log.debug("Assigning single file transfer task {} to the TRANSFER_COMPLETED queue as the remote source directory path is empty. And tenant id is {}", uuid, tenantId);
                                        // write to the completed event channel.
                                        assignedTransferTask.setStatus(TransferStatusType.COMPLETED);
                                        assignedTransferTask.setStartTime(Instant.now());
                                        assignedTransferTask.setEndTime(Instant.now());
                                        try{
                                            _doPublishEvent(TRANSFER_COMPLETED, assignedTransferTask.toJson());
                                        } catch (IOException e) {
                                            log.debug(e.getMessage());
                                        } catch (InterruptedException e) {
                                            log.debug(e.getMessage());
                                        }
                                        handler.handle(Future.succeededFuture(true));
                                    }
                                    // we couldn't update the transfer task value
                                    else {
                                        String message = String.format("Error updating status of transfer task %s to ASSIGNED. %s",
                                                uuid, updateResult.cause().getMessage());
                                        // write to error queue. we can retry
                                        try {
                                            doHandleError(updateResult.cause(), message, body, handler);
                                        } catch (IOException e) {
                                            e.printStackTrace();
                                        } catch (InterruptedException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                });
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
                                final String rootTaskId = assignedTransferTask.getRootTaskId() == null ?
                                    assignedTransferTask.getUuid() : assignedTransferTask.getRootTaskId();

                                // create a new executor pool so we don't block the event thread with the potentially large number of network operations
                                MutableBoolean ongoing = new MutableBoolean(true);
                                List<Future> childFutures = remoteFileInfoList.stream()
                                        .takeWhile(childFileItem -> ongoing.booleanValue())
                                        .filter(childFileItem -> !childFileItem.getName().endsWith("."))
                                        .map(childFileItem -> {
                                            Promise promise = Promise.promise();
                                            String childBaseFileName = Paths.get(childFileItem.getName()).normalize().getFileName().toString();
                                            // build the child paths
                                            String childSource = source + "/" + childBaseFileName;
                                            String childDest = dest + "/" + childBaseFileName;

                                            try {
                                                // if the assigned or ancestor transfer task were cancelled while this was running,
                                                // skip the rest.
                                                log.debug("Check interrupted?");
                                                if (taskIsNotInterrupted(assignedTransferTask)) {
                                                    log.debug("Task is good.  Creating task");

                                                    TransferTask childTransferTask = new TransferTask(childSource, childDest, tenantId);
                                                    childTransferTask.setTenantId(assignedTransferTask.getTenantId());
                                                    childTransferTask.setOwner(username);
                                                    childTransferTask.setParentTaskId(uuid);
                                                    childTransferTask.setRootTaskId(rootTaskId);
                                                    childTransferTask.setStatus(childFileItem.isDirectory() ? TransferStatusType.CREATED : TransferStatusType.ASSIGNED);
                                                    log.debug("Creating db insert.  JsonObject = {}", childTransferTask);
                                                    getDbService().createOrUpdateChildTransferTask(tenantId, childTransferTask, childResult -> {
                                                        String fileItemType = childFileItem.isFile() ? "file" : "directory";
                                                        if (childResult.succeeded()) {

                                                            String childMessageType = childFileItem.isFile() ? TRANSFER_ALL : TRANSFERTASK_CREATED;
                                                            log.debug(childMessageType);
                                                            log.debug("Finished processing child {} transfer tasks for {}: {} => {}",
                                                                    fileItemType,
                                                                    childResult.result().getString("uuid"),
                                                                    childSource,
                                                                    childDest);
                                                            try {
                                                                _doPublishEvent(childMessageType, childResult.result());
                                                            } catch (IOException e) {
                                                                log.debug(e.getMessage());
                                                            } catch (InterruptedException e) {
                                                                log.debug(e.getMessage());
                                                            }
                                                            promise.complete();
                                                        }
                                                        // we couldn't create a new task and none previously existed for this child, so we must
                                                        // fail the transfer for this transfertask. The decision about whether to delete the entire
                                                        // root transfer task will be made based on the TransferPolicy assigned to the root task in
                                                        // the failed handler.
                                                        else {
                                                            // this will break the stream processing and exit the loop without completing the
                                                            // remaining RemoteFileItem in the listing.
                                                            log.debug("Failed db insert.");
                                                            ongoing.setFalse();

                                                            String message = String.format("Error creating new child file transfer task for %s: %s -> %s. %s",
                                                                    uuid, childSource, childDest, childResult.cause().getMessage());
                                                            log.debug(message);
                                                            try {
                                                                doHandleFailure(childResult.cause(), message, body, null);
                                                            } catch (IOException e) {
                                                                e.printStackTrace();
                                                            } catch (InterruptedException e) {
                                                                e.printStackTrace();
                                                            }

                                                            promise.fail(childResult.cause());
                                                        }
                                                    });
                                                } else {
                                                    // interrupt happened while processing children. skip the rest.
                                                    // TODO: How do we know it wasn't a pause?
                                                    log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", uuid);
                                                    _doPublishEvent(TRANSFERTASK_CANCELED_ACK, body);

                                                    // this will break the stream processing and exit the loop without completing the
                                                    // remaining RemoteFileItem in the listing.
                                                    ongoing.setFalse();
                                                    promise.fail(new InterruptedException("Interrupt event occurred wile processing child file item "));
                                                }
                                            } catch (Throwable t) {
                                                // this will break the stream processing and exit the loop without completing the
                                                // remaining RemoteFileItem in the listing.
                                                ongoing.setFalse();

                                                String message = String.format("Failed processing child file transfer task for %s: %s -> %s. %s",
                                                        uuid, childSource, childDest, t.getMessage());
                                                log.debug(message);
                                                try {
                                                    doHandleFailure(t, message, body, null);
                                                } catch (IOException e) {
                                                    e.printStackTrace();
                                                } catch (InterruptedException e) {
                                                    e.printStackTrace();
                                                }
                                                promise.fail(t);
                                            }

                                            return promise.future();
                                        })
                                        .collect(Collectors.toList());

                                // Now we wait for all the futures to complete. A single failure will fail all futures
                                // here, and each of our futures will fail itself on an interrupt. Thus, in our
                                // completion handler, we just deal with whether it finished as expected or not.

                                CompositeFuture.join(childFutures).onComplete(ar -> {
                                    // all file items were processed successfully
                                    if (ar.succeeded()) {
                                        // update the original task now that we've processed all of its children
                                        assignedTransferTask.setStatus(TransferStatusType.ASSIGNED);
                                        assignedTransferTask.setStartTime(Instant.now());
                                        getDbService().update(tenantId, uuid, assignedTransferTask, updateResult -> {
                                            if (updateResult.succeeded()) {
                                                log.debug("Updated parent transfer task {} to ASSIGNED after processing all its children.", uuid);
                                                // write to the completed event channel.
//                                                _doPublishEvent(TRANSFER_COMPLETED, body);
                                                handler.handle(Future.succeededFuture(true));
                                            }
                                            // we couldn't update the transfer task value
                                            else {
                                                String message = String.format("Error updating status of parent transfer task %s to ASSIGNED. %s",
                                                        uuid, updateResult.cause().getMessage());
                                                // write to error queue. we can retry
                                                try {
                                                    doHandleError(updateResult.cause(), message, body, handler);
                                                } catch (IOException e) {
                                                    e.printStackTrace();
                                                } catch (InterruptedException e) {
                                                    e.printStackTrace();
                                                }
                                            }
                                        });
                                    } else if (!ongoing.booleanValue()) {
                                        // if a task failed due to interruption, return false. The ack has already been
                                        // handled by the child promises
                                        handler.handle(Future.succeededFuture(false));
                                    } else {
                                        // general failure.
                                        handler.handle(Future.failedFuture(ar.cause()));
                                    }
                                });
                            }
                        }
                    }
                } else {
                    // task was interrupted, so don't attempt a retry
                    log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", uuid);
                    _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, body);
                    handler.handle(Future.succeededFuture(false));
                }
            //}
        }
        catch (RemoteDataSyntaxException e) {
            String message = String.format("Failing transfer task %s due to invalid source syntax. %s", uuid, e.getMessage());
            log.error(message);
            doHandleFailure(e, message, body, handler);
        }
        catch (Throwable e) {
            String message = String.format("Caught a general exception. %s  %s", uuid, e.getMessage());
            log.error(message);
            doHandleError(e, e.getMessage(), body, handler);
        }
        finally {
            // cleanup the remote data client connections
            try { if (srcClient != null) srcClient.disconnect(); } catch (Exception ignored) {}
            try { if (destClient != null) destClient.disconnect(); } catch (Exception ignored) {}
        }
        handler.handle(Future.succeededFuture(true));
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
