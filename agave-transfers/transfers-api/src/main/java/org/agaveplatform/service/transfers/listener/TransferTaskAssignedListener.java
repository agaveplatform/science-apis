package org.agaveplatform.service.transfers.listener;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.agaveplatform.service.transfers.enumerations.MessageType;
import org.agaveplatform.service.transfers.model.TransferTask;
import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
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

import static org.agaveplatform.service.transfers.enumerations.MessageType.*;

public class TransferTaskAssignedListener extends AbstractTransferTaskListener {
    private static final Logger log = LoggerFactory.getLogger(TransferTaskAssignedListener.class);
    protected static final String EVENT_CHANNEL = MessageType.TRANSFERTASK_ASSIGNED;

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
        EventBus bus = vertx.eventBus();
        bus.<JsonObject>consumer(getEventChannel(), msg -> {
            JsonObject body = msg.body();
            String uuid = body.getString("uuid");
            String source = body.getString("source");
            String dest = body.getString("dest");
            log.info("Transfer task {} assigned: {} -> {}", uuid, source, dest);

            processTransferTask(body);
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

    protected Future<Boolean> processTransferTask(JsonObject body) {
        Promise<Boolean> promise = Promise.promise();
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
//                        // pull the system out of the url. system id is the hostname in an agave uri
//                        RemoteSystem srcSystem = new SystemDao().findBySystemId();
//                        // get a remote data client for the system
//                        srcClient = srcSystem.getRemoteDataClient();
//
                        // get a remote data client for the system
                        srcClient = getRemoteDataClient(tenantId, username, srcUri);


//                        // pull the dest system out of the url. system id is the hostname in an agave uri
//                        RemoteSystem destSystem = new SystemDao().findBySystemId(destUri.getHost());
//                        destClient = destSystem.getRemoteDataClient();

                        destClient = getRemoteDataClient(tenantId, username, destUri);
                        // stat the remote path to check its type
                        RemoteFileInfo fileInfo = srcClient.getFileInfo(srcUri.getPath());

                        // if the path is a file, then we can move it directly, so we raise an event telling the protocol
                        // listener to move the file item
                        if (fileInfo.isFile()) {
                            // write to the catchall transfer event channel.
                            _doPublishEvent(TRANSFER_ALL, body);
                        } else {
                            // path is a directory, so walk the first level of the directory
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

                                for (RemoteFileInfo childFileItem : remoteFileInfoList) {
                                    // if the assigned or ancestor transfer task were cancelled while this was running,
                                    // skip the rest.
                                    if (taskIsNotInterrupted(assignedTransferTask)) {
                                        // if it's a file, we can process this as we would if the original path were a file
                                        if (childFileItem.isFile()) {
                                            // build the child paths
                                            String childSource = body.getString("source") + "/" + childFileItem.getName();
                                            String childDest = body.getString("dest") + "/" + childFileItem.getName();

                                            TransferTask transferTask = new TransferTask(childSource, childDest, tenantId);
                                            transferTask.setTenantId(tenantId);
                                            transferTask.setOwner(username);
                                            transferTask.setParentTaskId(uuid);
                                            if (StringUtils.isNotEmpty(body.getString("rootTask"))) {
                                                transferTask.setRootTaskId(body.getString("rootTaskId"));
                                            }
                                            _doPublishEvent(MessageType.TRANSFERTASK_CREATED, transferTask.toJson());

                                            //                                            _doPublishEvent("transfertask." + srcSystem.getType(),
                                            //                                                    "agave://" + srcSystem.getSystemId() + "/" + srcUri.getPath() + "/" + childFileItem.getName());
                                        }
                                        // if a directory, then create a new transfer task to repeat this process,
                                        // keep the association between this transfer task, the original, and the children
                                        // in place for traversal in queries later on.
                                        else {
                                            // build the child paths
                                            String childSource = body.getString("source") + "/" + childFileItem.getName();
                                            String childDest = body.getString("dest") + "/" + childFileItem.getName();

//                                            // create the remote directory to ensure it's present when the transfers begin. This
//                                            // also allows us to check for things like permissions ahead of time and save the
//                                            // traversal in the event it's not allowed.
//                                            boolean isDestCreated = destClient.mkdirs(destUri.getPath() + "/" + childFileItem.getName());

                                            TransferTask transferTask = new TransferTask(childSource, childDest, tenantId);
                                            transferTask.setTenantId(tenantId);
                                            transferTask.setOwner(username);
                                            transferTask.setParentTaskId(uuid);
                                            if (StringUtils.isNotEmpty(body.getString("rootTask"))) {
                                                transferTask.setRootTaskId(body.getString("rootTaskId"));
                                            }
                                            _doPublishEvent(MessageType.TRANSFERTASK_CREATED, transferTask.toJson());
                                        }
                                    } else {
                                        // interrupt happened wild processing children. skip the rest.
                                        log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", uuid);
                                        _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, body);
                                        break;
                                    }
                                }
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
                promise.complete(true);
            } else {
                // task was interrupted, so don't attempt a retry
                log.info("Skipping processing of child file items for transfer tasks {} due to interrupt event.", uuid);
                _doPublishEvent(MessageType.TRANSFERTASK_CANCELED_ACK, body);
                promise.complete(false);
            }
        }
        catch (RemoteDataSyntaxException e) {
            String message = String.format("Failing transfer task %s due to invalid source syntax. %s", uuid, e.getMessage());
            JsonObject json = new JsonObject()
                    .put("cause", e.getClass().getName())
                    .put("message", message)
                    .mergeIn(body);

            _doPublishEvent(TRANSFERTASK_FAILED, json);
            promise.fail(e);
        }
        catch (Exception e) {
            log.error(e.getMessage());
            JsonObject json = new JsonObject()
                    .put("cause", e.getClass().getName())
                    .put("message", e.getMessage())
                    .mergeIn(body);

            _doPublishEvent(MessageType.TRANSFERTASK_ERROR, json);
            promise.fail(e);
        }
        finally {
            // cleanup the remote data client connections
            try { if (srcClient != null) srcClient.disconnect(); } catch (Exception ignored) {}
            try { if (destClient != null) destClient.disconnect(); } catch (Exception ignored) {}
        }

        return promise.future();
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
    private RemoteDataClient getRemoteDataClient(String tenantId, String username, URI target) throws NotImplementedException, SystemUnknownException, AgaveNamespaceException, RemoteCredentialException, PermissionException, FileNotFoundException, RemoteDataException {
        TenancyHelper.setCurrentTenantId(tenantId);
        return new RemoteDataClientFactory().getInstance(username, null, target);
    }
}
